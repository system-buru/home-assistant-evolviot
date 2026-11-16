"""Transport regressions runnable without a full Home Assistant installation.

Only the HA framework shell is stubbed; the integration, crypto, routing and
native socket reader are the production modules.
"""

from __future__ import annotations

import asyncio
import base64
import importlib.util
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch

from aiohttp import ClientConnectionError, ClientConnectorError, WSMsgType
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


def load_integration():
    """Load the actual source with minimal HA framework stand-ins."""
    root = Path(__file__).resolve().parents[1] / "custom_components" / "evolviot"
    package = types.ModuleType("evolviot_test")
    package.__path__ = [str(root)]

    class Coordinator:
        def __class_getitem__(cls, _):
            return cls

        def __init__(self, hass, *args, **kwargs):
            self.hass = hass
            self.data = None
            self.update_interval = kwargs.get("update_interval")

        def async_set_updated_data(self, data):
            self.data = data

        async def async_request_refresh(self):
            self.data = await self._async_update_data()

    class Entity(Coordinator):
        def __init__(self, coordinator):
            self.coordinator = coordinator
            self.hass = coordinator.hass

        def async_write_ha_state(self):
            pass

    class Store(Coordinator):
        stored = None

        async def async_load(self):
            return self.stored

        async def async_save(self, data):
            self.stored = data

    modules = {"evolviot_test": package}
    exports = {
        "homeassistant.const": {
            "Platform": types.SimpleNamespace(
                SWITCH="switch",
                LIGHT="light",
                FAN="fan",
                SENSOR="sensor",
                BINARY_SENSOR="binary_sensor",
            )
        },
        "homeassistant.core": {"HomeAssistant": object},
        "homeassistant.helpers.storage": {"Store": Store},
        "homeassistant.helpers.entity": {"DeviceInfo": dict},
        "homeassistant.helpers.update_coordinator": {
            "DataUpdateCoordinator": Coordinator,
            "CoordinatorEntity": Entity,
            "UpdateFailed": RuntimeError,
        },
    }
    for name, attrs in exports.items():
        module = types.ModuleType(name)
        module.__dict__.update(attrs)
        modules[name] = module
    loaded = []
    with patch.dict(sys.modules, modules):
        for name in ("const", "local_socket", "api", "coordinator", "entity"):
            spec = importlib.util.spec_from_file_location(
                f"evolviot_test.{name}", root / f"{name}.py"
            )
            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)
            loaded.append(module)
    return loaded


const, native, api_module, coordinator_module, entity_module = load_integration()


class FakeSocket:
    def __init__(self, *, acknowledge=True):
        self.closed = False
        self.messages = asyncio.Queue()
        self.commands = []
        self.sent = asyncio.Event()
        self.acknowledge = acknowledge
        self.snapshot = {"switch5": 45}
        self.push(self.snapshot)

    def push(self, payload):
        self.messages.put_nowait(
            types.SimpleNamespace(type=WSMsgType.TEXT, data=json.dumps(payload))
        )

    def __aiter__(self):
        return self

    async def __anext__(self):
        message = await self.messages.get()
        if message is None:
            raise StopAsyncIteration
        return message

    async def send_str(self, value):
        assert value == "status"
        self.push(self.snapshot)

    async def send_json(self, value):
        self.commands.append(value)
        self.sent.set()
        if self.acknowledge:
            self.push({"s": "ok", "key": "switch5", "value": 45})

    async def close(self):
        if not self.closed:
            self.closed = True
            self.messages.put_nowait(None)


class FakeSession:
    def __init__(self, socket=None, fail=False):
        self.socket = socket
        self.fail = fail
        self.urls = []

    async def ws_connect(self, url):
        self.urls.append(url)
        if self.fail:
            raise ClientConnectionError("unreachable")
        return self.socket


async def flush():
    for _ in range(15):
        await asyncio.sleep(0)


class NativeSocketTests(unittest.IsolatedAsyncioTestCase):
    async def test_short_status_deadline_does_not_cancel_mdns_lookup(self):
        finished = asyncio.Event()

        async def connect(_):
            await asyncio.sleep(0.04)
            finished.set()
            raise ClientConnectionError("MDNS lookup failed")

        session = FakeSession()
        session.ws_connect = connect
        client = native.LocalDeviceSocket(
            session,
            ["ws://device.local/ws"],
            lambda _: None,
            timeout=0.01,
            discovery_timeout=0.2,
        )
        self.addAsyncCleanup(client.close)
        with self.assertRaises(native.LocalSocketUnavailable):
            await client.request_status()
        await asyncio.wait_for(finished.wait(), 0.2)

    async def make_client(self, *, acknowledge=True):
        socket = FakeSocket(acknowledge=acknowledge)
        updates = []
        client = native.LocalDeviceSocket(
            FakeSession(socket),
            ["ws://device/ws"],
            updates.append,
            timeout=0.1,
            command_timeout=0.05,
        )
        self.addAsyncCleanup(client.close)
        return client, socket, updates

    async def test_initial_snapshot_refresh_and_live_pushes(self):
        client, socket, updates = await self.make_client()
        self.assertEqual((await client.request_status())["switch5"], 45)
        socket.push({"switch5": 80})
        await flush()
        self.assertEqual(updates[-1], {"switch5": 80})

    async def test_commands_serialized_and_status_is_not_an_ack(self):
        client, socket, _ = await self.make_client(acknowledge=False)
        first = asyncio.create_task(client.send_command("switch5", {"data": "first"}))
        await socket.sent.wait()
        second = asyncio.create_task(client.send_command("switch6", {"data": "second"}))
        socket.push({"switch5": 45})
        socket.push({"s": "ok", "key": "switch6"})
        await flush()
        self.assertFalse(first.done())
        self.assertEqual(len(socket.commands), 1)
        socket.push({"s": "ok", "key": "switch5"})
        await first
        await flush()
        self.assertEqual(len(socket.commands), 2)
        socket.push({"s": "error", "key": "switch6", "rsn": "Locked"})
        with self.assertRaisesRegex(native.LocalCommandRejected, "Locked"):
            await second

    async def test_timeout_closes_connection_and_is_uncertain(self):
        client, socket, _ = await self.make_client(acknowledge=False)
        with self.assertRaises(native.LocalCommandUncertain):
            await client.send_command("switch5", {"data": "sent"})
        self.assertTrue(socket.closed)

    async def test_disconnect_after_send_is_uncertain(self):
        client, socket, _ = await self.make_client(acknowledge=False)
        command = asyncio.create_task(client.send_command("switch5", {"data": "sent"}))
        await socket.sent.wait()
        await socket.close()
        with self.assertRaises(native.LocalCommandUncertain):
            await command

    async def test_dispose_stops_reconnects(self):
        client, socket, _ = await self.make_client()
        await client.request_status()
        await client.close()
        self.assertTrue(client._task.done())
        self.assertTrue(socket.closed)

    async def test_reconnect_receives_fresh_snapshot(self):
        first, second = FakeSocket(), FakeSocket()
        second.snapshot = {"switch5": 90}
        second.messages = asyncio.Queue()
        second.push(second.snapshot)
        session = FakeSession()
        session.ws_connect = AsyncMock(side_effect=[first, second])
        client = native.LocalDeviceSocket(
            session, ["ws://device/ws"], lambda _: None, timeout=2
        )
        self.addAsyncCleanup(client.close)
        await client.request_status()
        await first.close()
        await flush()
        self.assertEqual(await client.request_status(), {"switch5": 90})
        self.assertEqual(session.ws_connect.await_count, 2)

    async def test_stale_ip_timeout_still_tries_mdns(self):
        socket = FakeSocket()

        async def connect(url):
            if "192.168" in url:
                await asyncio.sleep(60)
            return socket

        session = FakeSession()
        session.ws_connect = connect
        client = native.LocalDeviceSocket(
            session,
            ["ws://192.168.1.9/ws", "ws://device.local/ws"],
            lambda _: None,
            timeout=0.2,
        )
        self.addAsyncCleanup(client.close)
        self.assertEqual(await client.request_status(), {"switch5": 45})

    async def test_close_wakes_status_waiters_during_connection(self):
        async def connect(_):
            await asyncio.sleep(60)

        session = FakeSession()
        session.ws_connect = connect
        client = native.LocalDeviceSocket(session, ["ws://device/ws"], lambda _: None)
        status = asyncio.create_task(client.request_status())
        await flush()
        await client.close()
        with self.assertRaises(native.LocalSocketUnavailable):
            await asyncio.wait_for(status, 0.1)


class ApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_http_command_and_status_use_the_automatically_supplied_ip(self):
        class Response:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_):
                pass

            def raise_for_status(self):
                pass

            async def text(self):
                return "ok"

            async def json(self, **_):
                return {"switch5": 45}

        from unittest.mock import Mock

        session = FakeSession()
        session.get = Mock(return_value=Response())
        session.post = Mock(return_value=Response())
        api = self.make_api(session)
        api._async_local_client = AsyncMock(
            return_value=types.SimpleNamespace(
                send_command=AsyncMock(side_effect=native.LocalSocketUnavailable()),
                request_status=AsyncMock(side_effect=native.LocalSocketUnavailable()),
            )
        )
        await api.async_local_command(
            uid="user",
            device_id="device",
            device_secret="secret",
            endpoint="switch5",
            switch_name="switch5",
            value=45,
            host="192.168.1.99",
        )
        await api.async_local_status(
            uid="user", device_id="device", device_secret="secret", host="192.168.1.99"
        )
        self.assertEqual(session.post.call_args.args[0], "http://192.168.1.99/switch5")
        self.assertEqual(session.get.call_args.args[0], "http://192.168.1.99/status")
        self.assertIn("X-Evolv-Signature", session.get.call_args.kwargs["headers"])

    def make_api(self, session=None):
        api = api_module.EvolvIOTApi(
            session or FakeSession(fail=True), "https://cloud.example"
        )
        self.addAsyncCleanup(api.async_close)
        return api

    async def test_encrypted_ws_payload_matches_app(self):
        socket = FakeSocket()
        api = self.make_api(FakeSession(socket))
        mode = await api.async_local_command(
            uid="user",
            device_id="device",
            device_secret="secret",
            endpoint="switch5",
            switch_name="switch5",
            value=45,
        )
        self.assertEqual(mode, "local_ws")
        envelope = socket.commands[0]
        self.assertEqual(
            envelope["hmac"],
            api_module._sign_local_payload(
                envelope["data"], "secret", "user", "device"
            ),
        )
        raw = base64.b64decode(envelope["data"])
        aes_key, _ = api_module._derive_local_keys("secret", "user", "device")
        decryptor = Cipher(algorithms.AES(aes_key), modes.CBC(raw[:16])).decryptor()
        plaintext = decryptor.update(raw[16:]) + decryptor.finalize()
        self.assertEqual(
            json.loads(plaintext[: -plaintext[-1]]), {"key": "switch5", "value": 45}
        )

    async def test_http_only_when_websocket_unavailable_before_send(self):
        api = self.make_api()
        client = types.SimpleNamespace(
            send_command=AsyncMock(side_effect=native.LocalSocketUnavailable())
        )
        api._async_local_client = AsyncMock(return_value=client)
        api._async_local_http_command = AsyncMock()
        mode = await api.async_local_command(
            uid="user",
            device_id="device",
            device_secret="secret",
            endpoint="switch5",
            switch_name="switch5",
            value=45,
        )
        self.assertEqual(mode, "local_http")
        api._async_local_http_command.assert_awaited_once()

    async def test_unsupported_ws_endpoint_uses_http(self):
        session = FakeSession(fail=True)
        api = self.make_api(session)
        api._async_local_http_command = AsyncMock()
        mode = await api.async_local_command(
            uid="user",
            device_id="device",
            device_secret="secret",
            endpoint="switch5",
            switch_name="switch5",
            value=45,
        )
        self.assertEqual(mode, "local_http")
        self.assertEqual(session.urls, ["ws://evolviot-device.local/ws"])
        api._async_local_http_command.assert_awaited_once()

    async def test_rejection_and_uncertain_ws_commands_never_replay_http(self):
        api = self.make_api()
        api._async_local_http_command = AsyncMock()
        for error, expected in (
            (native.LocalCommandRejected("Locked"), api_module.EvolvIOTApiError),
            (
                native.LocalCommandUncertain("Lost ack"),
                api_module.EvolvIOTCommandUncertain,
            ),
        ):
            client = types.SimpleNamespace(send_command=AsyncMock(side_effect=error))
            api._async_local_client = AsyncMock(return_value=client)
            with self.assertRaises(expected):
                await api.async_local_command(
                    uid="user",
                    device_id="device",
                    device_secret="secret",
                    endpoint="switch5",
                    switch_name="switch5",
                    value=45,
                )
        api._async_local_http_command.assert_not_awaited()

    async def test_status_falls_back_to_http(self):
        api = self.make_api()
        api._async_local_client = AsyncMock(
            return_value=types.SimpleNamespace(
                request_status=AsyncMock(side_effect=native.LocalSocketUnavailable()),
            )
        )
        api._async_local_http_status = AsyncMock(return_value={"switch5": 90})
        result = await api.async_local_status(
            uid="user", device_id="device", device_secret="secret"
        )
        self.assertEqual(result, {"switch5": 90})
        self.assertEqual(api.local_transports["device"], "local_http")

    async def test_cached_ip_then_mdns(self):
        api = self.make_api(FakeSession(FakeSocket()))
        client = await api._async_local_client("DEV_1", "192.168.1.5")
        self.assertEqual(
            client.urls, ["ws://192.168.1.5/ws", "ws://evolviot-dev-1.local/ws"]
        )
        self.assertIs(await api._async_local_client("DEV_1", "192.168.1.5"), client)
        await api.async_prune_local_clients(set())
        self.assertTrue(client._task.done())

    async def test_cloud_connect_failure_is_safe_but_response_loss_is_uncertain(self):
        api = self.make_api()
        key = types.SimpleNamespace(host="cloud.example", port=443, ssl=True)
        for cause, expected in (
            (
                ClientConnectorError(key, OSError("unreachable")),
                api_module.EvolvIOTCommandUnavailable,
            ),
            (
                ClientConnectionError("lost response"),
                api_module.EvolvIOTCommandUncertain,
            ),
        ):
            error = api_module.EvolvIOTConnectionError("failed")
            error.__cause__ = cause
            api._request = AsyncMock(side_effect=error)
            with self.assertRaises(expected):
                await api.async_command("switch.entity", {"command": "turn_on"})


class RoutingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.api = api_module.EvolvIOTApi(
            FakeSession(fail=True), "https://cloud.example"
        )
        self.addAsyncCleanup(self.api.async_close)
        self.api.async_command = AsyncMock(return_value={"success": True})
        self.api.async_local_command = AsyncMock(return_value="local_ws")
        hass = types.SimpleNamespace(async_create_task=asyncio.create_task)
        self.coordinator = coordinator_module.EvolvIOTDataUpdateCoordinator(
            hass, self.api, "test"
        )
        self.metadata = {
            "entity_id": "switch.entity",
            "domain": "switch",
            "device": {"id": "device", "local_control": {"device_secret": "secret"}},
            "control": {"key": "switch5"},
        }
        self.coordinator.data = {
            "user_id": "user",
            "entities": {"switch.entity": self.metadata},
            "states": {"switch.entity": {"available": True, "cloud_available": True}},
        }
        self.entity = entity_module.EvolvIOTEntity(self.coordinator, self.metadata)
        self.entity._schedule_refresh = lambda: None

    async def test_cloud_first_and_no_local_command_on_success(self):
        await self.entity._async_send_command({"command": "turn_on"})
        self.api.async_command.assert_awaited_once()
        self.api.async_local_command.assert_not_awaited()

    async def test_cloud_unavailable_uses_local(self):
        self.api.async_command.side_effect = api_module.EvolvIOTCommandUnavailable(
            "connect failed"
        )
        await self.entity._async_send_command({"command": "turn_on"})
        self.api.async_local_command.assert_awaited_once()
        self.assertEqual(
            self.entity.extra_state_attributes["connection_mode"], "local_ws"
        )

    async def test_known_cloud_offline_skips_cloud(self):
        self.coordinator.states["switch.entity"]["cloud_available"] = False
        await self.entity._async_send_command({"command": "turn_off"})
        self.api.async_command.assert_not_awaited()
        self.api.async_local_command.assert_awaited_once()

    async def test_cloud_uncertain_refreshes_without_local_replay(self):
        self.api.async_command.side_effect = api_module.EvolvIOTCommandUncertain(
            "lost ack"
        )
        self.coordinator.async_request_refresh = AsyncMock()
        with self.assertRaises(api_module.EvolvIOTCommandUncertain):
            await self.entity._async_send_command({"command": "turn_on"})
        self.coordinator.async_request_refresh.assert_awaited_once()
        self.api.async_local_command.assert_not_awaited()

    async def test_rejected_cloud_command_does_not_fall_back(self):
        self.api.async_command.side_effect = api_module.EvolvIOTApiError("Locked")
        with self.assertRaisesRegex(api_module.EvolvIOTApiError, "Locked"):
            await self.entity._async_send_command({"command": "turn_on"})
        self.api.async_local_command.assert_not_awaited()

    async def test_live_local_states_and_cloud_priority(self):
        state = self.coordinator.states["switch.entity"]
        state["state"] = "off"
        self.coordinator._handle_local_status("device", {"switch5": 45})
        self.assertEqual(self.entity.backend_state["state"], "off")
        self.assertEqual(self.entity.extra_state_attributes["connection_mode"], "cloud")
        self.coordinator.states["switch.entity"]["cloud_available"] = False
        self.coordinator._handle_local_status("device", {"switch5": 45})
        self.assertEqual(self.entity.backend_state["state"], "on")
        self.coordinator._handle_local_status("device", None)
        self.assertFalse(self.entity.available)

    async def test_cached_devices_allow_offline_refresh(self):
        self.coordinator._store.stored = {
            "devices_payload": {
                "user_id": "user",
                "entities": [self.metadata],
            }
        }
        self.coordinator.data = None
        await self.coordinator.async_load_cache()
        self.api.async_get_devices = AsyncMock(
            side_effect=api_module.EvolvIOTConnectionError("offline")
        )
        self.api.async_local_status = AsyncMock(return_value={"switch5": 1})
        self.api.local_transports["device"] = "local_http"
        result = await self.coordinator._async_http_update_data()
        state = result["states"]["switch.entity"]
        self.assertFalse(state["cloud_available"])
        self.assertTrue(state["available"])
        self.assertEqual(state["state"], "on")
        self.assertEqual(state["local_transport"], "local_http")
        self.assertEqual(const.DEFAULT_SCAN_INTERVAL.total_seconds(), 15)

    async def test_cloud_recovery_takes_priority_over_local_state(self):
        self.api.async_get_devices = AsyncMock(
            return_value={"user_id": "user", "entities": [self.metadata]}
        )
        self.api.async_get_states = AsyncMock(
            return_value={
                "states": [
                    {
                        "entity_id": "switch.entity",
                        "available": True,
                        "state": "on",
                    }
                ]
            }
        )
        self.api.async_local_status = AsyncMock(return_value={"switch5": 0})
        self.api.local_transports["device"] = "local_ws"
        self.coordinator.data = await self.coordinator._async_http_update_data()
        self.assertEqual(self.entity.backend_state["state"], "on")
        self.assertEqual(self.entity.extra_state_attributes["connection_mode"], "cloud")

        self.api.async_local_status.assert_not_awaited()

    async def test_cloud_offline_push_uses_latest_local_snapshot(self):
        self.coordinator._handle_local_status("device", {"switch5": 45})
        await self.coordinator._async_handle_ws_state_changed(
            {
                "state": {
                    "entity_id": "switch.entity",
                    "available": False,
                    "state": "off",
                },
            }
        )
        self.assertEqual(self.entity.backend_state["state"], "on")
        self.assertEqual(
            self.entity.extra_state_attributes["connection_mode"], "local_ws"
        )

    async def test_ws_reconnect_failure_keeps_http_available(self):
        self.api.local_transports["device"] = "local_http"
        state = self.coordinator.states["switch.entity"]
        state.update(
            cloud_available=False, local_available=True, local_transport="local_http"
        )
        self.coordinator._handle_local_status("device", None)
        self.assertTrue(self.entity.available)
        self.assertEqual(
            self.entity.extra_state_attributes["connection_mode"], "local_http"
        )

    async def test_periodic_refresh_is_independent_of_pushes(self):
        self.coordinator.async_request_refresh = AsyncMock()
        self.coordinator._handle_local_status("device", {"switch5": 45})
        await self.coordinator.async_periodic_refresh(None)
        self.coordinator.async_request_refresh.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
