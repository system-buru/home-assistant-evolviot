"""API client for the EvolvIOT Home Assistant cloud endpoints."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Awaitable, Callable
import contextlib
import hashlib
import hmac
import ipaddress
import json
import logging
import os
import re
import time
from typing import Any
from urllib.parse import quote, urlencode, urlsplit, urlunsplit

from aiohttp import (
    ClientError,
    ClientConnectorError,
    ClientResponseError,
    ClientSession,
    ClientTimeout,
    ClientWebSocketResponse,
    WSMsgType,
    WSServerHandshakeError,
)
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from .const import (
    DEFAULT_API_BASE_URL,
    DEFAULT_HEALTH_URL,
    DEFAULT_LOCAL_COMMAND_TIMEOUT,
    DEFAULT_LOCAL_DISCOVERY_TIMEOUT,
    DEFAULT_WS_CONNECT_TIMEOUT,
    DEFAULT_WS_HEARTBEAT,
    DEFAULT_WS_READY_TIMEOUT,
    DEFAULT_WS_RECONNECT_DELAYS,
    DEFAULT_WS_REQUEST_TIMEOUT,
    LOCAL_MDNS_DOMAIN,
)
from .local_socket import (
    LocalCommandRejected,
    LocalCommandUncertain,
    LocalDeviceSocket,
    LocalSocketUnavailable,
)

TokenUpdateCallback = Callable[[dict[str, Any]], Awaitable[None]]
WebSocketMessageCallback = Callable[[dict[str, Any]], Awaitable[None]]

_LOGGER = logging.getLogger(__name__)


class EvolvIOTApiError(Exception):
    """Base API error."""


class EvolvIOTAuthError(EvolvIOTApiError):
    """Authentication failed."""


class EvolvIOTConnectionError(EvolvIOTApiError):
    """Connection failed."""


class EvolvIOTCommandUnavailable(EvolvIOTConnectionError):
    """Command was not sent; another transport can be used."""


class EvolvIOTCommandUncertain(EvolvIOTApiError):
    """Command may have executed and must not be replayed."""


class EvolvIOTDeviceAuthorizationPending(EvolvIOTApiError):
    """Device authorization is still pending."""


class EvolvIOTDeviceAuthorizationDenied(EvolvIOTAuthError):
    """Device authorization was denied."""


class EvolvIOTDeviceAuthorizationExpired(EvolvIOTAuthError):
    """Device authorization expired."""


def normalize_api_base_url(value: str | None) -> str:
    """Normalize user supplied API URL to the Home Assistant route root."""
    base_url = (value or DEFAULT_API_BASE_URL).strip().rstrip("/")
    if base_url.endswith("/api/homeassistant"):
        return base_url
    if base_url.endswith("/api"):
        return f"{base_url}/homeassistant"
    return f"{base_url}/api/homeassistant"


def _sanitize_device_id_for_mdns(device_id: str) -> str:
    """Return the ESP mDNS-safe device id."""
    return re.sub(r"[^a-z0-9-]", "-", device_id.lower())


def local_device_host(
    device: dict[str, Any], local_control: dict[str, Any]
) -> str | None:
    """Return the cached LAN IP using the same metadata aliases as the app."""
    return (
        local_control.get("ip")
        or device.get("ip")
        or (device.get("info") or {}).get("ip")
        or (device.get("deviceInfo") or {}).get("ip")
    )


def _derive_local_keys(
    device_secret: str,
    uid: str,
    device_id: str,
) -> tuple[bytes, bytes]:
    """Derive AES and HMAC keys matching the EvolvIOT app."""
    key_material = f"{device_secret}:{uid}:{device_id}"
    aes_key = hashlib.sha256(f"{key_material}:AES".encode()).digest()
    hmac_key = hashlib.sha256(f"{key_material}:HMAC".encode()).digest()
    return aes_key, hmac_key


def _encrypt_local_payload(
    payload: dict[str, Any],
    device_secret: str,
    uid: str,
    device_id: str,
) -> str:
    """Encrypt local control payload with AES-256-CBC."""
    aes_key, _ = _derive_local_keys(device_secret, uid, device_id)
    payload_bytes = json.dumps(payload, separators=(",", ":")).encode()
    padding_size = 16 - (len(payload_bytes) % 16)
    padded_payload = payload_bytes + bytes([padding_size]) * padding_size

    iv = os.urandom(16)
    encryptor = Cipher(algorithms.AES(aes_key), modes.CBC(iv)).encryptor()
    ciphertext = encryptor.update(padded_payload) + encryptor.finalize()
    return base64.b64encode(iv + ciphertext).decode("ascii")


def _sign_local_payload(
    encrypted_data: str,
    device_secret: str,
    uid: str,
    device_id: str,
) -> str:
    """Sign encrypted local control payload with HMAC-SHA256."""
    _, hmac_key = _derive_local_keys(device_secret, uid, device_id)
    signature = hmac.new(
        hmac_key,
        encrypted_data.encode(),
        hashlib.sha256,
    ).digest()
    return base64.b64encode(signature).decode("ascii")


def _local_status_headers(
    uid: str, device_id: str, device_secret: str
) -> dict[str, str]:
    """Build signed local status headers matching the EvolvIOT app."""
    timestamp = str(int(time.time()))
    nonce = base64.urlsafe_b64encode(os.urandom(12)).decode("ascii").rstrip("=")
    canonical = f"GET\n/status\n{timestamp}\n{nonce}\n{device_id}"
    _, hmac_key = _derive_local_keys(device_secret, uid, device_id)
    signature = hmac.new(
        hmac_key,
        canonical.encode(),
        hashlib.sha256,
    ).digest()
    return {
        "X-Evolv-Timestamp": timestamp,
        "X-Evolv-Nonce": nonce,
        "X-Evolv-Signature": base64.b64encode(signature).decode("ascii"),
    }


class EvolvIOTApi:
    """Small async client for `/api/homeassistant`."""

    def __init__(
        self,
        session: ClientSession,
        api_base_url: str,
        access_token: str = "",
        *,
        refresh_token: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        health_url: str = DEFAULT_HEALTH_URL,
        verify_ssl: bool = True,
        token_update_callback: TokenUpdateCallback | None = None,
    ) -> None:
        self._session = session
        self.api_base_url = normalize_api_base_url(api_base_url)
        self.health_url = health_url.strip().rstrip("/") or DEFAULT_HEALTH_URL
        self.access_token = access_token.strip()
        self.refresh_token = (refresh_token or "").strip()
        self.client_id = (client_id or "").strip()
        self.client_secret = (client_secret or "").strip()
        self.verify_ssl = verify_ssl
        self._token_update_callback = token_update_callback
        self._ws: ClientWebSocketResponse | None = None
        self._ws_task: asyncio.Task[None] | None = None
        self._ws_ready: asyncio.Future[dict[str, Any]] | None = None
        self._ws_pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._ws_request_id = 0
        self._ws_stop_requested = False
        self._ws_on_ready: WebSocketMessageCallback | None = None
        self._ws_on_state_changed: WebSocketMessageCallback | None = None
        self._local_clients: dict[str, LocalDeviceSocket] = {}
        self.local_transports: dict[str, str] = {}
        self.local_status_callback: (
            Callable[[str, dict[str, Any] | None], None] | None
        ) = None

    @property
    def websocket_started(self) -> bool:
        """Return true when the WebSocket listener has been started."""
        return self._ws_task is not None and not self._ws_task.done()

    @property
    def websocket_connected(self) -> bool:
        """Return true when the WebSocket is currently connected."""
        return self._ws is not None and not self._ws.closed

    async def async_validate(self) -> dict[str, Any]:
        """Validate cloud reachability and the supplied bearer token."""
        await self.async_health()
        return await self.async_get_devices()

    async def async_refresh_token(self) -> bool:
        """Refresh the access token when possible."""
        return await self._async_refresh_token()

    async def async_health(self) -> None:
        """Check backend health."""
        try:
            async with self._session.get(
                self.health_url,
                ssl=None if self.verify_ssl else False,
            ) as response:
                response.raise_for_status()
                await response.text()
        except ClientResponseError as err:
            raise EvolvIOTApiError(f"EvolvIOT API returned HTTP {err.status}") from err
        except ClientError as err:
            raise EvolvIOTConnectionError("Could not connect to EvolvIOT") from err

    async def async_exchange_authorization_code(
        self,
        authorization_code: str,
        client_id: str,
        client_secret: str,
    ) -> dict[str, Any]:
        """Exchange an OAuth authorization code for tokens."""
        try:
            async with self._session.post(
                f"{self.api_base_url}/oauth/token",
                data={
                    "grant_type": "authorization_code",
                    "code": authorization_code,
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
                ssl=None if self.verify_ssl else False,
            ) as response:
                if response.status in (401, 403):
                    raise EvolvIOTAuthError("Invalid EvolvIOT OAuth credentials")
                response.raise_for_status()
                data = await response.json(content_type=None)
                if not isinstance(data, dict) or not data.get("access_token"):
                    raise EvolvIOTAuthError(
                        "Token response did not include access token"
                    )
                return data
        except EvolvIOTApiError:
            raise
        except ClientResponseError as err:
            raise EvolvIOTAuthError("Invalid OAuth authorization code") from err
        except ClientError as err:
            raise EvolvIOTConnectionError("Could not connect to EvolvIOT") from err

    async def async_start_device_authorization(self) -> dict[str, Any]:
        """Start app-based Home Assistant pairing."""
        try:
            async with self._session.post(
                f"{self.api_base_url}/device/authorize",
                ssl=None if self.verify_ssl else False,
            ) as response:
                response.raise_for_status()
                data = await response.json(content_type=None)
                return data if isinstance(data, dict) else {}
        except ClientResponseError as err:
            raise EvolvIOTApiError(f"EvolvIOT API returned HTTP {err.status}") from err
        except ClientError as err:
            raise EvolvIOTConnectionError("Could not connect to EvolvIOT") from err

    async def async_exchange_device_code(self, device_code: str) -> dict[str, Any]:
        """Exchange an approved device code for access and refresh tokens."""
        try:
            async with self._session.post(
                f"{self.api_base_url}/oauth/token",
                data={
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                    "device_code": device_code,
                },
                ssl=None if self.verify_ssl else False,
            ) as response:
                data = await response.json(content_type=None)
                if response.status >= 400:
                    error = str((data or {}).get("error") or "")
                    if error in {"authorization_pending", "slow_down"}:
                        raise EvolvIOTDeviceAuthorizationPending(
                            "Device authorization is pending"
                        )
                    if error == "access_denied":
                        raise EvolvIOTDeviceAuthorizationDenied(
                            "Device authorization was denied"
                        )
                    if error in {"expired_token", "invalid_grant"}:
                        raise EvolvIOTDeviceAuthorizationExpired(
                            "Device authorization expired"
                        )
                    raise EvolvIOTAuthError(error or "Device authorization failed")

                if not isinstance(data, dict) or not data.get("access_token"):
                    raise EvolvIOTAuthError(
                        "Token response did not include access token"
                    )
                return data
        except EvolvIOTApiError:
            raise
        except ClientError as err:
            raise EvolvIOTConnectionError("Could not connect to EvolvIOT") from err

    async def async_get_devices(self) -> dict[str, Any]:
        """Fetch entities available to the authenticated user."""
        if self.websocket_connected:
            data = await self.async_ws_request({"type": "devices"})
            return {"entities": data.get("entities", [])}
        return await self._request("get", "/devices")

    async def async_get_states(self) -> dict[str, Any]:
        """Fetch states for all entities."""
        if self.websocket_connected:
            data = await self.async_ws_request({"type": "devices.states"})
            return {"states": data.get("states", [])}
        return await self._request("get", "/devices/states")

    async def async_get_state(self, entity_id: str) -> dict[str, Any]:
        """Fetch one entity state."""
        if self.websocket_connected:
            return await self.async_ws_request(
                {"type": "device.state", "entity_id": entity_id}
            )
        safe_entity_id = quote(entity_id, safe="")
        return await self._request("get", f"/devices/{safe_entity_id}/state")

    async def async_command(
        self, entity_id: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """Send a command to one entity."""
        try:
            if self.websocket_connected:
                result = await self.async_ws_request(
                    {
                        "type": "device.command",
                        "entity_id": entity_id,
                        "payload": payload,
                    }
                )
            else:
                safe_entity_id = quote(entity_id, safe="")
                result = await self._request(
                    "post",
                    f"/devices/{safe_entity_id}/command",
                    json=payload,
                    timeout=ClientTimeout(total=DEFAULT_WS_REQUEST_TIMEOUT),
                )
        except EvolvIOTCommandUnavailable:
            raise
        except EvolvIOTConnectionError as err:
            if isinstance(err.__cause__, ClientConnectorError):
                raise EvolvIOTCommandUnavailable(str(err)) from err
            raise EvolvIOTCommandUncertain(
                "Cloud command acknowledgement lost; command will not be repeated"
            ) from err
        except TimeoutError as err:
            raise EvolvIOTCommandUncertain("Cloud command timed out") from err
        if result.get("success") is False or result.get("s") == "error":
            raise EvolvIOTApiError(
                str(
                    result.get("reason")
                    or result.get("message")
                    or "Cloud rejected command"
                )
            )
        return result

    async def async_start_websocket(
        self,
        on_ready: WebSocketMessageCallback,
        on_state_changed: WebSocketMessageCallback,
    ) -> None:
        """Start the Home Assistant raw WebSocket listener."""
        self._ws_on_ready = on_ready
        self._ws_on_state_changed = on_state_changed
        self._ws_stop_requested = False
        if self.websocket_started:
            return

        self._ws_ready = asyncio.get_running_loop().create_future()
        self._ws_task = asyncio.create_task(self._async_ws_listen_loop())

    async def async_wait_websocket_ready(
        self,
        timeout: int = DEFAULT_WS_READY_TIMEOUT,
    ) -> dict[str, Any]:
        """Wait for the next WebSocket ready payload."""
        if self._ws_ready is None:
            raise EvolvIOTConnectionError("EvolvIOT WebSocket is not started")
        try:
            return await asyncio.wait_for(asyncio.shield(self._ws_ready), timeout)
        except asyncio.TimeoutError as err:
            raise EvolvIOTConnectionError(
                "Timed out waiting for EvolvIOT WebSocket"
            ) from err

    async def async_ws_request(
        self,
        message: dict[str, Any],
        timeout: int = DEFAULT_WS_REQUEST_TIMEOUT,
    ) -> dict[str, Any]:
        """Send a request over the WebSocket and wait for its response."""
        if not self.websocket_connected or self._ws is None:
            raise EvolvIOTCommandUnavailable("EvolvIOT WebSocket is not connected")

        self._ws_request_id += 1
        request_id = str(message.get("id") or f"ha-{self._ws_request_id}")
        payload = {**message, "id": request_id}
        future: asyncio.Future[dict[str, Any]] = (
            asyncio.get_running_loop().create_future()
        )
        self._ws_pending[request_id] = future

        try:
            await self._ws.send_json(payload)
            return await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError as err:
            self._ws_pending.pop(request_id, None)
            raise EvolvIOTConnectionError(
                "Timed out waiting for EvolvIOT WebSocket response"
            ) from err
        except (ClientError, OSError, RuntimeError) as err:
            self._ws_pending.pop(request_id, None)
            raise EvolvIOTConnectionError(
                "Could not send EvolvIOT WebSocket request"
            ) from err
        finally:
            self._ws_pending.pop(request_id, None)
            if not future.done():
                future.cancel()

    async def async_close(self) -> None:
        """Close open EvolvIOT connections."""
        self.local_status_callback = None
        await asyncio.gather(
            *(client.close() for client in self._local_clients.values())
        )
        self._local_clients.clear()
        self._ws_stop_requested = True
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        if self._ws_task is not None:
            self._ws_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._ws_task
        self._ws_task = None
        self._ws = None
        self._fail_pending_ws_requests(
            EvolvIOTConnectionError("EvolvIOT WebSocket closed")
        )

    async def _async_ws_listen_loop(self) -> None:
        """Maintain a reconnecting WebSocket connection."""
        reconnect_index = 0
        while not self._ws_stop_requested:
            try:
                await self._async_ws_connect_once()
                reconnect_index = 0
                await self._async_ws_receive_messages()
            except asyncio.CancelledError:
                raise
            except EvolvIOTAuthError:
                if not await self._async_refresh_token():
                    self._set_ws_ready_exception(
                        EvolvIOTAuthError("Invalid EvolvIOT WebSocket credentials")
                    )
                    await asyncio.sleep(DEFAULT_WS_RECONNECT_DELAYS[-1])
                continue
            except EvolvIOTApiError as err:
                self._set_ws_ready_exception(err)
                _LOGGER.debug("EvolvIOT WebSocket disconnected: %s", err)
            finally:
                self._fail_pending_ws_requests(
                    EvolvIOTConnectionError("EvolvIOT WebSocket disconnected")
                )
                self._ws = None

            delay = DEFAULT_WS_RECONNECT_DELAYS[
                min(reconnect_index, len(DEFAULT_WS_RECONNECT_DELAYS) - 1)
            ]
            reconnect_index += 1
            await asyncio.sleep(delay)

    async def _async_ws_connect_once(self) -> None:
        """Open one authenticated WebSocket connection."""
        if not self.access_token:
            raise EvolvIOTAuthError("Missing EvolvIOT access token")

        if self._ws_ready is None:
            self._ws_ready = asyncio.get_running_loop().create_future()

        headers = {"Authorization": f"Bearer {self.access_token}"}
        try:
            self._ws = await self._session.ws_connect(
                self.websocket_url,
                headers=headers,
                ssl=None if self.verify_ssl else False,
                timeout=DEFAULT_WS_CONNECT_TIMEOUT,
                heartbeat=DEFAULT_WS_HEARTBEAT,
            )
        except WSServerHandshakeError as err:
            if err.status in (401, 403):
                raise EvolvIOTAuthError(
                    "Invalid EvolvIOT WebSocket credentials"
                ) from err
            raise EvolvIOTConnectionError(
                f"EvolvIOT WebSocket returned HTTP {err.status}"
            ) from err
        except (ClientError, TimeoutError) as err:
            raise EvolvIOTConnectionError(
                "Could not connect to EvolvIOT WebSocket"
            ) from err

    async def _async_ws_receive_messages(self) -> None:
        """Handle messages until the active WebSocket closes."""
        if self._ws is None:
            raise EvolvIOTConnectionError("EvolvIOT WebSocket is not connected")

        async for message in self._ws:
            if message.type == WSMsgType.TEXT:
                try:
                    data = message.json()
                except ValueError:
                    _LOGGER.debug("Ignoring invalid EvolvIOT WebSocket JSON")
                    continue
                await self._async_handle_ws_message(data)
                continue
            if message.type in (WSMsgType.CLOSED, WSMsgType.CLOSE, WSMsgType.ERROR):
                break

        raise EvolvIOTConnectionError("EvolvIOT WebSocket closed")

    async def _async_handle_ws_message(self, message: Any) -> None:
        """Route one WebSocket message."""
        if not isinstance(message, dict):
            return

        message_type = str(message.get("type") or "")
        request_id = message.get("id")

        if message_type == "ready":
            if self._ws_ready is not None and not self._ws_ready.done():
                self._ws_ready.set_result(message)
            if self._ws_on_ready is not None:
                await self._ws_on_ready(message)
            return

        if message_type == "state_changed":
            if self._ws_on_state_changed is not None:
                await self._ws_on_state_changed(message)
            return

        if message_type == "command_result" and isinstance(message.get("state"), dict):
            if self._ws_on_state_changed is not None:
                await self._ws_on_state_changed(
                    {"type": "state_changed", "state": message["state"]}
                )

        if request_id is None:
            return

        future = self._ws_pending.pop(str(request_id), None)
        if future is None or future.done():
            return

        if message_type == "error":
            future.set_exception(
                EvolvIOTApiError(
                    str(
                        message.get("message")
                        or message.get("error")
                        or "WebSocket error"
                    )
                )
            )
            return

        future.set_result(message)

    @property
    def websocket_url(self) -> str:
        """Return the raw Home Assistant WebSocket URL for the API host."""
        parsed = urlsplit(self.api_base_url)
        scheme = "wss" if parsed.scheme == "https" else "ws"
        api_path = parsed.path.rstrip("/")
        for suffix in ("/api/homeassistant", "/api"):
            if api_path.endswith(suffix):
                api_path = api_path[: -len(suffix)]
                break
        path = f"{api_path}/homeassistant-ws" if api_path else "/homeassistant-ws"
        return urlunsplit(
            (
                scheme,
                parsed.netloc,
                path,
                urlencode({"access_token": self.access_token}),
                "",
            )
        )

    def _set_ws_ready_exception(self, err: BaseException) -> None:
        """Fail the current ready wait if one is active."""
        if self._ws_ready is not None and not self._ws_ready.done():
            self._ws_ready.set_exception(err)

    def _fail_pending_ws_requests(self, err: BaseException) -> None:
        """Fail outstanding WebSocket request futures."""
        for future in self._ws_pending.values():
            if not future.done():
                future.set_exception(err)
        self._ws_pending.clear()

    def _local_host(self, device_id: str, host: str | None = None) -> str:
        """Use an automatically supplied LAN IP or the device mDNS hostname."""
        address = host
        if address:
            try:
                ip = ipaddress.ip_address(address)
            except ValueError:
                pass
            else:
                return f"[{ip}]" if ip.version == 6 else str(ip)
        return f"{LOCAL_MDNS_DOMAIN}-{_sanitize_device_id_for_mdns(device_id)}.local"

    async def async_prune_local_clients(self, device_ids: set[str]) -> None:
        """Release connections for devices no longer in the account."""
        for device_id in set(self._local_clients) - device_ids:
            await self._local_clients.pop(device_id).close()
            self.local_transports.pop(device_id, None)

    async def _async_local_client(
        self, device_id: str, host: str | None
    ) -> LocalDeviceSocket:
        """Get a shared native device connection, preferring a valid cached LAN IP."""
        mdns = f"{LOCAL_MDNS_DOMAIN}-{_sanitize_device_id_for_mdns(device_id)}.local"
        primary = self._local_host(device_id, host)
        hosts = [primary]
        if primary != mdns:
            hosts.append(mdns)
        urls = [f"ws://{name}/ws" for name in hosts]
        client = self._local_clients.get(device_id)
        if client is not None and client.urls != urls:
            await client.close()
            client = None
        if client is None:

            def on_status(data: dict[str, Any] | None) -> None:
                if data is not None:
                    self.local_transports[device_id] = "local_ws"
                elif self.local_transports.get(device_id) == "local_ws":
                    self.local_transports.pop(device_id, None)
                if self.local_status_callback is not None:
                    self.local_status_callback(device_id, data)

            client = LocalDeviceSocket(
                self._session,
                urls,
                on_status,
                discovery_timeout=DEFAULT_LOCAL_DISCOVERY_TIMEOUT,
            )
            self._local_clients[device_id] = client
        return client

    async def async_local_command(
        self,
        *,
        uid: str,
        device_id: str,
        endpoint: str,
        device_secret: str,
        switch_name: str,
        value: int | float | bool,
        host: str | None = None,
    ) -> str:
        """Try native WebSocket, then HTTP only if nothing was sent."""
        client = await self._async_local_client(device_id, host)
        data = _encrypt_local_payload(
            {"key": switch_name, "value": value}, device_secret, uid, device_id
        )
        envelope = {
            "data": data,
            "hmac": _sign_local_payload(data, device_secret, uid, device_id),
        }
        try:
            await client.send_command(switch_name, envelope)
        except LocalSocketUnavailable:
            await self._async_local_http_command(
                uid=uid,
                device_id=device_id,
                endpoint=endpoint,
                device_secret=device_secret,
                switch_name=switch_name,
                value=value,
                host=host,
            )
            self.local_transports[device_id] = "local_http"
            return "local_http"
        except LocalCommandRejected as err:
            raise EvolvIOTApiError(str(err)) from err
        except LocalCommandUncertain as err:
            raise EvolvIOTCommandUncertain(str(err)) from err
        self.local_transports[device_id] = "local_ws"
        return "local_ws"

    async def _async_local_http_command(
        self,
        *,
        uid: str,
        device_id: str,
        endpoint: str,
        device_secret: str,
        switch_name: str,
        value: int | float | bool,
        host: str | None = None,
    ) -> None:
        """Send an encrypted command directly to an ESP over local HTTP."""
        local_payload = {
            "switchName": switch_name,
            "value": value,
        }
        encrypted_data = _encrypt_local_payload(
            local_payload,
            device_secret,
            uid,
            device_id,
        )
        signature = _sign_local_payload(
            encrypted_data,
            device_secret,
            uid,
            device_id,
        )
        safe_endpoint = quote(endpoint.strip("/"), safe="")
        local_host = self._local_host(device_id, host)
        url = f"http://{local_host}/{safe_endpoint}"
        timeout = (
            DEFAULT_LOCAL_DISCOVERY_TIMEOUT
            if local_host.endswith(".local")
            else DEFAULT_LOCAL_COMMAND_TIMEOUT
        )

        try:
            _LOGGER.debug("Sending EvolvIOT local command to %s", url)
            async with self._session.post(
                url,
                json={"data": encrypted_data, "hmac": signature},
                timeout=ClientTimeout(total=timeout),
            ) as response:
                response.raise_for_status()
                await response.text()
        except ClientResponseError as err:
            raise EvolvIOTApiError(
                f"EvolvIOT local API returned HTTP {err.status}"
            ) from err
        except ClientConnectorError as err:
            raise EvolvIOTCommandUnavailable(
                "Could not connect to EvolvIOT device locally"
            ) from err
        except (asyncio.TimeoutError, ClientError) as err:
            raise EvolvIOTCommandUncertain(
                "Local HTTP command acknowledgement lost"
            ) from err

    async def async_local_status(
        self,
        *,
        uid: str,
        device_id: str,
        device_secret: str,
        host: str | None = None,
    ) -> dict[str, Any]:
        """Read a fresh native snapshot, falling back to signed HTTP status."""
        client = await self._async_local_client(device_id, host)
        try:
            return await client.request_status()
        except LocalSocketUnavailable:
            try:
                data = await self._async_local_http_status(
                    uid=uid,
                    device_id=device_id,
                    device_secret=device_secret,
                    host=host,
                )
            except EvolvIOTApiError:
                self.local_transports.pop(device_id, None)
                raise
            self.local_transports[device_id] = "local_http"
            return data

    async def _async_local_http_status(
        self,
        *,
        uid: str,
        device_id: str,
        device_secret: str,
        host: str | None = None,
    ) -> dict[str, Any]:
        """Check device status directly over local HTTP."""
        local_host = self._local_host(device_id, host)
        url = f"http://{local_host}/status"
        timeout = (
            DEFAULT_LOCAL_DISCOVERY_TIMEOUT
            if local_host.endswith(".local")
            else DEFAULT_LOCAL_COMMAND_TIMEOUT
        )
        headers = _local_status_headers(uid, device_id, device_secret)

        try:
            _LOGGER.debug("Checking EvolvIOT local status at %s", url)
            async with self._session.get(
                url,
                headers=headers,
                timeout=ClientTimeout(total=timeout),
            ) as response:
                response.raise_for_status()
                data = await response.json(content_type=None)
                return data if isinstance(data, dict) else {}
        except ClientResponseError as err:
            raise EvolvIOTApiError(
                f"EvolvIOT local API returned HTTP {err.status}"
            ) from err
        except (asyncio.TimeoutError, ClientError, ValueError) as err:
            raise EvolvIOTConnectionError(
                "Could not connect to EvolvIOT device locally"
            ) from err

    async def _request(
        self,
        method: str,
        path: str,
        *,
        auth: bool = True,
        retry_auth: bool = True,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Run an HTTP request and return JSON."""
        headers = dict(kwargs.pop("headers", {}))
        if auth:
            headers["Authorization"] = f"Bearer {self.access_token}"

        url = f"{self.api_base_url}{path}"
        kwargs.setdefault("timeout", ClientTimeout(total=DEFAULT_WS_REQUEST_TIMEOUT))
        try:
            async with self._session.request(
                method.upper(),
                url,
                headers=headers,
                ssl=None if self.verify_ssl else False,
                **kwargs,
            ) as response:
                if response.status in (401, 403):
                    if auth and retry_auth and await self._async_refresh_token():
                        return await self._request(
                            method,
                            path,
                            auth=auth,
                            retry_auth=False,
                            **kwargs,
                        )
                    raise EvolvIOTAuthError("Invalid EvolvIOT credentials")

                response.raise_for_status()
                data = await response.json(content_type=None)
                return data if isinstance(data, dict) else {}
        except EvolvIOTApiError:
            raise
        except ClientResponseError as err:
            raise EvolvIOTApiError(f"EvolvIOT API returned HTTP {err.status}") from err
        except (ClientError, TimeoutError, ValueError) as err:
            raise EvolvIOTConnectionError("Could not connect to EvolvIOT") from err

    async def _async_refresh_token(self) -> bool:
        """Refresh the access token when OAuth credentials are available."""
        if not self.refresh_token:
            return False

        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
        }
        if self.client_id:
            data["client_id"] = self.client_id
        if self.client_secret:
            data["client_secret"] = self.client_secret

        try:
            async with self._session.post(
                f"{self.api_base_url}/oauth/token",
                data=data,
                ssl=None if self.verify_ssl else False,
                timeout=ClientTimeout(total=DEFAULT_WS_REQUEST_TIMEOUT),
            ) as response:
                if response.status >= 400:
                    return False
                token_data = await response.json(content_type=None)
        except (ClientError, TimeoutError):
            return False

        access_token = str(token_data.get("access_token") or "").strip()
        if not access_token:
            return False

        self.access_token = access_token
        self.refresh_token = str(
            token_data.get("refresh_token") or self.refresh_token
        ).strip()

        if self._token_update_callback is not None:
            await self._token_update_callback(
                {
                    "access_token": self.access_token,
                    "refresh_token": self.refresh_token,
                }
            )

        return True
