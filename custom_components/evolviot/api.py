"""API client for the EvolvIOT Home Assistant cloud endpoints."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Awaitable, Callable
import contextlib
import hashlib
import hmac
import json
import logging
import os
import re
import time
from typing import Any
from urllib.parse import quote, urlencode, urlsplit, urlunsplit

from aiohttp import (
    ClientError,
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
    DEFAULT_WS_CONNECT_TIMEOUT,
    DEFAULT_WS_HEARTBEAT,
    DEFAULT_WS_READY_TIMEOUT,
    DEFAULT_WS_RECONNECT_DELAYS,
    DEFAULT_WS_REQUEST_TIMEOUT,
    LOCAL_MDNS_DOMAIN,
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


def _local_status_headers(uid: str, device_id: str, device_secret: str) -> dict[str, str]:
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
            raise EvolvIOTApiError(
                f"EvolvIOT API returned HTTP {err.status}"
            ) from err
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
                    raise EvolvIOTAuthError("Token response did not include access token")
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
            raise EvolvIOTApiError(
                f"EvolvIOT API returned HTTP {err.status}"
            ) from err
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
                    raise EvolvIOTAuthError("Token response did not include access token")
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
        if self.websocket_connected:
            return await self.async_ws_request(
                {
                    "type": "device.command",
                    "entity_id": entity_id,
                    "payload": payload,
                }
            )
        safe_entity_id = quote(entity_id, safe="")
        return await self._request(
            "post", f"/devices/{safe_entity_id}/command", json=payload
        )

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
            raise EvolvIOTConnectionError("EvolvIOT WebSocket is not connected")

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
        except ClientError as err:
            self._ws_pending.pop(request_id, None)
            raise EvolvIOTConnectionError(
                "Could not send EvolvIOT WebSocket request"
            ) from err

    async def async_close(self) -> None:
        """Close open EvolvIOT connections."""
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
                raise EvolvIOTAuthError("Invalid EvolvIOT WebSocket credentials") from err
            raise EvolvIOTConnectionError(
                f"EvolvIOT WebSocket returned HTTP {err.status}"
            ) from err
        except ClientError as err:
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

    async def async_local_command(
        self,
        *,
        uid: str,
        device_id: str,
        endpoint: str,
        device_secret: str,
        switch_name: str,
        value: int | float | bool,
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
        safe_device_id = _sanitize_device_id_for_mdns(device_id)
        url = f"http://{LOCAL_MDNS_DOMAIN}-{safe_device_id}.local/{safe_endpoint}"

        try:
            _LOGGER.debug("Sending EvolvIOT local command to %s", url)
            async with self._session.post(
                url,
                json={"data": encrypted_data, "hmac": signature},
                timeout=ClientTimeout(total=DEFAULT_LOCAL_COMMAND_TIMEOUT),
            ) as response:
                response.raise_for_status()
                await response.text()
        except ClientResponseError as err:
            raise EvolvIOTApiError(
                f"EvolvIOT local API returned HTTP {err.status}"
            ) from err
        except (asyncio.TimeoutError, ClientError) as err:
            raise EvolvIOTConnectionError(
                "Could not connect to EvolvIOT device locally"
            ) from err

    async def async_local_status(
        self,
        *,
        uid: str,
        device_id: str,
        device_secret: str,
    ) -> dict[str, Any]:
        """Check device status directly over local HTTP."""
        safe_device_id = _sanitize_device_id_for_mdns(device_id)
        url = f"http://{LOCAL_MDNS_DOMAIN}-{safe_device_id}.local/status"
        headers = _local_status_headers(uid, device_id, device_secret)

        try:
            _LOGGER.debug("Checking EvolvIOT local status at %s", url)
            async with self._session.get(
                url,
                headers=headers,
                timeout=ClientTimeout(total=DEFAULT_LOCAL_COMMAND_TIMEOUT),
            ) as response:
                response.raise_for_status()
                data = await response.json(content_type=None)
                return data if isinstance(data, dict) else {}
        except ClientResponseError as err:
            raise EvolvIOTApiError(
                f"EvolvIOT local API returned HTTP {err.status}"
            ) from err
        except (asyncio.TimeoutError, ClientError) as err:
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
            raise EvolvIOTApiError(
                f"EvolvIOT API returned HTTP {err.status}"
            ) from err
        except ClientError as err:
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
            ) as response:
                if response.status >= 400:
                    return False
                token_data = await response.json(content_type=None)
        except ClientError:
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
