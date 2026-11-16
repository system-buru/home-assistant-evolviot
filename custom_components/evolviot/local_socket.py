"""Persistent native ESP WebSocket client (not the cloud protocol)."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import contextlib
import json
from typing import Any

from aiohttp import ClientError, ClientSession, WSMsgType


class LocalSocketUnavailable(Exception):
    """The transport failed before a command was sent; HTTP may be tried."""


class LocalCommandUncertain(Exception):
    """A sent command may have executed; it must not be replayed."""


class LocalCommandRejected(Exception):
    """The device explicitly rejected a command."""


class LocalDeviceSocket:
    """Share one connection and serialize commands without firmware request IDs."""

    def __init__(
        self,
        session: ClientSession,
        urls: list[str],
        on_status: Callable[[dict[str, Any] | None], None],
        *,
        timeout: float = 3,
        command_timeout: float = 10,
        discovery_timeout: float | None = None,
    ) -> None:
        self._session = session
        self.urls = urls
        self._on_status = on_status
        self._timeout = timeout
        self._command_timeout = command_timeout
        self._discovery_timeout = discovery_timeout or timeout
        self._socket = None
        self._status_waiters: set[asyncio.Future[dict[str, Any]]] = set()
        self._pending: tuple[str, asyncio.Future[dict[str, Any]]] | None = None
        self._command_lock = asyncio.Lock()
        self._closed = False
        self._task = asyncio.create_task(self._listen())

    async def _listen(self) -> None:
        delay = 1
        while not self._closed:
            try:
                # A stale cached IP must not prevent mDNS discovery.
                for index, url in enumerate(self.urls):
                    try:
                        deadline = (
                            self._discovery_timeout
                            if ".local/" in url
                            else self._timeout / len(self.urls)
                        )
                        async with asyncio.timeout(deadline):
                            self._socket = await self._session.ws_connect(url)
                        break
                    except (ClientError, OSError, TimeoutError):
                        if index == len(self.urls) - 1:
                            raise
                delay = 1
                async for message in self._socket:
                    if message.type == WSMsgType.TEXT:
                        try:
                            payload = json.loads(message.data)
                        except (ValueError, TypeError):
                            continue
                        if not isinstance(payload, dict):
                            continue
                        if payload.get("s") in ("ok", "error"):
                            if self._pending is not None:
                                key, future = self._pending
                                if not future.done() and payload.get("key", key) == key:
                                    future.set_result(payload)
                            continue
                        self._on_status(payload)
                        for waiter in tuple(self._status_waiters):
                            if not waiter.done():
                                waiter.set_result(payload)
                    elif message.type in (
                        WSMsgType.ERROR,
                        WSMsgType.CLOSE,
                        WSMsgType.CLOSED,
                    ):
                        break
            except (ClientError, OSError, TimeoutError):
                pass
            finally:
                if self._socket is not None:
                    await self._socket.close()
                    self._socket = None
                if self._pending is not None and not self._pending[1].done():
                    self._pending[1].set_exception(
                        LocalCommandUncertain(
                            "Device disconnected after command was sent"
                        )
                    )
                for waiter in tuple(self._status_waiters):
                    if not waiter.done():
                        waiter.set_exception(
                            LocalSocketUnavailable("Device disconnected")
                        )
                self._on_status(None)
            if not self._closed:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def request_status(self) -> dict[str, Any]:
        """Request a fresh snapshot, including the initial connection snapshot."""
        if self._closed:
            raise LocalSocketUnavailable("Connection closed")
        future = asyncio.get_running_loop().create_future()
        self._status_waiters.add(future)
        try:
            async with asyncio.timeout(self._timeout):
                if self._socket is not None and not self._socket.closed:
                    await self._socket.send_str("status")
                # On a new connection, the firmware sends the initial snapshot.
                # Await the snapshot/disconnect directly so shutdown wakes waiters.
                return await future
        except (ClientError, OSError, TimeoutError) as err:
            if self._socket is not None:
                await self._socket.close()
            raise LocalSocketUnavailable("Device WebSocket status unavailable") from err
        finally:
            self._status_waiters.discard(future)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()

    async def send_command(self, key: str, envelope: dict[str, str]) -> dict[str, Any]:
        """Fall back only when connection readiness fails before sending."""
        async with self._command_lock:
            await self.request_status()
            socket = self._socket
            if socket is None or socket.closed:
                raise LocalSocketUnavailable("Device disconnected before sending")
            future = asyncio.get_running_loop().create_future()
            self._pending = (key, future)
            try:
                async with asyncio.timeout(self._command_timeout):
                    await socket.send_json(envelope)
                    result = await future
                if result["s"] == "error":
                    raise LocalCommandRejected(
                        str(
                            result.get("rsn")
                            or result.get("reason")
                            or "Device rejected command"
                        )
                    )
                return result
            except (ClientError, OSError, TimeoutError) as err:
                raise LocalCommandUncertain(
                    "Device command acknowledgement lost"
                ) from err
            finally:
                self._pending = None
                if not future.done():
                    future.cancel()
                # Drop this connection after timeout/cancellation/disconnect so a late
                # acknowledgement cannot accidentally complete the next command.
                if future.cancelled() or future.exception() is not None:
                    await socket.close()

    async def close(self) -> None:
        """Stop reconnecting and release the socket and pending requests."""
        self._closed = True
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        for waiter in tuple(self._status_waiters):
            if not waiter.done():
                waiter.set_exception(LocalSocketUnavailable("Connection closed"))
