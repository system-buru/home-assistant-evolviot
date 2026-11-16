"""Base entities for EvolvIOT."""

from __future__ import annotations

import asyncio
from typing import Any

from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .api import (
    EvolvIOTAuthError,
    EvolvIOTCommandUnavailable,
    EvolvIOTCommandUncertain,
    local_device_host,
)
from .const import DOMAIN
from .coordinator import EvolvIOTDataUpdateCoordinator


class EvolvIOTEntity(CoordinatorEntity[EvolvIOTDataUpdateCoordinator]):
    """Base EvolvIOT entity."""

    _attr_has_entity_name = False

    def __init__(
        self,
        coordinator: EvolvIOTDataUpdateCoordinator,
        entity: dict[str, Any],
    ) -> None:
        super().__init__(coordinator)
        self._backend_entity_id = str(entity["entity_id"])
        self._fallback_entity = entity
        self._attr_unique_id = str(entity.get("unique_id") or self._backend_entity_id)
        self._attr_name = str(entity.get("name") or self._backend_entity_id)

        device = entity.get("device") or {}
        device_id = str(device.get("id") or self._attr_unique_id)
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, device_id)},
            name=str(device.get("name") or "EvolvIOT Device"),
            manufacturer=str(device.get("manufacturer") or "EvolvIOT"),
            model=str(device.get("model") or "") or None,
        )

    @property
    def backend_entity(self) -> dict[str, Any]:
        """Return latest backend entity metadata."""
        return self.coordinator.entities.get(
            self._backend_entity_id, self._fallback_entity
        )

    @property
    def backend_state(self) -> dict[str, Any]:
        """Return latest backend state."""
        return self.coordinator.states.get(self._backend_entity_id, {})

    @property
    def available(self) -> bool:
        """Return availability from cloud or local device reachability."""
        state = self.backend_state
        if not state:
            return False
        return bool(state.get("available", True) or state.get("local_available"))

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Expose useful EvolvIOT metadata."""
        entity = self.backend_entity
        state = self.backend_state
        local_available = bool(state.get("local_available"))
        cloud_available = bool(
            state.get("cloud_available", state.get("available", True))
        )
        return {
            "evolviot_entity_id": self._backend_entity_id,
            "evolviot_device_id": str((entity.get("device") or {}).get("id") or ""),
            "connection_mode": self._connection_mode(
                cloud_available,
                local_available,
            ),
            "cloud_available": cloud_available,
            "local_available": local_available,
            "raw_value": state.get("raw_value"),
            "control": entity.get("control") or {},
        }

    def _connection_mode(self, cloud_available: bool, local_available: bool) -> str:
        """Return the preferred currently available transport."""
        if cloud_available:
            return "cloud"
        if local_available:
            return self.backend_state.get("local_transport") or "local_http"
        return "offline"

    async def _async_send_command(self, payload: dict[str, Any]) -> None:
        """Use cloud, native device WebSocket, then the legacy local HTTP API."""
        local_command = self._local_command(payload)
        cloud_available = bool(
            self.backend_state.get(
                "cloud_available", self.backend_state.get("available", True)
            )
        )
        mode = "cloud"
        try:
            if cloud_available or local_command is None:
                try:
                    await self.coordinator.api.async_command(
                        self._backend_entity_id, payload
                    )
                except (EvolvIOTCommandUnavailable, EvolvIOTAuthError):
                    if local_command is None:
                        raise
                    mode = await self.coordinator.api.async_local_command(
                        **local_command
                    )
            else:
                mode = await self.coordinator.api.async_local_command(**local_command)
        except EvolvIOTCommandUncertain:
            # Refresh rather than replay: the original command may have executed.
            await self.coordinator.async_request_refresh()
            raise
        self._apply_optimistic_state(payload)
        state = self.coordinator.states.get(self._backend_entity_id)
        if state is not None:
            state["cloud_available"] = mode == "cloud"
            if mode != "cloud":
                state["local_available"] = True
                state["local_transport"] = mode
            self.async_write_ha_state()
        self._schedule_refresh()

    def _local_command(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        """Build local encrypted command details when entity metadata supports it."""
        value = self._local_command_value(payload)
        if value is None:
            return None

        entity = self.backend_entity
        device = entity.get("device") or {}
        local_control = device.get("local_control") or entity.get("local_control") or {}
        if local_control.get("enabled") is False:
            return None

        control = entity.get("control") or {}
        uid = str((self.coordinator.data or {}).get("user_id") or "").strip()
        device_id = str(device.get("id") or "").strip()
        device_secret = str(
            local_control.get("device_secret")
            or local_control.get("deviceSecret")
            or ""
        ).strip()
        switch_name = str(
            local_control.get("switch_name")
            or local_control.get("switchName")
            or local_control.get("status_key")
            or local_control.get("statusKey")
            or control.get("key")
            or ""
        ).strip()
        endpoint = str(local_control.get("endpoint") or "").strip()
        if not endpoint or endpoint == "control":
            endpoint = switch_name

        if not all((uid, device_id, device_secret, switch_name, endpoint)):
            return None

        return {
            "uid": uid,
            "device_id": device_id,
            "endpoint": endpoint,
            "device_secret": device_secret,
            "switch_name": switch_name,
            "value": value,
            "host": local_device_host(device, local_control),
        }

    def _local_command_value(self, payload: dict[str, Any]) -> int | None:
        """Map Home Assistant command payload to local ESP command value."""
        command = payload.get("command")
        if command == "turn_on":
            return 1
        if command == "turn_off":
            return 0
        if "brightness" in payload:
            return max(0, min(100, int(payload["brightness"])))
        if "percentage" in payload:
            return max(0, min(100, int(payload["percentage"])))
        return None

    def _apply_optimistic_state(self, payload: dict[str, Any]) -> None:
        """Reflect a successful command immediately while backend state settles."""
        data = self.coordinator.data
        if not isinstance(data, dict):
            return
        states = data.get("states")
        if not isinstance(states, dict):
            return

        state = dict(states.get(self._backend_entity_id) or {})
        attributes = dict(state.get("attributes") or {})
        command = payload.get("command")

        if command == "turn_on":
            state["state"] = "on"
        elif command == "turn_off":
            state["state"] = "off"
        elif "brightness" in payload:
            state["state"] = "on" if int(payload["brightness"]) > 0 else "off"
            attributes["brightness"] = int(payload["brightness"])
        elif "percentage" in payload:
            state["state"] = "on" if int(payload["percentage"]) > 0 else "off"
            attributes["percentage"] = int(payload["percentage"])

        state["available"] = True
        if attributes:
            state["attributes"] = attributes
        states[self._backend_entity_id] = state
        self.async_write_ha_state()

    def _schedule_refresh(self) -> None:
        """Refresh after a short delay to avoid rendering stale command state."""
        if (
            self.backend_state.get("cloud_available")
            and self.coordinator.api.websocket_connected
        ):
            return
        self.hass.async_create_task(self._async_delayed_refresh())

    async def _async_delayed_refresh(self) -> None:
        """Request a delayed coordinator refresh."""
        await asyncio.sleep(2)
        await self.coordinator.async_request_refresh()
