"""Base entities for EvolvIOT."""

from __future__ import annotations

from typing import Any

from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.update_coordinator import CoordinatorEntity

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
        return self.coordinator.entities.get(self._backend_entity_id, self._fallback_entity)

    @property
    def backend_state(self) -> dict[str, Any]:
        """Return latest backend state."""
        return self.coordinator.states.get(self._backend_entity_id, {})

    @property
    def available(self) -> bool:
        """Return availability from the backend."""
        state = self.backend_state
        if not state:
            return False
        return bool(state.get("available", True))

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Expose useful EvolvIOT metadata."""
        entity = self.backend_entity
        state = self.backend_state
        return {
            "evolviot_entity_id": self._backend_entity_id,
            "raw_value": state.get("raw_value"),
            "control": entity.get("control") or {},
        }

    async def _async_send_command(self, payload: dict[str, Any]) -> None:
        """Send a backend command and refresh coordinator data."""
        await self.coordinator.api.async_command(self._backend_entity_id, payload)
        await self.coordinator.async_request_refresh()
