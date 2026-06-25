"""Data coordinator for EvolvIOT."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import EvolvIOTApi, EvolvIOTApiError
from .const import DEFAULT_SCAN_INTERVAL, DOMAIN

_LOGGER = logging.getLogger(__name__)


class EvolvIOTDataUpdateCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Fetch EvolvIOT entities and states on a shared schedule."""

    def __init__(self, hass: HomeAssistant, api: EvolvIOTApi) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=DEFAULT_SCAN_INTERVAL,
        )
        self.api = api

    @property
    def entities(self) -> dict[str, dict[str, Any]]:
        """Return entities keyed by backend entity id."""
        data = self.data or {}
        entities = data.get("entities", {})
        return entities if isinstance(entities, dict) else {}

    @property
    def states(self) -> dict[str, dict[str, Any]]:
        """Return states keyed by backend entity id."""
        data = self.data or {}
        states = data.get("states", {})
        return states if isinstance(states, dict) else {}

    def entities_for_domain(self, domain: str) -> list[dict[str, Any]]:
        """Return entities for one Home Assistant platform domain."""
        return [
            entity
            for entity in self.entities.values()
            if str(entity.get("domain") or "") == domain
        ]

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch fresh data from EvolvIOT."""
        try:
            devices_payload = await self.api.async_get_devices()
            states_payload = await self.api.async_get_states()
        except EvolvIOTApiError as err:
            raise UpdateFailed(str(err)) from err

        entities = {
            str(entity.get("entity_id")): entity
            for entity in devices_payload.get("entities", [])
            if entity.get("entity_id")
        }
        states = {
            str(state.get("entity_id")): state
            for state in states_payload.get("states", [])
            if state.get("entity_id")
        }

        return {
            "user_id": devices_payload.get("user_id"),
            "entities": entities,
            "states": states,
        }
