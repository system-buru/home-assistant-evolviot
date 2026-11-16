"""Data coordinator for EvolvIOT."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import EvolvIOTApi, EvolvIOTApiError, local_device_host
from .const import DOMAIN, STORAGE_KEY_PREFIX, STORAGE_VERSION

_LOGGER = logging.getLogger(__name__)


class EvolvIOTDataUpdateCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Fetch EvolvIOT entities and states on a shared schedule."""

    def __init__(self, hass: HomeAssistant, api: EvolvIOTApi, entry_id: str) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=None,
        )
        self.api = api
        self.api.local_status_callback = self._handle_local_status
        self._store: Store[dict[str, Any]] = Store(
            hass,
            STORAGE_VERSION,
            f"{STORAGE_KEY_PREFIX}.{entry_id}",
        )
        self._cached_devices_payload: dict[str, Any] | None = None
        self._local_snapshots: dict[str, dict[str, Any]] = {}

    async def async_periodic_refresh(self, _now: Any) -> None:
        """Refresh independently of pushes, which reset coordinator poll timers."""
        await self.async_request_refresh()

    async def async_load_cache(self) -> None:
        """Load cached device metadata for offline startup."""
        stored = await self._store.async_load()
        if not isinstance(stored, dict):
            return

        devices_payload = stored.get("devices_payload")
        if isinstance(devices_payload, dict):
            self._cached_devices_payload = devices_payload

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
        if not self.api.websocket_started:
            try:
                await self.api.async_start_websocket(
                    self._async_handle_ws_ready,
                    self._async_handle_ws_state_changed,
                )
                ready_payload = await self.api.async_wait_websocket_ready()
                return await self._async_data_from_ws_ready(ready_payload)
            except EvolvIOTApiError as err:
                _LOGGER.debug("Falling back to EvolvIOT HTTP refresh: %s", err)

        return await self._async_http_update_data()

    async def _async_http_update_data(self) -> dict[str, Any]:
        """Fetch fresh data from EvolvIOT over HTTP or WS request fallback."""
        states_payload: dict[str, Any] = {}
        cloud_error: EvolvIOTApiError | None = None

        try:
            devices_payload = await self.api.async_get_devices()
        except EvolvIOTApiError as err:
            cloud_error = err
            devices_payload = self._cached_devices_payload or {}
        else:
            if not devices_payload.get("user_id") and isinstance(self.data, dict):
                devices_payload["user_id"] = self.data.get("user_id")
            await self._async_save_cached_devices_payload(devices_payload)
            try:
                states_payload = await self.api.async_get_states()
            except EvolvIOTApiError as err:
                cloud_error = err

        return await self._async_merge_payloads(
            devices_payload,
            states_payload,
            cloud_error,
        )

    async def _async_data_from_ws_ready(
        self, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """Build coordinator data from a WebSocket ready payload."""
        devices_payload = {
            "user_id": payload.get("user_id"),
            "entities": payload.get("entities")
            if isinstance(payload.get("entities"), list)
            else [],
        }
        states_payload = {
            "states": payload.get("states")
            if isinstance(payload.get("states"), list)
            else [],
        }
        await self._async_save_cached_devices_payload(devices_payload)
        return await self._async_merge_payloads(devices_payload, states_payload)

    async def _async_merge_payloads(
        self,
        devices_payload: dict[str, Any],
        states_payload: dict[str, Any],
        cloud_error: EvolvIOTApiError | None = None,
    ) -> dict[str, Any]:
        """Merge entity metadata, cloud states, and local status checks."""
        entities = {
            str(entity.get("entity_id")): entity
            for entity in devices_payload.get("entities", [])
            if entity.get("entity_id")
        }

        if not entities and cloud_error is not None:
            raise UpdateFailed(str(cloud_error)) from cloud_error

        cloud_states = {
            str(state.get("entity_id")): state
            for state in states_payload.get("states", [])
            if state.get("entity_id")
        }
        states = {}
        for entity_id in entities:
            state = dict(
                cloud_states.get(entity_id)
                or self.states.get(entity_id)
                or {"entity_id": entity_id}
            )
            cloud_available = entity_id in cloud_states and bool(
                state.get("available", True)
            )
            state.update(
                cloud_available=cloud_available,
                available=cloud_available,
                local_available=False,
                local_transport=None,
            )
            states[entity_id] = state

        # Cloud-first applies to status discovery too. Do not continuously
        # probe every LAN device when its cloud state is already available.
        local_entities = {
            entity_id: entity
            for entity_id, entity in entities.items()
            if not states[entity_id]["cloud_available"]
        }
        active_device_ids = {
            str((entity.get("device") or {}).get("id") or "")
            for entity in local_entities.values()
            if (
                (entity.get("device") or {}).get("local_control")
                or entity.get("local_control")
                or {}
            ).get("enabled")
            is not False
        }
        self._local_snapshots = {
            device_id: snapshot
            for device_id, snapshot in self._local_snapshots.items()
            if device_id in active_device_ids
        }
        await self.api.async_prune_local_clients(active_device_ids)
        local_statuses = await self._async_local_statuses(
            str(devices_payload.get("user_id") or ""),
            local_entities,
        )

        for entity_id, local_status in local_statuses.items():
            state = dict(states.get(entity_id) or {"entity_id": entity_id})
            is_available = bool(local_status.get("available"))
            state["local_available"] = is_available
            state["local_transport"] = local_status.get("transport")
            if is_available:
                state["available"] = True
            elif "available" not in state:
                state["available"] = False
            if "value" in local_status and not state.get("cloud_available"):
                self._apply_local_state(
                    state,
                    entities[entity_id],
                    local_status["value"],
                )
            states[entity_id] = state

        return {
            "user_id": devices_payload.get("user_id"),
            "entities": entities,
            "states": states,
        }

    async def _async_handle_ws_ready(self, payload: dict[str, Any]) -> None:
        """Apply a WebSocket ready payload to Home Assistant."""
        data = await self._async_data_from_ws_ready(payload)
        self.async_set_updated_data(data)

    async def _async_handle_ws_state_changed(self, payload: dict[str, Any]) -> None:
        """Apply a pushed WebSocket state update."""
        state = payload.get("state")
        if not isinstance(state, dict):
            return

        entity_id = str(state.get("entity_id") or "")
        if not entity_id:
            return

        data = dict(self.data or {})
        if not data:
            return

        states = dict(data.get("states") or {})
        previous = states.get(entity_id) or {}
        state = {**previous, **state}
        state["cloud_available"] = bool(payload["state"].get("available", True))
        state["available"] = state["cloud_available"] or bool(
            state.get("local_available")
        )
        entity = self.entities.get(entity_id)
        if entity is not None and not state["cloud_available"]:
            device_id = str((entity.get("device") or {}).get("id") or "")
            snapshot = self._local_snapshots.get(device_id)
            if snapshot is not None and state.get("local_available"):
                value = self._local_value_for_entity(entity, snapshot)
                if value is not None:
                    self._apply_local_state(state, entity, value)
        states[entity_id] = state
        data["states"] = states
        self.async_set_updated_data(data)

    def _handle_local_status(
        self, device_id: str, payload: dict[str, Any] | None
    ) -> None:
        """Apply live device snapshots to every entity on that physical device."""
        if payload is not None:
            self._local_snapshots[device_id] = payload
        else:
            self._local_snapshots.pop(device_id, None)
            if self.api.local_transports.get(device_id) == "local_http":
                # A failed WS reconnect must not invalidate a reachable HTTP device.
                return
        if not self.data:
            return
        data = dict(self.data)
        states = dict(self.states)
        for entity_id, entity in self.entities.items():
            if str((entity.get("device") or {}).get("id") or "") != device_id:
                continue
            state = dict(states.get(entity_id) or {"entity_id": entity_id})
            state["local_available"] = payload is not None
            state["local_transport"] = "local_ws" if payload is not None else None
            state["available"] = (
                bool(state.get("cloud_available")) or payload is not None
            )
            if payload is not None and not state.get("cloud_available"):
                value = self._local_value_for_entity(entity, payload)
                if value is not None:
                    self._apply_local_state(state, entity, value)
            states[entity_id] = state
        data["states"] = states
        self.async_set_updated_data(data)

    async def _async_save_cached_devices_payload(
        self,
        devices_payload: dict[str, Any],
    ) -> None:
        """Persist device metadata needed for local-only operation."""
        if not devices_payload.get("entities"):
            return

        self._cached_devices_payload = devices_payload
        await self._store.async_save({"devices_payload": devices_payload})

    async def _async_local_statuses(
        self,
        uid: str,
        entities: dict[str, dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        """Fetch local status once per physical local-capable device."""
        if not uid:
            return {}

        device_checks: dict[tuple[str, str], asyncio.Task[dict[str, Any] | None]] = {}
        entity_devices: dict[str, tuple[str, str]] = {}

        for entity_id, entity in entities.items():
            device = entity.get("device") or {}
            local_control = (
                device.get("local_control") or entity.get("local_control") or {}
            )
            if local_control.get("enabled") is False:
                continue

            device_id = str(device.get("id") or "").strip()
            device_secret = str(
                local_control.get("device_secret")
                or local_control.get("deviceSecret")
                or ""
            ).strip()
            if not device_id or not device_secret:
                continue

            key = (device_id, device_secret)
            entity_devices[entity_id] = key
            if key not in device_checks:
                device_checks[key] = self.hass.async_create_task(
                    self._async_local_device_status(
                        uid,
                        device_id,
                        device_secret,
                        local_device_host(device, local_control),
                    )
                )

        if not device_checks:
            return {}

        results = await asyncio.gather(*device_checks.values())
        status_by_device = dict(zip(device_checks.keys(), results, strict=True))
        local_statuses: dict[str, dict[str, Any]] = {}

        for entity_id, device_key in entity_devices.items():
            local_data = status_by_device.get(device_key)
            local_status: dict[str, Any] = {"available": local_data is not None}
            local_status["transport"] = self.api.local_transports.get(device_key[0])
            if local_data is not None:
                local_value = self._local_value_for_entity(
                    entities[entity_id], local_data
                )
                if local_value is not None:
                    local_status["value"] = local_value
            local_statuses[entity_id] = local_status

        return local_statuses

    async def _async_local_device_status(
        self,
        uid: str,
        device_id: str,
        device_secret: str,
        host: str | None = None,
    ) -> dict[str, Any] | None:
        """Return local device status data when reachable."""
        try:
            return await self.api.async_local_status(
                uid=uid,
                device_id=device_id,
                device_secret=device_secret,
                host=host,
            )
        except EvolvIOTApiError:
            return None

    def _local_value_for_entity(
        self,
        entity: dict[str, Any],
        local_data: dict[str, Any],
    ) -> Any:
        """Return this entity's value from a local device status payload."""
        device = entity.get("device") or {}
        local_control = device.get("local_control") or entity.get("local_control") or {}
        control = entity.get("control") or {}
        candidates = [
            local_control.get("status_key"),
            local_control.get("statusKey"),
            local_control.get("switch_name"),
            local_control.get("switchName"),
            control.get("key"),
            control.get("name"),
            control.get("appliance_name"),
        ]

        for candidate in candidates:
            key = str(candidate or "").strip()
            if key and key in local_data:
                return local_data[key]

        normalized_local_data = {
            self._normalize_local_status_key(key): value
            for key, value in local_data.items()
        }
        for candidate in candidates:
            normalized_key = self._normalize_local_status_key(candidate)
            if normalized_key and normalized_key in normalized_local_data:
                return normalized_local_data[normalized_key]
        return None

    @staticmethod
    def _normalize_local_status_key(value: Any) -> str:
        """Normalize local status keys for tolerant matching."""
        return "".join(
            character for character in str(value or "").lower() if character.isalnum()
        )

    def _apply_local_state(
        self,
        state: dict[str, Any],
        entity: dict[str, Any],
        value: Any,
    ) -> None:
        """Apply a local status value to a Home Assistant state payload."""
        domain = str(entity.get("domain") or "")
        attributes = dict(state.get("attributes") or {})
        state["raw_value"] = value

        if domain in {"switch", "light", "binary_sensor", "fan"}:
            numeric_value: float | None
            try:
                numeric_value = float(value)
            except (TypeError, ValueError):
                numeric_value = None
                normalized = str(value).lower()
                is_on = normalized in {"on", "true", "1"}
            else:
                is_on = numeric_value > 0

            state["state"] = "on" if is_on else "off"
            if domain == "fan" and numeric_value is not None:
                attributes["percentage"] = max(0, min(100, round(numeric_value)))
            if (
                domain == "light"
                and entity.get("capabilities", {}).get("supports_brightness")
                and numeric_value is not None
            ):
                attributes["brightness"] = max(0, min(100, round(numeric_value)))
        else:
            state["state"] = value

        if attributes:
            state["attributes"] = attributes
