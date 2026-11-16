"""EvolvIOT Home Assistant integration."""

from __future__ import annotations

from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.event import async_track_time_interval

from .api import EvolvIOTApi, normalize_api_base_url
from .const import (
    CONF_ACCESS_TOKEN,
    CONF_API_BASE_URL,
    CONF_CLIENT_ID,
    CONF_CLIENT_SECRET,
    CONF_REFRESH_TOKEN,
    CONF_VERIFY_SSL,
    DATA_API,
    DATA_COORDINATOR,
    DATA_KNOWN_ENTITIES,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
    PLATFORMS,
)
from .coordinator import EvolvIOTDataUpdateCoordinator


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up EvolvIOT from a config entry."""

    async def async_token_updated(token_data: dict[str, Any]) -> None:
        hass.config_entries.async_update_entry(
            entry,
            data={
                **entry.data,
                CONF_ACCESS_TOKEN: token_data[CONF_ACCESS_TOKEN],
                CONF_REFRESH_TOKEN: token_data.get(CONF_REFRESH_TOKEN, ""),
            },
        )

    verify_ssl = bool(entry.data.get(CONF_VERIFY_SSL, True))
    session = async_get_clientsession(hass, verify_ssl=verify_ssl)
    api = EvolvIOTApi(
        session,
        normalize_api_base_url(entry.data[CONF_API_BASE_URL]),
        entry.data[CONF_ACCESS_TOKEN],
        refresh_token=entry.data.get(CONF_REFRESH_TOKEN),
        client_id=entry.data.get(CONF_CLIENT_ID),
        client_secret=entry.data.get(CONF_CLIENT_SECRET),
        verify_ssl=verify_ssl,
        token_update_callback=async_token_updated,
    )
    coordinator = EvolvIOTDataUpdateCoordinator(hass, api, entry.entry_id)
    try:
        await coordinator.async_load_cache()
        await coordinator.async_config_entry_first_refresh()
    except BaseException:
        await api.async_close()
        raise

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = {
        DATA_API: api,
        DATA_COORDINATOR: coordinator,
        DATA_KNOWN_ENTITIES: {},
    }

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    entry.async_on_unload(
        async_track_time_interval(
            hass, coordinator.async_periodic_refresh, DEFAULT_SCAN_INTERVAL
        )
    )
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload EvolvIOT config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        data = hass.data[DOMAIN].pop(entry.entry_id)
        await data[DATA_API].async_close()
    return unload_ok
