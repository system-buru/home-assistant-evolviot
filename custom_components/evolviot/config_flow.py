"""Config flow for EvolvIOT."""

from __future__ import annotations

import asyncio
import time
from typing import Any
from urllib.parse import quote

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import (
    EvolvIOTApi,
    EvolvIOTAuthError,
    EvolvIOTConnectionError,
    EvolvIOTDeviceAuthorizationDenied,
    EvolvIOTDeviceAuthorizationExpired,
    EvolvIOTDeviceAuthorizationPending,
    normalize_api_base_url,
)
from .const import (
    CONF_ACCESS_TOKEN,
    CONF_API_BASE_URL,
    CONF_REFRESH_TOKEN,
    CONF_VERIFY_SSL,
    DEFAULT_API_BASE_URL,
    DOMAIN,
    NAME,
)


def _connection_schema(user_input: dict[str, Any] | None = None) -> vol.Schema:
    values = user_input or {}
    return vol.Schema(
        {
            vol.Required(
                CONF_API_BASE_URL,
                default=values.get(CONF_API_BASE_URL, DEFAULT_API_BASE_URL),
            ): str,
            vol.Optional(CONF_VERIFY_SSL, default=values.get(CONF_VERIFY_SSL, True)): bool,
        }
    )


class EvolvIOTConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle an EvolvIOT config flow."""

    VERSION = 1

    def __init__(self) -> None:
        self._api_base_url = DEFAULT_API_BASE_URL
        self._verify_ssl = True
        self._pairing: dict[str, Any] = {}
        self._poll_task: asyncio.Task[dict[str, Any]] | None = None

    def _api(self) -> EvolvIOTApi:
        session = async_get_clientsession(self.hass, verify_ssl=self._verify_ssl)
        return EvolvIOTApi(
            session,
            self._api_base_url,
            verify_ssl=self._verify_ssl,
        )

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ):
        """Start app-based pairing."""
        errors: dict[str, str] = {}

        if user_input is not None:
            self._api_base_url = normalize_api_base_url(user_input[CONF_API_BASE_URL])
            self._verify_ssl = bool(user_input.get(CONF_VERIFY_SSL, True))

            try:
                await self._async_start_pairing()
            except EvolvIOTConnectionError:
                errors["base"] = "cannot_connect"
            except Exception:
                errors["base"] = "unknown"
                return self.async_show_form(
                    step_id="user",
                    data_schema=_connection_schema(user_input),
                    errors=errors,
                )

            return await self.async_step_pair()

        return self.async_show_form(
            step_id="user",
            data_schema=_connection_schema(user_input),
            errors=errors,
        )

    async def async_step_pair(
        self, user_input: dict[str, Any] | None = None
    ):
        """Show pairing details while polling for app approval."""
        if not self._pairing:
            return await self.async_step_user()

        if self._poll_task is not None and self._poll_task.done():
            return self.async_show_progress_done(next_step_id="pair_done")

        return self.async_show_progress(
            step_id="pair",
            progress_action="pair",
            progress_task=self._poll_task,
            description_placeholders=self._pair_description_placeholders(),
        )

    async def async_step_pair_done(
        self, user_input: dict[str, Any] | None = None
    ):
        """Finish pairing after polling completes."""
        if self._poll_task is None:
            return await self.async_step_user()

        try:
            result = self._poll_task.result()
        except EvolvIOTDeviceAuthorizationExpired:
            try:
                await self._async_start_pairing()
            except EvolvIOTConnectionError:
                return self._restart_with_error("cannot_connect")
            except Exception:
                return self._restart_with_error("unknown")
            return await self.async_step_pair()
        except EvolvIOTDeviceAuthorizationDenied:
            return self._restart_with_error("authorization_denied")
        except EvolvIOTAuthError:
            return self._restart_with_error("invalid_auth")
        except EvolvIOTConnectionError:
            return self._restart_with_error("cannot_connect")
        except Exception:
            return self._restart_with_error("unknown")

        unique_id = str(result["payload"].get("user_id") or self._api_base_url)
        await self.async_set_unique_id(unique_id)
        self._abort_if_unique_id_configured()
        return self.async_create_entry(
            title=NAME,
            data={
                CONF_API_BASE_URL: self._api_base_url,
                CONF_ACCESS_TOKEN: result["access_token"],
                CONF_REFRESH_TOKEN: result["refresh_token"],
                CONF_VERIFY_SSL: self._verify_ssl,
            },
        )

    async def _async_poll_pairing(self) -> dict[str, Any]:
        """Poll EvolvIOT until the app approves pairing or the code expires."""
        expires_in = max(1, int(self._pairing.get("expires_in") or 600))
        interval = max(1, int(self._pairing.get("interval") or 5))
        deadline = time.monotonic() + expires_in
        device_code = str(self._pairing["device_code"])

        while time.monotonic() < deadline:
            try:
                token_data = await self._api().async_exchange_device_code(
                    device_code
                )
                access_token = str(token_data.get("access_token") or "").strip()
                refresh_token = str(token_data.get("refresh_token") or "").strip()
                api = EvolvIOTApi(
                    async_get_clientsession(self.hass, verify_ssl=self._verify_ssl),
                    self._api_base_url,
                    access_token,
                    refresh_token=refresh_token,
                    verify_ssl=self._verify_ssl,
                )
                payload = await api.async_validate()
                return {
                    "access_token": access_token,
                    "refresh_token": refresh_token,
                    "payload": payload,
                }
            except EvolvIOTDeviceAuthorizationPending:
                await asyncio.sleep(min(interval, max(0, deadline - time.monotonic())))

        raise EvolvIOTDeviceAuthorizationExpired("Pairing expired")

    async def _async_start_pairing(self) -> None:
        """Start a fresh pairing session and polling task."""
        self._pairing = await self._api().async_start_device_authorization()
        self._poll_task = self.hass.async_create_task(self._async_poll_pairing())

    def _pair_description_placeholders(self) -> dict[str, str]:
        """Return placeholders shown in the pairing progress step."""
        qr_payload = str(
            self._pairing.get("qr_payload")
            or self._pairing.get("verification_uri_complete")
            or self._pairing.get("user_code")
            or ""
        )
        qr_image_url = (
            "https://api.qrserver.com/v1/create-qr-code/?size=240x240&data="
            f"{quote(qr_payload)}"
            if qr_payload
            else ""
        )

        return {
            "user_code": str(self._pairing.get("user_code") or ""),
            "verification_uri": str(
                self._pairing.get("verification_uri_complete")
                or self._pairing.get("verification_uri")
                or ""
            ),
            "qr_image_url": qr_image_url,
            "expires_in": str(self._pairing.get("expires_in") or ""),
        }

    def _restart_with_error(self, error: str):
        """Reset pairing state and show the first step with an error."""
        self._pairing = {}
        self._poll_task = None
        return self.async_show_form(
            step_id="user",
            data_schema=_connection_schema(
                {
                    CONF_API_BASE_URL: self._api_base_url,
                    CONF_VERIFY_SSL: self._verify_ssl,
                }
            ),
            errors={"base": error},
        )

    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> config_entries.OptionsFlow:
        """Return the options flow."""
        return EvolvIOTOptionsFlow(config_entry)


class EvolvIOTOptionsFlow(config_entries.OptionsFlow):
    """Handle EvolvIOT options updates."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        self.config_entry = config_entry

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ):
        """Update stored connection details."""
        if user_input is not None:
            data = {
                **self.config_entry.data,
                CONF_API_BASE_URL: normalize_api_base_url(user_input[CONF_API_BASE_URL]),
                CONF_VERIFY_SSL: bool(user_input.get(CONF_VERIFY_SSL, True)),
            }
            self.hass.config_entries.async_update_entry(self.config_entry, data=data)
            await self.hass.config_entries.async_reload(self.config_entry.entry_id)
            return self.async_create_entry(title="", data={})

        return self.async_show_form(
            step_id="init",
            data_schema=_connection_schema(dict(self.config_entry.data)),
        )
