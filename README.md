# EvolvIOT Home Assistant Integration

Custom Home Assistant integration for EvolvIOT smart home devices.

This repository is structured for HACS:

```text
custom_components/evolviot/
hacs.json
README.md
```

HACS requires one integration under `custom_components/`, with the required integration files inside that directory. The integration includes a local `icon.png`, but Home Assistant's **Add integration** picker can still show `icon not available` until the EvolvIOT brand assets are published through the Home Assistant brands service.

## Install With HACS

1. Open HACS in Home Assistant.
2. Add this repository as a custom repository.
3. Choose the `Integration` category.
4. Install `EvolvIOT`.
5. Restart Home Assistant.
6. Go to **Settings > Devices & services > Add integration > EvolvIOT**.

## Configuration

By default, the integration talks to the EvolvIOT Home Assistant route:

```text
https://api.evolviot.com/api/homeassistant
```

It checks backend reachability with:

```text
https://api.evolviot.com/health
```

During setup, Home Assistant starts a short-lived pairing session and shows a QR code plus a pairing code.

In the EvolvIOT app:

1. Open **Profile**.
2. Open **Third party services**.
3. Open **Home Assistant**.
4. Select **Scan QR**.
5. Scan the QR code shown in Home Assistant.
6. After the app shows success, select **Submit** in Home Assistant.

If the pairing code expires before approval, Home Assistant requests a fresh code and QR payload. If Home Assistant says the account is already configured, remove the existing EvolvIOT integration from **Settings > Devices & services** before adding it again.

## Security Notes

- Pairing QR codes are generated locally inside Home Assistant and embedded in the setup dialog. The pairing payload is not sent to a third-party QR service.
- Access and refresh tokens are stored in the Home Assistant config entry, which is the normal storage path for Home Assistant integrations.
- Avoid disabling SSL verification except in a controlled local development environment.

## Backend Routes Used

The Home Assistant integration calls this backend health route:

- `GET /health`

It also calls these routes under `/api/homeassistant`:

- `POST /device/authorize`
- `POST /oauth/token` with `grant_type=urn:ietf:params:oauth:grant-type:device_code`
- `POST /oauth/token` with `grant_type=refresh_token`
- `GET /devices`
- `GET /devices/states`
- `GET /devices/:entityId/state`
- `POST /devices/:entityId/command`

## Supported Entities

EvolvIOT backend entity domains are mapped to Home Assistant platforms:

- `switch`
- `light`
- `fan`
- `sensor`
- `binary_sensor`

Brightness and fan speed are supported when the backend returns the corresponding capability flags.

## Publishing Updates

For HACS users, publish GitHub releases such as:

- `v1.0.0`
- `v1.0.1`

HACS can use the default branch without releases, but releases provide a cleaner update experience.

## References

- HACS integration publishing: https://www.hacs.xyz/docs/publish/integration/
