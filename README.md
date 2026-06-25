# EvolvIOT Home Assistant Integration

Custom Home Assistant integration for EvolvIOT smart home devices.

This repository is structured for HACS:

```text
custom_components/evolviot/
brands/evolviot/icon.png
hacs.json
README.md
```

HACS requires one integration under `custom_components/`, with the required integration files inside that directory. The current HACS publishing docs also require brand assets and prefer GitHub releases for versioned updates.

## Install With HACS

1. Open HACS in Home Assistant.
2. Add this repository as a custom repository.
3. Choose the `Integration` category.
4. Install `EvolvIOT`.
5. Restart Home Assistant.
6. Go to **Settings > Devices & services > Add integration > EvolvIOT**.

## Configuration

The integration talks only to the EvolvIOT Home Assistant route:

```text
https://api.evolviot.com/api/homeassistant
```

During setup, provide:

- API base URL
- SSL verification preference

Home Assistant then starts a short-lived pairing session. The user scans the QR code with the EvolvIOT app, or enters the pairing code manually in the app. After the app approves the pairing, Home Assistant exchanges the device code for access and refresh tokens.

Do not hardcode OAuth client secrets in this public repository. The app-based device-code flow does not require a public HACS integration to ship a client secret.

## Backend Routes Used

The integration uses the backend routes already exposed under `/api/homeassistant`:

- `GET /status`
- `POST /device/authorize`
- `POST /device/approve`
- `POST /oauth/token` with `grant_type=urn:ietf:params:oauth:grant-type:device_code`
- `POST /oauth/token` with `grant_type=refresh_token`
- `GET /devices`
- `GET /devices/states`
- `GET /devices/:entityId/state`
- `POST /devices/:entityId/command`

## EvolvIOT App Pairing Contract

Start pairing:

```http
POST /api/homeassistant/device/authorize
```

Response:

```json
{
  "device_code": "uuid",
  "user_code": "ABCD-2345",
  "verification_uri": "https://evolviot.com/home-assistant/link",
  "verification_uri_complete": "https://evolviot.com/home-assistant/link?user_code=ABCD-2345&platform=homeassistant",
  "qr_payload": "https://evolviot.com/home-assistant/link?user_code=ABCD-2345&platform=homeassistant",
  "expires_in": 600,
  "interval": 5
}
```

Approve from logged-in EvolvIOT app:

```http
POST /api/homeassistant/device/approve
Authorization: Bearer <app user access token>
Content-Type: application/json

{
  "user_code": "ABCD-2345"
}
```

Home Assistant polls:

```http
POST /api/homeassistant/oauth/token
Content-Type: application/x-www-form-urlencoded

grant_type=urn:ietf:params:oauth:grant-type:device_code
device_code=<device_code>
```

Pending response:

```json
{ "error": "authorization_pending", "interval": 5 }
```

Approved response:

```json
{
  "access_token": "...",
  "refresh_token": "...",
  "token_type": "Bearer",
  "expires_in": 3600
}
```

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

## Backend Environment

Your backend must be configured with:

```text
HOME_ASSISTANT_OAUTH_CLIENT_ID=...
HOME_ASSISTANT_OAUTH_CLIENT_SECRET=...
HOME_ASSISTANT_OAUTH_REDIRECT_URIS=...
HOME_ASSISTANT_DEVICE_AUTH_EXPIRES_IN_SECONDS=600
HOME_ASSISTANT_DEVICE_AUTH_POLL_INTERVAL_SECONDS=5
HOME_ASSISTANT_TOKEN_EXPIRES_IN_SECONDS=3600
FRONTEND_URL=...
```

The public integration should never contain those secret values.

## References

- HACS integration publishing: https://www.hacs.xyz/docs/publish/integration/
