# EvolvIOT Home Assistant Integration

Use EvolvIOT smart home devices in Home Assistant.

## Installation

1. Open **HACS** in Home Assistant.
2. Add this repository as a custom repository.
3. Choose the **Integration** category.
4. Install **EvolvIOT**.
5. Restart Home Assistant.
6. Go to **Settings > Devices & services > Add integration**.
7. Search for **EvolvIOT** and start setup.

## Setup

During setup, Home Assistant shows a QR code and a pairing code.

In the EvolvIOT app:

1. Open **Profile**.
2. Open **Third party services**.
3. Open **Home Assistant**.
4. Select **Scan QR**.
5. Scan the QR code shown in Home Assistant.
6. After the app shows success, select **Submit** in Home Assistant.

If the pairing code expires, Home Assistant will generate a new QR code and pairing code.

## Supported Devices

The integration supports EvolvIOT entities exposed as:

- Switches
- Lights
- Fans
- Sensors
- Binary sensors

Brightness and fan speed are available when supported by the connected device.

## How Control Works

Commands use EvolvIOT cloud control first. When cloud control is unavailable, Home Assistant connects directly to the device's native `/ws` WebSocket on the local network. If that connection is unavailable before sending a command, it uses the existing encrypted local HTTP API.

Device WebSockets provide live status updates. Local HTTP status is checked every 15 seconds when WebSocket status is unavailable. Previously synced device metadata allows local control after a Home Assistant restart without internet access; initial pairing still requires the cloud.

Commands are sent through one path at a time. If an acknowledgement is lost after a command was sent, the integration refreshes status and reports the uncertain result instead of repeating the command. Device rejections are reported without trying another transport.

Entity attributes show `connection_mode` as `cloud`, `local_ws`, `local_http`, or `offline`.

The device's **Connection Mode** sensor displays `local live` for a local WebSocket connection and `local` for local HTTP control.

Device addresses are selected automatically from synced device metadata or the device's `evolviot-<device-id>.local` hostname. No manual IP entry is required. Local discovery runs only for entities unavailable through the cloud.

When testing in WSL, multicast/mDNS must be able to reach the devices on the LAN. WSL NAT networking can prevent discovery even when cloud control works.

You do not need to choose online or offline mode. The integration uses the available path automatically.

## Troubleshooting

### Account Already Configured

Home Assistant only allows one setup entry for the same EvolvIOT account.

To pair again, remove the existing EvolvIOT integration from **Settings > Devices & services**, then add it again.

### Icon Not Available

Home Assistant may show `icon not available` in the integration picker until EvolvIOT brand assets are available through Home Assistant's brands service. This does not affect pairing or device control.

### Cannot Connect

Check that:

- Home Assistant has internet access.
- The EvolvIOT service is reachable.
- SSL verification is enabled unless you are using a controlled local test environment.

### Local Control Is Not Working

Check that:

- Home Assistant and the EvolvIOT device are on the same local network.
- Your network allows mDNS/Bonjour device discovery.
- The device is powered on and connected to Wi-Fi.
- The device has been synced in the EvolvIOT app.

## Security

- The QR code is generated locally inside Home Assistant.
- The pairing payload is not sent to a third-party QR service.
- Local commands are encrypted before being sent to the device.
- Authentication data is stored by Home Assistant for this integration.
- Do not share screenshots that show a valid QR code or pairing code.
