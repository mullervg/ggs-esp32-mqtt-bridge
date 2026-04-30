# GGS ESP32 MQTT Bridge

An ESP32 BLE-to-MQTT bridge for a Spider Farmer / GGS controller, with Home Assistant MQTT Discovery support.

The bridge reads controller state over BLE, publishes sensor/device state to MQTT, and controls the light, fan, and blower locally without using the vendor cloud.

## Features

- BLE → MQTT bridge for the GGS controller
- Local control, no cloud dependency
- Home Assistant MQTT Discovery entities
- Temperature, humidity, and VPD publishing
- Light on/off and brightness control
- Light manual/time-slot mode control
- Light schedule settings: enabled, start hour, end hour, brightness
- Fan on/off, gear, percentage, and oscillation control
- Blower on/off and percentage control
- Raw JSON and `AA AA` framed BLE message support
- Full `configFile`-based control with post-write verification
- Retained MQTT command cleanup to avoid stale command replay

## Repository structure

```text
ggs-esp32-mqtt-bridge/
├── esp32/
│   ├── ggs_bridge.ino
│   └── config.example.h
├── python/
│   ├── test_ble.py
│   └── sniff_protocol.py
├── docs/
│   └── protocol.md
├── README.md
├── .gitignore
└── config.example.h
```

`config.example.h` is duplicated at the repository root and inside `esp32/` for convenience. For Arduino IDE usage, copy the one inside `esp32/` to `esp32/config.h`.

## How it works

The controller exposes two BLE characteristics:

| Purpose | UUID |
|---|---|
| Notify | `0000ff01-0000-1000-8000-00805f9b34fb` |
| Write | `0000ff02-0000-1000-8000-00805f9b34fb` |

Small commands can be sent as raw JSON. Large messages use a binary `AA AA` frame format that chunks and reassembles JSON payloads.

The ESP32 firmware intentionally controls devices by reading the full `configFile`, modifying only the required fields, sending the full config back with `setConfigFile`, then verifying the result. This is more reliable than direct commands such as `setFan`, which may acknowledge but not apply correctly depending on firmware behavior.

## Setup

### 1. Install Arduino dependencies

Install the following libraries in Arduino IDE or PlatformIO:

- `WiFi` for ESP32
- `PubSubClient`
- `ArduinoJson`
- ESP32 BLE Arduino APIs

Use an ESP32 board package compatible with the BLE classes used by the sketch.

### 2. Create local config

Copy the example config:

```bash
cp esp32/config.example.h esp32/config.h
```

Edit `esp32/config.h`:

```cpp
#define WIFI_SSID "YOUR_WIFI_SSID"
#define WIFI_PASS "YOUR_WIFI_PASSWORD"

#define MQTT_HOST "YOUR_MQTT_HOST"
#define MQTT_PORT 1883
#define MQTT_USER "YOUR_MQTT_USER"
#define MQTT_PASS "YOUR_MQTT_PASSWORD"

#define GGS_TARGET_NAME "SF-GGS-CB"
#define GGS_TARGET_MAC  "AA:BB:CC:DD:EE:FF"

#define GGS_PID   "YOUR_GGS_PID"
#define GGS_UID   "YOUR_GGS_UID"
#define GGS_PCODE 1004
```

Leave `GGS_TARGET_MAC` as the placeholder if you prefer matching by BLE name only.

Never commit `esp32/config.h`.

### 3. Flash the ESP32

Open `esp32/ggs_bridge.ino` in Arduino IDE, select your ESP32 board and serial port, then upload.

### 4. MQTT / Home Assistant

The default topic base is:

```text
grow/GGS
```

The bridge subscribes to command topics and publishes state topics. It also publishes Home Assistant MQTT Discovery configs under:

```text
homeassistant/...
```

After the first successful BLE bootstrap, entities should appear in Home Assistant automatically if MQTT Discovery is enabled.

## Python tools

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install bleak
```

### Read device state

```bash
python python/test_ble.py \
  --target-name SF-GGS-CB \
  --pid YOUR_GGS_PID \
  --uid YOUR_GGS_UID \
  get-dev-sta
```

### Read config file

```bash
python python/test_ble.py \
  --target-name SF-GGS-CB \
  --pid YOUR_GGS_PID \
  --uid YOUR_GGS_UID \
  get-config-file
```

### Set a device through full configFile

```bash
python python/test_ble.py \
  --target-name SF-GGS-CB \
  --pid YOUR_GGS_PID \
  --uid YOUR_GGS_UID \
  set --device blower --on --level 40
```

For fan gear:

```bash
python python/test_ble.py --pid YOUR_GGS_PID --uid YOUR_GGS_UID set --device fan --on --level 5
```

### Sniff notifications

```bash
python python/sniff_protocol.py --target-name SF-GGS-CB --probe
```

Avoid publishing logs containing real BLE MAC addresses, PID, UID, local IPs, or MQTT credentials.

## Security notes

This repository is designed so that secrets live only in local files or environment variables.

Do not commit:

- Wi-Fi SSID/password
- MQTT host/user/password
- local IP addresses
- real BLE MAC addresses if you consider them sensitive
- real GGS PID/UID values
- captured config backups
- raw logs from your local setup

## Limitations

- The GGS protocol is not officially documented.
- `setConfigFile` is safer than direct `setFan`/`setLight`/`setBlower`, but it still depends on firmware behavior.
- The bridge preserves the full config file and refuses obviously partial configs, but unknown firmware variants may add fields that require additional validation.
- BLE stability depends on ESP32 board quality, power supply, distance, and RF noise.
- Home Assistant retained commands are cleared before subscription to avoid stale replay; this is intentional.

## Future improvements

- Add PlatformIO support
- Add unit tests for the `AA AA` frame parser
- Add a small CLI to decode saved BLE logs
- Add optional MQTT TLS support
- Add a web configuration portal for Wi-Fi/MQTT/device IDs
- Add structured JSON logs for easier debugging

## License

Choose and add a license before publishing, for example MIT or Apache-2.0.
