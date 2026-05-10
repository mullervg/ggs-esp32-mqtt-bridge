````md
# GGS ESP32 MQTT Bridge

An ESP32 BLE-to-MQTT bridge for a Spider Farmer / GGS controller, with Home Assistant MQTT Discovery support.

The bridge reads controller state over BLE, publishes sensor and device state to MQTT, and controls the light, fan, and blower locally without using the vendor cloud.

The project is designed around reliable local-first control, robust BLE synchronization, and safe configuration updates compatible with different GGS firmware behaviors.

---

## Features

- BLE → MQTT bridge for the GGS controller
- Local control without vendor cloud dependency
- Home Assistant MQTT Discovery support
- Temperature, humidity, and VPD publishing
- Light on/off and brightness control
- Light manual/time-slot mode control
- Light schedule configuration support
- Fan on/off, gear, percentage, and oscillation control
- Blower on/off and percentage control
- Raw JSON and `AA AA` framed BLE message support
- Granular `setConfigField`-based updates
- Reliable state synchronization between BLE, MQTT, and Home Assistant
- Post-write verification and automatic state refresh
- Watchdog recovery and reconnect handling
- Retained MQTT command cleanup to avoid stale command replay
- Reduced BLE traffic and lower risk of config corruption

---

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
└── .gitignore
```

The repository includes a safe placeholder configuration file:

```text
esp32/config.example.h
```

Copy it locally to:

```text
esp32/config.h
```

The local `config.h` file is ignored by Git and should never be committed because it contains credentials and device identifiers.

---

## How it works

The controller exposes two BLE characteristics:

| Purpose | UUID |
|---|---|
| Notify | `0000ff01-0000-1000-8000-00805f9b34fb` |
| Write | `0000ff02-0000-1000-8000-00805f9b34fb` |

Small commands can be sent as raw JSON. Larger payloads use a binary `AA AA` frame format that chunks and reassembles JSON messages.

The firmware primarily uses granular `setConfigField` operations to update only the required configuration fields while preserving the remaining controller state.

The bridge maintains a synchronized local state model derived from:

- BLE notifications
- `getDevSta` responses
- configuration snapshots
- MQTT command acknowledgements

After configuration writes, the firmware verifies the applied device state before publishing updates to MQTT and Home Assistant.

This architecture:

- reduces BLE traffic
- lowers the risk of configuration corruption
- improves state consistency
- avoids unnecessary full-config rewrites
- improves compatibility across firmware variants

Large configuration payloads may still use full `configFile` synchronization internally when required by specific firmware behaviors or recovery flows.

---

## Setup

### 1. Install Arduino dependencies

Install the following libraries in Arduino IDE or PlatformIO:

- `WiFi` for ESP32
- `PubSubClient`
- `ArduinoJson`
- ESP32 BLE Arduino APIs

Use an ESP32 board package compatible with the BLE classes used by the sketch.

---

### 2. Create local config

Copy the example config:

```bash
cp esp32/config.example.h esp32/config.h
```

Edit `esp32/config.h`:

```cpp
#pragma once

/*
  Copy this file to esp32/config.h before compiling the Arduino sketch.
  Never commit esp32/config.h because it contains local credentials and device IDs.
*/

// Wi-Fi
#define WIFI_SSID "YOUR_WIFI_SSID"
#define WIFI_PASS "YOUR_WIFI_PASSWORD"

// MQTT broker
#define MQTT_HOST "YOUR_MQTT_HOST"
#define MQTT_PORT 1883
#define MQTT_USER "YOUR_MQTT_USER"
#define MQTT_PASS "YOUR_MQTT_PASSWORD"

// BLE target. Leave GGS_TARGET_MAC empty to match only by advertised name.
#define GGS_TARGET_NAME "SF-GGS-CB"
#define GGS_TARGET_MAC  "AA:BB:CC:DD:EE:FF"

// Device identifiers captured from your controller/app.
// Treat them as device-specific secrets and do not publish real values.
#define GGS_PID   "YOUR_GGS_PID"
#define GGS_UID   "YOUR_GGS_UID"
#define GGS_PCODE 1004

// MQTT namespace and Home Assistant discovery metadata.
#define MQTT_TOPIC_BASE        "grow/GGS"
#define HA_DISCOVERY_PREFIX    "homeassistant"
#define HA_DEVICE_ID           "ggs_esp32_mqtt_bridge"
#define HA_DEVICE_NAME         "Spider Farmer GGS"
#define HA_DEVICE_MODEL        "SF-GGS-CB"
#define HA_DEVICE_MANUFACTURER "Spider Farmer"
```

Leave `GGS_TARGET_MAC` empty or as the placeholder value if you prefer matching by BLE name only.

Never commit `esp32/config.h`.

---

### 3. Flash the ESP32

Open:

```text
esp32/ggs_bridge.ino
```

in Arduino IDE, select your ESP32 board and serial port, then upload the firmware.

---

### 4. MQTT / Home Assistant

The default MQTT topic base is:

```text
grow/GGS
```

The bridge subscribes to MQTT command topics and publishes state topics.

It also publishes Home Assistant MQTT Discovery configs under:

```text
homeassistant/...
```

After the first successful BLE bootstrap, entities should appear automatically in Home Assistant if MQTT Discovery is enabled.

---

## Python tools

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
```

Linux/macOS:

```bash
source .venv/bin/activate
```

Windows:

```bash
.venv\Scripts\activate
```

Install dependencies:

```bash
pip install bleak
```

---

### Read device state

```bash
python python/test_ble.py \
  --target-name SF-GGS-CB \
  --pid YOUR_GGS_PID \
  --uid YOUR_GGS_UID \
  get-dev-sta
```

---

### Read config file

```bash
python python/test_ble.py \
  --target-name SF-GGS-CB \
  --pid YOUR_GGS_PID \
  --uid YOUR_GGS_UID \
  get-config-file
```

---

### Set a device state

Example: blower 40%

```bash
python python/test_ble.py \
  --target-name SF-GGS-CB \
  --pid YOUR_GGS_PID \
  --uid YOUR_GGS_UID \
  set --device blower --on --level 40
```

Example: fan gear 5

```bash
python python/test_ble.py \
  --pid YOUR_GGS_PID \
  --uid YOUR_GGS_UID \
  set --device fan --on --level 5
```

---

### Sniff BLE notifications

```bash
python python/sniff_protocol.py --target-name SF-GGS-CB --probe
```

Avoid publishing logs containing:

- real BLE MAC addresses
- PID/UID values
- MQTT credentials
- local IP addresses
- captured config snapshots

---

## Security notes

This repository is designed so that secrets live only in local files or environment variables.

Do not commit:

- Wi-Fi SSID/password
- MQTT host/user/password
- local IP addresses
- real BLE MAC addresses
- real GGS PID/UID values
- captured config backups
- raw logs from your local setup
- local `config.h` files

---

## Limitations

- The GGS protocol is not officially documented.
- The bridge primarily relies on `setConfigField` operations for safer and smaller updates, but some firmware versions may still require partial or full config synchronization flows depending on device behavior.
- Unknown firmware variants may introduce additional fields requiring validation or compatibility handling.
- BLE stability depends on ESP32 board quality, power supply, distance, and RF noise.
- Home Assistant retained commands are intentionally cleared before subscription to avoid stale replay issues.

---

## Future improvements

- Add PlatformIO support
- Add unit tests for the `AA AA` frame parser
- Add a CLI to decode saved BLE logs
- Add optional MQTT TLS support
- Add a web configuration portal for Wi-Fi/MQTT/device IDs
- Add structured JSON logs for easier debugging
- Improve automatic recovery and BLE reconnection handling
- Add optional OTA firmware updates

---

## License

Choose and add a license before publishing, for example:

- MIT
- Apache-2.0
```
````
