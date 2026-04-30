# GGS BLE protocol notes

These notes describe the protocol behavior observed while integrating a Spider Farmer / GGS controller with an ESP32 bridge.

## BLE characteristics

| Purpose | UUID |
|---|---|
| Notify | `0000ff01-0000-1000-8000-00805f9b34fb` |
| Write | `0000ff02-0000-1000-8000-00805f9b34fb` |

The bridge subscribes to the notify characteristic and writes commands to the write characteristic.

## Message types

The controller uses two message formats over the same write/notify channel.

### Raw JSON

Small commands can be written directly as UTF-8 JSON.

Example:

```json
{"method":"getDevSta","params":{},"msgId":"123"}
```

This is useful for simple read requests such as `getDevSta`.

### AA AA framed payloads

Large JSON payloads, especially `getConfigFile` responses and `setConfigFile` writes, are carried in binary frames.

Observed frame layout:

```text
AA AA
frame_type:u16be
body_len:u16be
body
frame_crc:u16be
```

Observed body layout:

```text
field_a:u8
field_b:u8
payload_crc:u16be
total_len:u32be
offset:u32be
chunk_len:u16be
chunk:bytes
```

The ESP32 firmware reconstructs the payload by `(payload_crc, total_len)` and accepts frames out of order or with duplicated chunks.

## CRC

The project uses CRC16/MODBUS:

```text
poly reflected: 0xA001
init: 0xFFFF
```

For RX, the firmware accepts the final frame CRC calculated either over the complete frame without its final CRC or over the frame without the `AA AA` prefix. This is intentional because firmware/app variants have shown small differences in CRC scope.

## Control strategy

The project controls devices through this flow:

```text
getConfigFile -> modify full configFile -> setConfigFile -> verify with getConfigFile/getDevSta
```

Direct methods such as `setFan`, `setBlower`, or `setLight` are deliberately avoided in the ESP32 firmware because they may acknowledge successfully without applying the expected real device state on some firmware versions.

## Device sections

Typical config sections include:

```text
device.light
  mOnOff
  mLevel
  modeType
  lastAutoModeType
  timePeriod[0].enabled
  timePeriod[0].weekmask
  timePeriod[0].startTime
  timePeriod[0].endTime
  timePeriod[0].brightness

device.fan
  mOnOff
  mLevel
  shakeLevel

device.blower
  mOnOff
  mLevel and/or maxSpeed
```

The firmware preserves the full config file and only mutates the minimum required fields for the requested target.

## MQTT command model

Commands are accepted under:

```text
grow/GGS/cmd/...
```

State is published under:

```text
grow/GGS/state/...
```

The firmware clears retained command topics before subscribing. This prevents stale retained Home Assistant commands from replaying unexpectedly after an ESP32 reboot.
