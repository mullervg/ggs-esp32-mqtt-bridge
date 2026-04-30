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
#define MQTT_TOPIC_BASE       "grow/GGS"
#define HA_DISCOVERY_PREFIX   "homeassistant"
#define HA_DEVICE_ID          "ggs_esp32_mqtt_bridge"
#define HA_DEVICE_NAME        "Spider Farmer GGS"
#define HA_DEVICE_MODEL       "SF-GGS-CB"
#define HA_DEVICE_MANUFACTURER "Spider Farmer"
