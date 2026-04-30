#include <Arduino.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>

#include <BLEDevice.h>
#include <BLEUtils.h>
#include <BLEClient.h>
#include <BLERemoteCharacteristic.h>
#include <BLERemoteDescriptor.h>
#include <BLEScan.h>
#include <BLEAdvertisedDevice.h>

#include <vector>
#include <map>
#include <math.h>
#include <string.h>

#include "config.h"

/*
  ESP32 bridge for the Spider Farmer / GGS controller.

  The controller accepts two transport shapes over the same BLE write characteristic:
  1. short raw JSON messages, useful for getDevSta/getConfigFile requests;
  2. AA AA framed messages, required for large payloads such as setConfigFile.

  Device control is intentionally implemented through getConfigFile -> modify full
  configFile -> setConfigFile. Direct commands such as setFan/setBlower/setLight may
  acknowledge successfully but are not reliable across firmware versions and were
  observed to fail especially for fan speed changes.
*/

// -----------------------------------------------------------------------------
// BLE UUIDs
// -----------------------------------------------------------------------------
static BLEUUID NOTIFY_UUID("0000ff01-0000-1000-8000-00805f9b34fb");
static BLEUUID WRITE_UUID ("0000ff02-0000-1000-8000-00805f9b34fb");

// -----------------------------------------------------------------------------
// Firmware metadata
// -----------------------------------------------------------------------------
const char* FW_VERSION = "1.2.0-github-sanitized";

// -----------------------------------------------------------------------------
// Device defaults and protocol limits
// -----------------------------------------------------------------------------
const int FAN_OSC_MIN = 1;
const int FAN_OSC_MAX = 10;

const int DEFAULT_LIGHT_LEVEL_ON = 100;
const int DEFAULT_FAN_GEAR_ON = 5;
const int DEFAULT_BLOWER_LEVEL_ON = 50;

const int DEFAULT_LIGHT_SCHEDULE_ENABLED = 1;
const int DEFAULT_LIGHT_SCHEDULE_WEEKMASK = 127;
const int DEFAULT_LIGHT_SCHEDULE_START_H = 20;
const int DEFAULT_LIGHT_SCHEDULE_END_H = 8;
const int DEFAULT_LIGHT_SCHEDULE_BRIGHTNESS = 100;

const size_t MQTT_BUFFER_SIZE = 8192;
const size_t RX_JSON_DOC_CAPACITY = 16384;
const size_t CONFIG_DOC_CAPACITY = 16384;

const size_t RAW_WRITE_LIMIT = 500;
const size_t AAAA_CHUNK_SIZE = 80;
const size_t MAX_RX_STREAM_BUFFER = 8192;
const size_t MAX_AAAA_PAYLOAD_SIZE = 16384;
const size_t RX_NOTIFY_QUEUE_SIZE = 8192;

const unsigned long PROBE_INTERVAL_MS = 30000;
const unsigned long STATE_STALE_TIMEOUT_MS = 90000;
const unsigned long JSON_RX_TIMEOUT_MS = 2500;
const unsigned long AAAA_RX_TIMEOUT_MS = 7000;

const unsigned long CONFIG_TIMEOUT_MS = 8000;
const unsigned long SET_ACK_TIMEOUT_MS = 8000;
const unsigned long VERIFY_TIMEOUT_MS = 10000;
const int MAX_CONFIG_RETRIES = 2;
const int MAX_SET_RETRIES = 1;
const int MAX_VERIFY_RETRIES = 2;

const unsigned long BLE_FRAME_DELAY_MS = 35;
const unsigned long BLE_TX_STUCK_TIMEOUT_MS = 15000;
const unsigned long POST_SET_VERIFY_DELAY_MS = 1500;
const unsigned long MQTT_RECONNECT_INTERVAL_MS = 5000;
const unsigned long QUEUED_START_DELAY_MS = 120;
const unsigned long BLE_BOOTSTRAP_START_DELAY_MS = 900;
const unsigned long BLE_BOOTSTRAP_BETWEEN_REQ_MS = 650;
const unsigned long BLE_BOOTSTRAP_TIMEOUT_MS = 30000;
const unsigned long MQTT_STEP_INTERVAL_MS = 120;
const unsigned long MQTT_FAIL_COOLDOWN_MS = 3000;
const unsigned long BLE_CONNECT_RETRY_BACKOFF_MS = 5000;
const int BLE_CONNECT_REINIT_AFTER_FAILS = 3;

const bool MQTT_DEBUG_ENABLED = true;
const bool MQTT_PUBLISH_RX_RAW_JSON = false;
const size_t MQTT_DEBUG_MAX_LEN = 360;

// -----------------------------------------------------------------------------
// MQTT topics
// -----------------------------------------------------------------------------
String topic(const char* suffix) {
  String s = MQTT_TOPIC_BASE;
  s += suffix;
  return s;
}

String topicState(const char* suffix) { return topic((String("/state") + suffix).c_str()); }
String topicCmd(const char* suffix) { return topic((String("/cmd") + suffix).c_str()); }
String topicDebug(const char* suffix) { return topic((String("/debug") + suffix).c_str()); }

const char* MQTT_COMMAND_SUFFIXES[] = {
  "/cmd/raw",
  "/cmd/getDevSta",
  "/cmd/getConfigFile",
  "/cmd/light/set",
  "/cmd/light/brightness/set",
  "/cmd/light/mode/set",
  "/cmd/light/schedule/enabled/set",
  "/cmd/light/schedule/start_hour/set",
  "/cmd/light/schedule/end_hour/set",
  "/cmd/light/schedule/brightness/set",
  "/cmd/fan/set",
  "/cmd/fan/percentage/set",
  "/cmd/fan/gear/set",
  "/cmd/fan/oscillation/set",
  "/cmd/blower/set",
  "/cmd/blower/percentage/set",
  "/cmd/discovery/republish"
};
const size_t MQTT_COMMAND_TOPIC_COUNT = sizeof(MQTT_COMMAND_SUFFIXES) / sizeof(MQTT_COMMAND_SUFFIXES[0]);

String mqttCommandTopic(size_t index) {
  if (index >= MQTT_COMMAND_TOPIC_COUNT) return String();
  return topic(MQTT_COMMAND_SUFFIXES[index]);
}

// -----------------------------------------------------------------------------
// Global runtime objects
// -----------------------------------------------------------------------------
WiFiClient wifiClient;
PubSubClient mqttClient(wifiClient);

BLEAdvertisedDevice* foundDevice = nullptr;
BLEClient* pClient = nullptr;
BLERemoteCharacteristic* pNotifyChar = nullptr;
BLERemoteCharacteristic* pWriteChar = nullptr;

bool doConnect = false;
bool connected = false;
volatile bool bleDisconnectPending = false;

bool bleTxInProgress = false;
unsigned long bleTxStartedAtMs = 0;
String bleTxLabel = "";

unsigned long bootStartedAtMs = 0;
unsigned long lastMqttRetry = 0;
unsigned long lastScanAt = 0;
unsigned long lastProbeAt = 0;
unsigned long msgCounter = 0;
bool prevMqttConnected = false;
bool mqttBufferConfigured = false;
unsigned long bleConnectRetryAfterMs = 0;
int bleConsecutiveConnectFailures = 0;

bool parserProcessing = false;

bool bootstrapBleActive = false;
bool initialBleBootstrapDone = false;
bool initialConfigReceived = false;
int bootstrapStep = 0;
unsigned long bootstrapStartedAtMs = 0;
unsigned long bootstrapNextActionAtMs = 0;

bool discoveryPending = false;
int discoveryStep = 0;
const int DISCOVERY_TOTAL_STEPS = 15;

bool availabilityPublishPending = false;
bool availabilityValuePending = false;
bool statusPublishPending = false;
bool statePublishPending = false;
unsigned long lastMqttPublishStepAtMs = 0;
unsigned long mqttPublishCooldownUntilMs = 0;
int mqttConsecutiveFails = 0;

// -----------------------------------------------------------------------------
// Notify queue
// -----------------------------------------------------------------------------
static uint8_t rxNotifyQueue[RX_NOTIFY_QUEUE_SIZE];
volatile size_t rxNotifyHead = 0;
volatile size_t rxNotifyTail = 0;
volatile bool rxNotifyOverflow = false;
volatile unsigned long rxNotifyDroppedBytes = 0;
volatile unsigned long rxNotifyCallbacks = 0;
volatile unsigned long rxNotifyBytes = 0;
portMUX_TYPE rxNotifyMux = portMUX_INITIALIZER_UNLOCKED;

// -----------------------------------------------------------------------------
// State model
// -----------------------------------------------------------------------------
struct GgsState {
  bool bleConnected = false;
  float temp = NAN;
  float humi = NAN;
  float vpd = NAN;

  int fanOn = -1;
  int fanLevel = -1;
  int fanPercentage = -1;
  int fanOscillation = -1;

  int blowerOn = -1;
  int blowerLevel = -1;
  int blowerPercentage = -1;

  int lightOn = -1;
  int lightLevel = -1;
  int lightBrightness = -1;

  int lightModeType = -1;
  int lightLastAutoModeType = -1;
  int lightScheduleEnabled = -1;
  int lightScheduleWeekmask = -1;
  int lightScheduleStartTime = -1;
  int lightScheduleEndTime = -1;
  int lightScheduleStartHour = -1;
  int lightScheduleEndHour = -1;
  int lightScheduleBrightness = -1;

  bool devStateValid = false;
  bool configStateValid = false;
  unsigned long lastStateRxMs = 0;
  unsigned long lastConfigRxMs = 0;
};

GgsState currentState;

// -----------------------------------------------------------------------------
// Operation model
// -----------------------------------------------------------------------------
enum OperationState { OP_IDLE, OP_WAITING_CONFIG, OP_SETTING_CONFIG, OP_VERIFYING };

enum TargetKind {
  TARGET_KIND_DEVICE_LEVEL,
  TARGET_KIND_FAN_GEAR,
  TARGET_KIND_FAN_OSCILLATION,
  TARGET_KIND_LIGHT_MODE,
  TARGET_KIND_LIGHT_SCHEDULE_ENABLED,
  TARGET_KIND_LIGHT_SCHEDULE_START_HOUR,
  TARGET_KIND_LIGHT_SCHEDULE_END_HOUR,
  TARGET_KIND_LIGHT_SCHEDULE_BRIGHTNESS
};

struct DeviceTarget {
  String device;
  TargetKind kind;
  int on;
  int level;
  DeviceTarget() : device(""), kind(TARGET_KIND_DEVICE_LEVEL), on(-1), level(-1) {}
};

const int TARGET_QUEUE_SIZE = 6;
OperationState opState = OP_IDLE;
DeviceTarget activeTarget;
bool activeTargetValid = false;
DeviceTarget targetQueue[TARGET_QUEUE_SIZE];
int targetQueueCount = 0;
unsigned long opStartedAtMs = 0;
int opRetryCount = 0;

String pendingSetMsgId;
String pendingSetJson;

bool verifyDevSeen = false;
bool verifyConfigSeen = false;
bool verifyRequestsSent = false;
unsigned long verifyRequestDueMs = 0;

DynamicJsonDocument cachedConfigDoc(CONFIG_DOC_CAPACITY);
DynamicJsonDocument rxJsonDoc(RX_JSON_DOC_CAPACITY);
bool configCacheValid = false;
unsigned long configCacheAtMs = 0;

bool queuedStartPending = false;
unsigned long queuedStartDueMs = 0;

// -----------------------------------------------------------------------------
// Parsers
// -----------------------------------------------------------------------------
struct JsonRawParser {
  String buf;
  bool collecting = false;
  bool inString = false;
  bool escapeNext = false;
  int braceDepth = 0;
  unsigned long frameStartMs = 0;
};

JsonRawParser jsonParser;
std::vector<uint8_t> rxStream;

struct AaaaAssembly {
  bool active = false;
  uint16_t payloadCrc = 0;
  uint32_t totalLen = 0;
  uint32_t received = 0;
  unsigned long startedAtMs = 0;
  std::vector<uint8_t> data;
  std::vector<uint8_t> seen;

  void reset() {
    active = false;
    payloadCrc = 0;
    totalLen = 0;
    received = 0;
    startedAtMs = 0;
    data.clear();
    seen.clear();
  }
};

AaaaAssembly aaaaRx;

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------
int clampInt(int v, int lo, int hi) { return v < lo ? lo : (v > hi ? hi : v); }
int stateOrZero(int v) { return v >= 0 ? v : 0; }
int hourToSeconds(int hour) { return clampInt(hour, 0, 23) * 3600; }
int secondsToHour(int seconds) { return clampInt(seconds, 0, 86399) / 3600; }

String secondsToHHMM(int seconds) {
  seconds = clampInt(seconds, 0, 86399);
  char buf[6];
  snprintf(buf, sizeof(buf), "%02d:%02d", seconds / 3600, (seconds % 3600) / 60);
  return String(buf);
}

int lightLevelToBrightness255(int level100) { return (clampInt(level100, 0, 100) * 255 + 50) / 100; }
int brightness255ToLightLevel(int b) { return (clampInt(b, 0, 255) * 100 + 127) / 255; }
int fanLevelToPercentage(int level) { level = clampInt(level, 0, 10); return level <= 0 ? 0 : level * 10; }
int percentageToFanLevel(int pct) { pct = clampInt(pct, 0, 100); return pct <= 0 ? 0 : clampInt((pct + 9) / 10, 1, 10); }
int normalizeFanOscillation(int value) { return clampInt(value, FAN_OSC_MIN, FAN_OSC_MAX); }
int blowerLevelToPercentage(int level) { return clampInt(level, 0, 100); }
int percentageToBlowerLevel(int pct) { return clampInt(pct, 0, 100); }

String boolJson(bool v) { return v ? "true" : "false"; }

String nextMsgId() {
  msgCounter++;
  return String(millis()) + "_" + String(msgCounter);
}

String truncateForLog(const String& s, size_t maxLen = MQTT_DEBUG_MAX_LEN) {
  if (s.length() <= maxLen) return s;
  return s.substring(0, maxLen) + "...<truncated len=" + String(s.length()) + ">";
}

String bytesToHexPreview(const uint8_t* data, size_t len, size_t maxLen = 48) {
  const char* hex = "0123456789ABCDEF";
  String out;
  size_t n = len < maxLen ? len : maxLen;
  for (size_t i = 0; i < n; i++) {
    if (i > 0) out += " ";
    out += hex[(data[i] >> 4) & 0x0F];
    out += hex[data[i] & 0x0F];
  }
  if (len > maxLen) out += " ...";
  return out;
}

const char* targetKindName(TargetKind kind) {
  switch (kind) {
    case TARGET_KIND_DEVICE_LEVEL: return "device_level";
    case TARGET_KIND_FAN_GEAR: return "fan_gear";
    case TARGET_KIND_FAN_OSCILLATION: return "fan_oscillation";
    case TARGET_KIND_LIGHT_MODE: return "light_mode";
    case TARGET_KIND_LIGHT_SCHEDULE_ENABLED: return "light_schedule_enabled";
    case TARGET_KIND_LIGHT_SCHEDULE_START_HOUR: return "light_schedule_start_hour";
    case TARGET_KIND_LIGHT_SCHEDULE_END_HOUR: return "light_schedule_end_hour";
    case TARGET_KIND_LIGHT_SCHEDULE_BRIGHTNESS: return "light_schedule_brightness";
    default: return "unknown";
  }
}

String targetToString(const DeviceTarget& t) {
  return "{device=" + t.device + ",kind=" + String(targetKindName(t.kind)) + ",on=" + String(t.on) + ",level=" + String(t.level) + "}";
}

const char* opStateName(OperationState st) {
  switch (st) {
    case OP_IDLE: return "idle";
    case OP_WAITING_CONFIG: return "waiting_config";
    case OP_SETTING_CONFIG: return "setting_config";
    case OP_VERIFYING: return "verifying";
    default: return "unknown";
  }
}

bool isLightConfigOnlyKind(TargetKind kind) {
  return kind == TARGET_KIND_LIGHT_MODE ||
         kind == TARGET_KIND_LIGHT_SCHEDULE_ENABLED ||
         kind == TARGET_KIND_LIGHT_SCHEDULE_START_HOUR ||
         kind == TARGET_KIND_LIGHT_SCHEDULE_END_HOUR ||
         kind == TARGET_KIND_LIGHT_SCHEDULE_BRIGHTNESS;
}

void initializeLocalStateDefaults() {
  currentState.fanOn = 0;
  currentState.fanLevel = 0;
  currentState.fanPercentage = 0;
  currentState.blowerOn = 0;
  currentState.blowerLevel = 0;
  currentState.blowerPercentage = 0;
  currentState.lightOn = 0;
  currentState.lightLevel = 0;
  currentState.lightBrightness = 0;
}

// -----------------------------------------------------------------------------
// CRC16 / endian helpers
// -----------------------------------------------------------------------------
uint16_t crc16Modbus(const uint8_t* data, size_t len) {
  uint16_t crc = 0xFFFF;
  for (size_t i = 0; i < len; i++) {
    crc ^= data[i];
    for (int bit = 0; bit < 8; bit++) crc = (crc & 1) ? (crc >> 1) ^ 0xA001 : (crc >> 1);
  }
  return crc;
}

void appendU16BE(std::vector<uint8_t>& out, uint16_t v) {
  out.push_back((v >> 8) & 0xFF);
  out.push_back(v & 0xFF);
}

void appendU32BE(std::vector<uint8_t>& out, uint32_t v) {
  out.push_back((v >> 24) & 0xFF);
  out.push_back((v >> 16) & 0xFF);
  out.push_back((v >> 8) & 0xFF);
  out.push_back(v & 0xFF);
}

uint16_t readU16BE(const uint8_t* p) { return ((uint16_t)p[0] << 8) | p[1]; }
uint32_t readU32BE(const uint8_t* p) { return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3]; }

// -----------------------------------------------------------------------------
// MQTT helpers
// -----------------------------------------------------------------------------
bool mqttPub(const String& t, const String& value, bool retained = true) {
  if (!mqttClient.connected()) return false;
  bool ok = mqttClient.publish(t.c_str(), value.c_str(), retained);
  Serial.print("[MQTT PUB] ");
  Serial.print(t);
  Serial.print(" = ");
  Serial.print(truncateForLog(value, 160));
  Serial.print(" retain=");
  Serial.print(retained ? "true" : "false");
  Serial.print(" -> ");
  Serial.println(ok ? "OK" : "FAIL");
  lastMqttPublishStepAtMs = millis();
  if (ok) mqttConsecutiveFails = 0;
  else {
    mqttConsecutiveFails++;
    mqttPublishCooldownUntilMs = millis() + MQTT_FAIL_COOLDOWN_MS;
  }
  return ok;
}

void mqttPubDebug(const char* subtopic, const String& value) {
  if (!MQTT_DEBUG_ENABLED) return;
  Serial.println(String("[DEBUG] ") + subtopic + " = " + truncateForLog(value, 220));
  if (!mqttClient.connected()) return;
  if (bleTxInProgress || parserProcessing || bootstrapBleActive) return;
  mqttPub(topicDebug((String("/") + subtopic).c_str()), truncateForLog(value), false);
}

String getDeviceJson() {
  String s = "{";
  s += "\"identifiers\":[\"" + String(HA_DEVICE_ID) + "\"],";
  s += "\"name\":\"" + String(HA_DEVICE_NAME) + "\",";
  s += "\"manufacturer\":\"" + String(HA_DEVICE_MANUFACTURER) + "\",";
  s += "\"model\":\"" + String(HA_DEVICE_MODEL) + "\",";
  s += "\"sw_version\":\"" + String(FW_VERSION) + "\"";
  s += "}";
  return s;
}

String availabilityFragment() {
  return "\"availability_topic\":\"" + topic("/availability") + "\",\"payload_available\":\"online\",\"payload_not_available\":\"offline\"";
}

String commandFragment() { return "\"qos\":0,\"retain\":false,\"optimistic\":false"; }

void requestAvailabilityPublish(bool online) { availabilityValuePending = online; availabilityPublishPending = true; }
void requestStatusPublish(const String& reason) { statusPublishPending = true; Serial.println("[STATUS] scheduled " + reason); }
void requestStatePublish(const String& reason) { statePublishPending = true; Serial.println("[STATE] scheduled " + reason); }

void setOperationState(OperationState st, const String& reason) {
  opState = st;
  opStartedAtMs = millis();
  mqttPubDebug("op_state", String(opStateName(st)) + " reason=" + reason);
  requestStatusPublish("op_state");
}

// -----------------------------------------------------------------------------
// Home Assistant discovery
// -----------------------------------------------------------------------------
bool publishDiscoveryPayload(const char* component, const char* objectId, const String& payload) {
  String t = String(HA_DISCOVERY_PREFIX) + "/" + component + "/" + HA_DEVICE_ID + "/" + objectId + "/config";
  return mqttPub(t, payload, true);
}

void publishDiscoverySensor(const char* objectId, const char* name, const char* entityId, const char* stateTopic, const char* deviceClass, const char* unit, const char* stateClass) {
  String p = "{";
  p += "\"name\":\"" + String(name) + "\",";
  p += "\"unique_id\":\"" + String(HA_DEVICE_ID) + "_" + objectId + "\",";
  p += "\"default_entity_id\":\"" + String(entityId) + "\",";
  p += "\"state_topic\":\"" + String(stateTopic) + "\",";
  p += availabilityFragment() + ",";
  if (deviceClass && strlen(deviceClass)) p += "\"device_class\":\"" + String(deviceClass) + "\",";
  if (unit && strlen(unit)) p += "\"unit_of_measurement\":\"" + String(unit) + "\",";
  if (stateClass && strlen(stateClass)) p += "\"state_class\":\"" + String(stateClass) + "\",";
  p += "\"device\":" + getDeviceJson() + "}";
  publishDiscoveryPayload("sensor", objectId, p);
}

void publishDiscoveryNumber(const char* objectId, const char* name, const char* entityId, const char* stateTopic, const char* cmdTopic, int minVal, int maxVal, int step, const char* unit) {
  String p = "{";
  p += "\"name\":\"" + String(name) + "\",";
  p += "\"unique_id\":\"" + String(HA_DEVICE_ID) + "_" + objectId + "\",";
  p += "\"default_entity_id\":\"" + String(entityId) + "\",";
  p += "\"state_topic\":\"" + String(stateTopic) + "\",";
  p += "\"command_topic\":\"" + String(cmdTopic) + "\",";
  p += "\"min\":" + String(minVal) + ",\"max\":" + String(maxVal) + ",\"step\":" + String(step) + ",\"mode\":\"slider\",";
  if (unit && strlen(unit)) p += "\"unit_of_measurement\":\"" + String(unit) + "\",";
  p += availabilityFragment() + "," + commandFragment() + ",\"device\":" + getDeviceJson() + "}";
  publishDiscoveryPayload("number", objectId, p);
}

void publishDiscoverySwitch(const char* objectId, const char* name, const char* entityId, const char* stateTopic, const char* cmdTopic) {
  String p = "{";
  p += "\"name\":\"" + String(name) + "\",";
  p += "\"unique_id\":\"" + String(HA_DEVICE_ID) + "_" + objectId + "\",";
  p += "\"default_entity_id\":\"" + String(entityId) + "\",";
  p += "\"state_topic\":\"" + String(stateTopic) + "\",";
  p += "\"command_topic\":\"" + String(cmdTopic) + "\",";
  p += "\"payload_on\":\"ON\",\"payload_off\":\"OFF\",";
  p += availabilityFragment() + "," + commandFragment() + ",\"device\":" + getDeviceJson() + "}";
  publishDiscoveryPayload("switch", objectId, p);
}

void publishDiscoveryLight() {
  String p = "{";
  p += "\"name\":\"GGS Light\",";
  p += "\"unique_id\":\"" + String(HA_DEVICE_ID) + "_light\",";
  p += "\"default_entity_id\":\"light.ggs_light\",";
  p += "\"state_topic\":\"" + topic("/state/light/on") + "\",";
  p += "\"state_value_template\":\"{% if value | int == 1 %}ON{% else %}OFF{% endif %}\",";
  p += "\"command_topic\":\"" + topic("/cmd/light/set") + "\",";
  p += "\"payload_on\":\"ON\",\"payload_off\":\"OFF\",";
  p += "\"brightness_state_topic\":\"" + topic("/state/light/brightness") + "\",";
  p += "\"brightness_command_topic\":\"" + topic("/cmd/light/brightness/set") + "\",";
  p += "\"brightness_scale\":255,";
  p += availabilityFragment() + "," + commandFragment() + ",\"device\":" + getDeviceJson() + "}";
  publishDiscoveryPayload("light", "light", p);
}

void publishDiscoverySelectLightMode() {
  String p = "{";
  p += "\"name\":\"GGS Light Mode\",";
  p += "\"unique_id\":\"" + String(HA_DEVICE_ID) + "_light_mode\",";
  p += "\"default_entity_id\":\"select.ggs_light_mode\",";
  p += "\"state_topic\":\"" + topic("/state/light/mode") + "\",";
  p += "\"command_topic\":\"" + topic("/cmd/light/mode/set") + "\",";
  p += "\"options\":[\"manual\",\"time_slot\"],";
  p += availabilityFragment() + "," + commandFragment() + ",\"device\":" + getDeviceJson() + "}";
  publishDiscoveryPayload("select", "light_mode", p);
}

void publishDiscoveryFanLike(const char* objectId, const char* name, const char* entityId, const char* stateTopic, const char* cmdTopic, const char* pctStateTopic, const char* pctCmdTopic) {
  String p = "{";
  p += "\"name\":\"" + String(name) + "\",";
  p += "\"unique_id\":\"" + String(HA_DEVICE_ID) + "_" + objectId + "\",";
  p += "\"default_entity_id\":\"" + String(entityId) + "\",";
  p += "\"state_topic\":\"" + String(stateTopic) + "\",";
  p += "\"state_value_template\":\"{% if value | int == 1 %}ON{% else %}OFF{% endif %}\",";
  p += "\"command_topic\":\"" + String(cmdTopic) + "\",";
  p += "\"payload_on\":\"ON\",\"payload_off\":\"OFF\",";
  p += "\"percentage_state_topic\":\"" + String(pctStateTopic) + "\",";
  p += "\"percentage_value_template\":\"{{ value | int }}\",";
  p += "\"percentage_command_topic\":\"" + String(pctCmdTopic) + "\",";
  p += "\"speed_range_min\":1,\"speed_range_max\":100,";
  p += availabilityFragment() + "," + commandFragment() + ",\"device\":" + getDeviceJson() + "}";
  publishDiscoveryPayload("fan", objectId, p);
}

void publishDiscoveryStep(int step) {
  switch (step) {
    case 0: publishDiscoverySensor("temperature", "GGS Temperature", "sensor.ggs_temperature", topic("/state/sensor/temp").c_str(), "temperature", "°C", "measurement"); break;
    case 1: publishDiscoverySensor("humidity", "GGS Humidity", "sensor.ggs_humidity", topic("/state/sensor/humi").c_str(), "humidity", "%", "measurement"); break;
    case 2: publishDiscoverySensor("vpd", "GGS VPD", "sensor.ggs_vpd", topic("/state/sensor/vpd").c_str(), "", "kPa", "measurement"); break;
    case 3: publishDiscoveryLight(); break;
    case 4: publishDiscoverySelectLightMode(); break;
    case 5: publishDiscoverySwitch("light_schedule_enabled", "GGS Light Schedule Enabled", "switch.ggs_light_schedule_enabled", topic("/state/light/schedule/enabled").c_str(), topic("/cmd/light/schedule/enabled/set").c_str()); break;
    case 6: publishDiscoveryNumber("light_schedule_start_hour", "GGS Light Schedule Start Hour", "number.ggs_light_schedule_start_hour", topic("/state/light/schedule/start_hour").c_str(), topic("/cmd/light/schedule/start_hour/set").c_str(), 0, 23, 1, "h"); break;
    case 7: publishDiscoveryNumber("light_schedule_end_hour", "GGS Light Schedule End Hour", "number.ggs_light_schedule_end_hour", topic("/state/light/schedule/end_hour").c_str(), topic("/cmd/light/schedule/end_hour/set").c_str(), 0, 23, 1, "h"); break;
    case 8: publishDiscoveryNumber("light_schedule_brightness", "GGS Light Schedule Brightness", "number.ggs_light_schedule_brightness", topic("/state/light/schedule/brightness").c_str(), topic("/cmd/light/schedule/brightness/set").c_str(), 0, 100, 1, "%"); break;
    case 9: publishDiscoverySensor("light_schedule_start_time", "GGS Light Schedule Start Time", "sensor.ggs_light_schedule_start_time", topic("/state/light/schedule/start_time").c_str(), "", "", ""); break;
    case 10: publishDiscoverySensor("light_schedule_end_time", "GGS Light Schedule End Time", "sensor.ggs_light_schedule_end_time", topic("/state/light/schedule/end_time").c_str(), "", "", ""); break;
    case 11: publishDiscoveryFanLike("fan", "GGS Fan", "fan.ggs_fan", topic("/state/fan/on").c_str(), topic("/cmd/fan/set").c_str(), topic("/state/fan/percentage").c_str(), topic("/cmd/fan/percentage/set").c_str()); break;
    case 12: publishDiscoveryNumber("fan_gear", "GGS Fan Gear", "number.ggs_fan_gear", topic("/state/fan/level").c_str(), topic("/cmd/fan/gear/set").c_str(), 1, 10, 1, ""); break;
    case 13: publishDiscoveryNumber("fan_oscillation", "GGS Fan Oscillation", "number.ggs_fan_oscillation", topic("/state/fan/oscillation").c_str(), topic("/cmd/fan/oscillation/set").c_str(), 1, 10, 1, ""); break;
    case 14: publishDiscoveryFanLike("blower", "GGS Blower", "fan.ggs_blower", topic("/state/blower/on").c_str(), topic("/cmd/blower/set").c_str(), topic("/state/blower/percentage").c_str(), topic("/cmd/blower/percentage/set").c_str()); break;
  }
}

void requestDiscoveryPublish(const String& reason) {
  discoveryPending = true;
  discoveryStep = 0;
  Serial.println("[DISCOVERY] scheduled reason=" + reason);
}

bool mqttQuietEnoughForWork() {
  return mqttClient.connected() && !bootstrapBleActive && initialBleBootstrapDone && opState == OP_IDLE && !bleTxInProgress && !parserProcessing && rxStream.empty() && !activeTargetValid && targetQueueCount == 0 && !queuedStartPending;
}

bool mqttCanDoStepNow() {
  unsigned long now = millis();
  if (now < mqttPublishCooldownUntilMs) return false;
  if (now - lastMqttPublishStepAtMs < MQTT_STEP_INTERVAL_MS) return false;
  return mqttQuietEnoughForWork();
}

void discoveryTick() {
  if (!discoveryPending || !mqttCanDoStepNow()) return;
  int beforeFails = mqttConsecutiveFails;
  publishDiscoveryStep(discoveryStep);
  if (mqttConsecutiveFails > beforeFails) return;
  discoveryStep++;
  if (discoveryStep >= DISCOVERY_TOTAL_STEPS) {
    discoveryPending = false;
    discoveryStep = 0;
    requestAvailabilityPublish(connected);
    requestStatePublish("discovery_complete");
  }
}

void publishStatusJsonNow() {
  String payload = "{";
  payload += "\"wifi\":\"" + String(WiFi.status() == WL_CONNECTED ? "connected" : "disconnected") + "\",";
  payload += "\"mqtt\":\"" + String(mqttClient.connected() ? "connected" : "disconnected") + "\",";
  payload += "\"ble\":\"" + String(connected ? "connected" : "disconnected") + "\",";
  payload += "\"op_state\":\"" + String(opStateName(opState)) + "\",";
  payload += "\"config_cache_valid\":" + boolJson(configCacheValid) + ",";
  payload += "\"dev_state_valid\":" + boolJson(currentState.devStateValid) + ",";
  payload += "\"config_state_valid\":" + boolJson(currentState.configStateValid) + ",";
  payload += "\"target_queue_count\":" + String(targetQueueCount) + ",";
  payload += "\"rx_notify_callbacks\":" + String(rxNotifyCallbacks) + ",";
  payload += "\"rx_notify_bytes\":" + String(rxNotifyBytes) + ",";
  payload += "\"rx_stream_size\":" + String(rxStream.size()) + ",";
  payload += "\"free_heap\":" + String(ESP.getFreeHeap()) + ",";
  payload += "\"last_dev_rx_age_ms\":" + String(currentState.lastStateRxMs == 0 ? -1 : (long)(millis() - currentState.lastStateRxMs)) + ",";
  payload += "\"last_config_rx_age_ms\":" + String(currentState.lastConfigRxMs == 0 ? -1 : (long)(millis() - currentState.lastConfigRxMs));
  payload += "}";
  mqttPub(topic("/status/json"), payload, true);
}

void publishAllStateNow() {
  if (!mqttClient.connected()) return;
  if (!isnan(currentState.temp)) mqttPub(topic("/state/sensor/temp"), String(currentState.temp, 1), true);
  if (!isnan(currentState.humi)) mqttPub(topic("/state/sensor/humi"), String(currentState.humi, 1), true);
  if (!isnan(currentState.vpd)) mqttPub(topic("/state/sensor/vpd"), String(currentState.vpd, 2), true);

  mqttPub(topic("/state/fan/on"), String(stateOrZero(currentState.fanOn)), true);
  mqttPub(topic("/state/fan/level"), String(stateOrZero(currentState.fanLevel)), true);
  mqttPub(topic("/state/fan/percentage"), String(stateOrZero(currentState.fanPercentage)), true);
  if (currentState.fanOscillation >= 0) mqttPub(topic("/state/fan/oscillation"), String(currentState.fanOscillation), true);

  mqttPub(topic("/state/blower/on"), String(stateOrZero(currentState.blowerOn)), true);
  mqttPub(topic("/state/blower/level"), String(stateOrZero(currentState.blowerLevel)), true);
  mqttPub(topic("/state/blower/percentage"), String(stateOrZero(currentState.blowerPercentage)), true);

  mqttPub(topic("/state/light/on"), String(stateOrZero(currentState.lightOn)), true);
  mqttPub(topic("/state/light/level"), String(stateOrZero(currentState.lightLevel)), true);
  mqttPub(topic("/state/light/brightness"), String(stateOrZero(currentState.lightBrightness)), true);
  if (currentState.lightModeType >= 0) mqttPub(topic("/state/light/mode"), currentState.lightModeType == 1 ? "time_slot" : "manual", true);
  if (currentState.lightScheduleEnabled >= 0) mqttPub(topic("/state/light/schedule/enabled"), currentState.lightScheduleEnabled == 1 ? "ON" : "OFF", true);
  if (currentState.lightScheduleStartHour >= 0) mqttPub(topic("/state/light/schedule/start_hour"), String(currentState.lightScheduleStartHour), true);
  if (currentState.lightScheduleEndHour >= 0) mqttPub(topic("/state/light/schedule/end_hour"), String(currentState.lightScheduleEndHour), true);
  if (currentState.lightScheduleStartTime >= 0) mqttPub(topic("/state/light/schedule/start_time"), secondsToHHMM(currentState.lightScheduleStartTime), true);
  if (currentState.lightScheduleEndTime >= 0) mqttPub(topic("/state/light/schedule/end_time"), secondsToHHMM(currentState.lightScheduleEndTime), true);
  if (currentState.lightScheduleBrightness >= 0) mqttPub(topic("/state/light/schedule/brightness"), String(currentState.lightScheduleBrightness), true);
}

void availabilityPublishTick() {
  if (availabilityPublishPending && mqttCanDoStepNow()) {
    if (mqttPub(topic("/availability"), availabilityValuePending ? "online" : "offline", true)) availabilityPublishPending = false;
  }
}

void statusPublishTick() {
  if (statusPublishPending && mqttCanDoStepNow()) {
    publishStatusJsonNow();
    statusPublishPending = false;
  }
}

void statePublishTick() {
  if (statePublishPending && mqttCanDoStepNow()) {
    publishAllStateNow();
    statePublishPending = false;
    requestStatusPublish("state_published");
  }
}

// -----------------------------------------------------------------------------
// BLE TX and AA AA framing
// -----------------------------------------------------------------------------
bool beginBleTx(const String& label) {
  if (bleTxInProgress) return false;
  if (!connected || pWriteChar == nullptr) return false;
  bleTxInProgress = true;
  bleTxStartedAtMs = millis();
  bleTxLabel = label;
  return true;
}

void endBleTx(const String& reason) {
  Serial.println("[BLE TX] end label=" + bleTxLabel + " reason=" + reason);
  bleTxInProgress = false;
  bleTxStartedAtMs = 0;
  bleTxLabel = "";
}

void handleBleTxStuck(const String& reason) {
  if (!bleTxInProgress) return;
  mqttPubDebug("tx_watchdog", "stuck label=" + bleTxLabel + " reason=" + reason);
  bleTxInProgress = false;
  bleTxStartedAtMs = 0;
  bleTxLabel = "";
  if (pClient != nullptr && connected) pClient->disconnect();
}

void bleTxWatchdogTick() {
  if (bleTxInProgress && millis() - bleTxStartedAtMs > BLE_TX_STUCK_TIMEOUT_MS) handleBleTxStuck("periodic_tick");
}

String extractMethodPreview(const String& json) {
  int p = json.indexOf("\"method\"");
  if (p < 0) return "method=?";
  int colon = json.indexOf(':', p);
  int q1 = json.indexOf('"', colon + 1);
  int q2 = json.indexOf('"', q1 + 1);
  if (colon < 0 || q1 < 0 || q2 < 0) return "method=?";
  return "method=" + json.substring(q1 + 1, q2);
}

std::vector<uint8_t> buildAaaaFrame(const uint8_t* payload, size_t payloadLen, uint16_t payloadCrc, uint32_t offset, uint16_t chunkLen) {
  std::vector<uint8_t> frame;
  frame.reserve(6 + 14 + chunkLen + 2);

  appendU16BE(frame, 0xAAAA);
  appendU16BE(frame, 0x0003);
  appendU16BE(frame, 14 + chunkLen);

  frame.push_back(0x00);
  frame.push_back(0x01);
  appendU16BE(frame, payloadCrc);
  appendU32BE(frame, (uint32_t)payloadLen);
  appendU32BE(frame, offset);
  appendU16BE(frame, chunkLen);

  for (uint16_t i = 0; i < chunkLen; i++) frame.push_back(payload[offset + i]);

  appendU16BE(frame, crc16Modbus(frame.data(), frame.size()));
  return frame;
}

bool sendJsonFramedAAAA(const String& json) {
  const uint8_t* payload = (const uint8_t*)json.c_str();
  size_t payloadLen = json.length();
  if (payloadLen == 0 || payloadLen > MAX_AAAA_PAYLOAD_SIZE) return false;
  if (!beginBleTx("AAAA:" + extractMethodPreview(json))) return false;

  uint16_t payloadCrc = crc16Modbus(payload, payloadLen);
  for (uint32_t offset = 0; offset < payloadLen; offset += AAAA_CHUNK_SIZE) {
    size_t remaining = payloadLen - offset;
    uint16_t chunkLen = (uint16_t)((remaining < AAAA_CHUNK_SIZE) ? remaining : AAAA_CHUNK_SIZE);
    std::vector<uint8_t> frame = buildAaaaFrame(payload, payloadLen, payloadCrc, offset, chunkLen);

    Serial.println("[AAAA TX FRAME] offset=" + String(offset) + " chunk_len=" + String(chunkLen) + " frame_len=" + String(frame.size()) + " hex=" + bytesToHexPreview(frame.data(), frame.size()));
    bool ok = pWriteChar->writeValue(frame.data(), frame.size(), true);
    if (!ok) {
      endBleTx("aaaa_write_failed");
      return false;
    }
    delay(BLE_FRAME_DELAY_MS);
    yield();
  }

  endBleTx("aaaa_all_frames_sent");
  return true;
}

bool sendJsonAuto(const String& json) {
  if (json.length() == 0 || bleTxInProgress || !connected || pWriteChar == nullptr) return false;
  const uint8_t* payload = (const uint8_t*)json.c_str();
  size_t payloadLen = json.length();

  Serial.println("[BLE TX JSON] " + truncateForLog(json, 900));
  if (payloadLen <= RAW_WRITE_LIMIT) {
    if (!beginBleTx("RAW:" + extractMethodPreview(json))) return false;
    bool ok = pWriteChar->writeValue((uint8_t*)payload, payloadLen, true);
    endBleTx(ok ? "raw_write_ok" : "raw_write_failed");
    return ok;
  }

  return sendJsonFramedAAAA(json);
}

bool requestGetDevSta() {
  String json = "{\"method\":\"getDevSta\",\"params\":{},\"msgId\":\"" + nextMsgId() + "\"}";
  lastProbeAt = millis();
  return sendJsonAuto(json);
}

bool requestGetConfigFile() {
  String json = "{";
  json += "\"method\":\"getConfigFile\",\"params\":{},\"msgId\":\"" + nextMsgId() + "\",";
  json += "\"pid\":\"" + String(GGS_PID) + "\",\"pcode\":" + String(GGS_PCODE) + ",\"uid\":\"" + String(GGS_UID) + "\"";
  json += "}";
  return sendJsonAuto(json);
}

// -----------------------------------------------------------------------------
// Config cache and target updates
// -----------------------------------------------------------------------------
int readOnFromDeviceObject(JsonObject dev) {
  if (dev.isNull()) return -999;
  if (!dev["mOnOff"].isNull()) return dev["mOnOff"].as<int>() ? 1 : 0;
  if (!dev["on"].isNull()) return dev["on"].as<int>() ? 1 : 0;
  return -999;
}

int readLightLevelFromObject(JsonObject dev) {
  if (dev.isNull()) return -999;
  if (!dev["mLevel"].isNull()) return clampInt(dev["mLevel"].as<int>(), 0, 100);
  if (!dev["level"].isNull()) return clampInt(dev["level"].as<int>(), 0, 100);
  return -999;
}

int readBlowerLevelFromObject(JsonObject dev) {
  if (dev.isNull()) return -999;
  if (!dev["mLevel"].isNull()) return clampInt(dev["mLevel"].as<int>(), 0, 100);
  if (!dev["maxSpeed"].isNull()) return clampInt(dev["maxSpeed"].as<int>(), 0, 100);
  if (!dev["level"].isNull()) return clampInt(dev["level"].as<int>(), 0, 100);
  return -999;
}

int readFanLevelFromObject(JsonObject dev) {
  if (dev.isNull()) return -999;
  if (!dev["level"].isNull()) return clampInt(dev["level"].as<int>(), 0, 10);
  if (!dev["mLevel"].isNull()) return clampInt(dev["mLevel"].as<int>(), 0, 10);
  return -999;
}

int readFanOscillationFromObject(JsonObject dev) {
  if (dev.isNull()) return -999;
  if (!dev["shakeLevel"].isNull()) return clampInt(dev["shakeLevel"].as<int>(), 1, 10);
  return -999;
}

JsonObject getLightScheduleObject(JsonObject light) {
  if (light.isNull()) return JsonObject();
  JsonArray periods = light["timePeriod"].as<JsonArray>();
  if (periods.isNull() || periods.size() < 1) return JsonObject();
  return periods[0].as<JsonObject>();
}

JsonObject ensureLightScheduleObject(JsonObject light) {
  JsonArray periods = light["timePeriod"].as<JsonArray>();
  if (periods.isNull()) periods = light.createNestedArray("timePeriod");
  JsonObject period0;
  if (periods.size() < 1) period0 = periods.createNestedObject();
  else period0 = periods[0].as<JsonObject>();
  if (period0.isNull()) {
    periods.remove(0);
    period0 = periods.createNestedObject();
  }
  if (period0["enabled"].isNull()) period0["enabled"] = DEFAULT_LIGHT_SCHEDULE_ENABLED;
  if (period0["weekmask"].isNull()) period0["weekmask"] = DEFAULT_LIGHT_SCHEDULE_WEEKMASK;
  if (period0["startTime"].isNull()) period0["startTime"] = hourToSeconds(DEFAULT_LIGHT_SCHEDULE_START_H);
  if (period0["endTime"].isNull()) period0["endTime"] = hourToSeconds(DEFAULT_LIGHT_SCHEDULE_END_H);
  if (period0["brightness"].isNull()) period0["brightness"] = DEFAULT_LIGHT_SCHEDULE_BRIGHTNESS;
  return period0;
}

int readLightModeTypeFromObject(JsonObject light) { if (!light.isNull() && !light["modeType"].isNull()) return light["modeType"].as<int>(); return -999; }
int readLightLastAutoModeTypeFromObject(JsonObject light) { if (!light.isNull() && !light["lastAutoModeType"].isNull()) return light["lastAutoModeType"].as<int>(); return -999; }
int readLightScheduleEnabledFromObject(JsonObject light) { JsonObject p = getLightScheduleObject(light); if (!p.isNull() && !p["enabled"].isNull()) return p["enabled"].as<int>() ? 1 : 0; return -999; }
int readLightScheduleWeekmaskFromObject(JsonObject light) { JsonObject p = getLightScheduleObject(light); if (!p.isNull() && !p["weekmask"].isNull()) return p["weekmask"].as<int>(); return -999; }
int readLightScheduleStartTimeFromObject(JsonObject light) { JsonObject p = getLightScheduleObject(light); if (!p.isNull() && !p["startTime"].isNull()) return clampInt(p["startTime"].as<int>(), 0, 86399); return -999; }
int readLightScheduleEndTimeFromObject(JsonObject light) { JsonObject p = getLightScheduleObject(light); if (!p.isNull() && !p["endTime"].isNull()) return clampInt(p["endTime"].as<int>(), 0, 86399); return -999; }
int readLightScheduleBrightnessFromObject(JsonObject light) { JsonObject p = getLightScheduleObject(light); if (!p.isNull() && !p["brightness"].isNull()) return clampInt(p["brightness"].as<int>(), 0, 100); return -999; }

bool validateCachedConfigCompleteness(const char* where) {
  JsonObject root = cachedConfigDoc.as<JsonObject>();
  JsonObject devices = root["device"].as<JsonObject>();
  if (root.isNull() || devices.isNull()) { mqttPubDebug("config_validate", String(where) + ":missing_root_or_device"); return false; }
  JsonObject light = devices["light"].as<JsonObject>();
  JsonObject blower = devices["blower"].as<JsonObject>();
  JsonObject fan = devices["fan"].as<JsonObject>();
  if (light.isNull() || blower.isNull() || fan.isNull()) return false;
  if (readOnFromDeviceObject(light) == -999 || readLightLevelFromObject(light) == -999) return false;
  if (readOnFromDeviceObject(blower) == -999 || readBlowerLevelFromObject(blower) == -999) return false;
  if (readOnFromDeviceObject(fan) == -999 || readFanLevelFromObject(fan) == -999) return false;
  return true;
}

void applyConfigCacheToCurrentState() {
  if (!configCacheValid) return;
  JsonObject devices = cachedConfigDoc["device"].as<JsonObject>();
  JsonObject light = devices["light"].as<JsonObject>();
  JsonObject blower = devices["blower"].as<JsonObject>();
  JsonObject fan = devices["fan"].as<JsonObject>();
  int v;
  v = readOnFromDeviceObject(light); if (v != -999) currentState.lightOn = v;
  v = readLightLevelFromObject(light); if (v != -999) { currentState.lightLevel = v; currentState.lightBrightness = lightLevelToBrightness255(v); }
  v = readLightModeTypeFromObject(light); if (v != -999) currentState.lightModeType = v;
  v = readLightLastAutoModeTypeFromObject(light); if (v != -999) currentState.lightLastAutoModeType = v;
  v = readLightScheduleEnabledFromObject(light); if (v != -999) currentState.lightScheduleEnabled = v;
  v = readLightScheduleWeekmaskFromObject(light); if (v != -999) currentState.lightScheduleWeekmask = v;
  v = readLightScheduleStartTimeFromObject(light); if (v != -999) { currentState.lightScheduleStartTime = v; currentState.lightScheduleStartHour = secondsToHour(v); }
  v = readLightScheduleEndTimeFromObject(light); if (v != -999) { currentState.lightScheduleEndTime = v; currentState.lightScheduleEndHour = secondsToHour(v); }
  v = readLightScheduleBrightnessFromObject(light); if (v != -999) currentState.lightScheduleBrightness = v;
  v = readOnFromDeviceObject(blower); if (v != -999) currentState.blowerOn = v;
  v = readBlowerLevelFromObject(blower); if (v != -999) { currentState.blowerLevel = v; currentState.blowerPercentage = blowerLevelToPercentage(v); }
  v = readOnFromDeviceObject(fan); if (v != -999) currentState.fanOn = v;
  v = readFanLevelFromObject(fan); if (v != -999) { currentState.fanLevel = v; currentState.fanPercentage = fanLevelToPercentage(v); }
  v = readFanOscillationFromObject(fan); if (v != -999) currentState.fanOscillation = v;
  currentState.configStateValid = true;
  currentState.lastConfigRxMs = millis();
}

bool cacheConfigFileFromDoc(JsonDocument& doc) {
  JsonVariant cfg;
  if (!doc["data"]["configFile"].isNull()) cfg = doc["data"]["configFile"];
  else if (!doc["params"]["configFile"].isNull()) cfg = doc["params"]["configFile"];
  else if (!doc["configFile"].isNull()) cfg = doc["configFile"];
  else if (!doc["data"]["device"].isNull()) cfg = doc["data"];
  else if (!doc["device"].isNull()) cfg = doc.as<JsonVariant>();
  if (cfg.isNull()) return false;

  cachedConfigDoc.clear();
  cachedConfigDoc.set(cfg);
  if (cachedConfigDoc.overflowed() || !validateCachedConfigCompleteness("cache")) {
    configCacheValid = false;
    cachedConfigDoc.clear();
    return false;
  }

  configCacheValid = true;
  configCacheAtMs = millis();
  applyConfigCacheToCurrentState();
  requestStatePublish("config_cache_updated");
  return true;
}

void normalizeTarget(DeviceTarget& t) {
  t.on = (t.on >= 0) ? (t.on ? 1 : 0) : -1;
  if (t.device == "light") {
    if (t.kind == TARGET_KIND_LIGHT_MODE) { t.on = -1; t.level = clampInt(t.level, 0, 1); }
    else if (t.kind == TARGET_KIND_LIGHT_SCHEDULE_ENABLED) { t.on = -1; t.level = t.level > 0 ? 1 : 0; }
    else if (t.kind == TARGET_KIND_LIGHT_SCHEDULE_START_HOUR || t.kind == TARGET_KIND_LIGHT_SCHEDULE_END_HOUR) { t.on = -1; t.level = clampInt(t.level, 0, 23); }
    else if (t.kind == TARGET_KIND_LIGHT_SCHEDULE_BRIGHTNESS) { t.on = -1; t.level = clampInt(t.level, 0, 100); }
    else { t.kind = TARGET_KIND_DEVICE_LEVEL; if (t.level >= 0) t.level = clampInt(t.level, 0, 100); }
  } else if (t.device == "blower") {
    t.kind = TARGET_KIND_DEVICE_LEVEL;
    if (t.level >= 0) t.level = clampInt(t.level, 0, 100);
  } else if (t.device == "fan") {
    if (t.kind == TARGET_KIND_FAN_OSCILLATION) { t.on = -1; if (t.level >= 0) t.level = normalizeFanOscillation(t.level); }
    else { t.kind = TARGET_KIND_FAN_GEAR; if (t.level >= 0) t.level = clampInt(t.level, 0, 10); }
  }
}

int cachedDeviceOn(const String& device) {
  if (!configCacheValid) return -999;
  JsonObject dev = cachedConfigDoc["device"][device.c_str()].as<JsonObject>();
  return readOnFromDeviceObject(dev);
}

int cachedDeviceLevelForTarget(const DeviceTarget& t) {
  if (!configCacheValid) return -999;
  JsonObject dev = cachedConfigDoc["device"][t.device.c_str()].as<JsonObject>();
  if (dev.isNull()) return -999;
  if (t.device == "light" && t.kind == TARGET_KIND_LIGHT_MODE) return readLightModeTypeFromObject(dev);
  if (t.device == "light" && t.kind == TARGET_KIND_LIGHT_SCHEDULE_ENABLED) return readLightScheduleEnabledFromObject(dev);
  if (t.device == "light" && t.kind == TARGET_KIND_LIGHT_SCHEDULE_START_HOUR) { int v = readLightScheduleStartTimeFromObject(dev); return v == -999 ? -999 : secondsToHour(v); }
  if (t.device == "light" && t.kind == TARGET_KIND_LIGHT_SCHEDULE_END_HOUR) { int v = readLightScheduleEndTimeFromObject(dev); return v == -999 ? -999 : secondsToHour(v); }
  if (t.device == "light" && t.kind == TARGET_KIND_LIGHT_SCHEDULE_BRIGHTNESS) return readLightScheduleBrightnessFromObject(dev);
  if (t.kind == TARGET_KIND_FAN_OSCILLATION) return readFanOscillationFromObject(dev);
  if (t.kind == TARGET_KIND_FAN_GEAR) return readFanLevelFromObject(dev);
  if (t.device == "blower") return readBlowerLevelFromObject(dev);
  if (t.device == "light") return readLightLevelFromObject(dev);
  return -999;
}

void completeImplicitOnLevelFromCache(DeviceTarget& t) {
  if (t.on != 1 || t.level >= 0) return;
  int cachedLevel = cachedDeviceLevelForTarget(t);
  if (cachedLevel > 0) return;
  if (t.device == "light" && t.kind == TARGET_KIND_DEVICE_LEVEL) t.level = DEFAULT_LIGHT_LEVEL_ON;
  else if (t.device == "fan" && t.kind == TARGET_KIND_FAN_GEAR) t.level = DEFAULT_FAN_GEAR_ON;
  else if (t.device == "blower") t.level = DEFAULT_BLOWER_LEVEL_ON;
}

bool updateCachedConfigDevice(DeviceTarget& t) {
  if (!configCacheValid || !validateCachedConfigCompleteness("before_update")) return false;
  normalizeTarget(t);
  completeImplicitOnLevelFromCache(t);
  JsonObject dev = cachedConfigDoc["device"][t.device.c_str()].as<JsonObject>();
  if (dev.isNull()) return false;

  if (t.device == "light" && t.kind == TARGET_KIND_DEVICE_LEVEL) {
    dev["modeType"] = 0;
    if (t.on >= 0) dev["mOnOff"] = t.on ? 1 : 0;
    if (t.level >= 0) dev["mLevel"] = clampInt(t.level, 0, 100);
  } else if (t.device == "light" && t.kind == TARGET_KIND_LIGHT_MODE) {
    dev["modeType"] = clampInt(t.level, 0, 1);
    if (t.level == 1) { dev["lastAutoModeType"] = 1; ensureLightScheduleObject(dev); }
  } else if (t.device == "light" && t.kind == TARGET_KIND_LIGHT_SCHEDULE_ENABLED) {
    ensureLightScheduleObject(dev)["enabled"] = t.level > 0 ? 1 : 0;
  } else if (t.device == "light" && t.kind == TARGET_KIND_LIGHT_SCHEDULE_START_HOUR) {
    ensureLightScheduleObject(dev)["startTime"] = hourToSeconds(t.level);
  } else if (t.device == "light" && t.kind == TARGET_KIND_LIGHT_SCHEDULE_END_HOUR) {
    ensureLightScheduleObject(dev)["endTime"] = hourToSeconds(t.level);
  } else if (t.device == "light" && t.kind == TARGET_KIND_LIGHT_SCHEDULE_BRIGHTNESS) {
    ensureLightScheduleObject(dev)["brightness"] = clampInt(t.level, 0, 100);
  } else if (t.device == "blower") {
    if (t.on >= 0) dev["mOnOff"] = t.on ? 1 : 0;
    if (t.level >= 0) {
      int lvl = clampInt(t.level, 0, 100);
      if (!dev["maxSpeed"].isNull()) dev["maxSpeed"] = lvl;
      if (!dev["mLevel"].isNull()) dev["mLevel"] = lvl;
      if (dev["maxSpeed"].isNull() && dev["mLevel"].isNull()) dev["maxSpeed"] = lvl;
    }
  } else if (t.device == "fan" && t.kind == TARGET_KIND_FAN_GEAR) {
    if (t.level >= 0) {
      int lvl = clampInt(t.level, 0, 10);
      if (lvl <= 0) dev["mOnOff"] = 0;
      else { dev["mOnOff"] = 1; dev["mLevel"] = lvl; }
    } else if (t.on >= 0) {
      dev["mOnOff"] = t.on ? 1 : 0;
    }
  } else if (t.device == "fan" && t.kind == TARGET_KIND_FAN_OSCILLATION) {
    dev["shakeLevel"] = normalizeFanOscillation(t.level);
  } else {
    return false;
  }

  return validateCachedConfigCompleteness("after_update");
}

bool targetMatchesConfig(const DeviceTarget& t) {
  if (!configCacheValid) return false;
  if (t.device == "light" && t.kind == TARGET_KIND_DEVICE_LEVEL) {
    JsonObject light = cachedConfigDoc["device"]["light"].as<JsonObject>();
    if (readLightModeTypeFromObject(light) != 0) return false;
  }
  if (t.on >= 0 && cachedDeviceOn(t.device) != t.on) return false;
  if (t.level >= 0 && !(t.device == "fan" && t.kind == TARGET_KIND_FAN_GEAR && t.level <= 0)) {
    if (cachedDeviceLevelForTarget(t) != t.level) return false;
  }
  return true;
}

bool targetMatchesDevState(const DeviceTarget& t) {
  if (!currentState.devStateValid && !currentState.configStateValid) return false;
  if (t.device == "light" && isLightConfigOnlyKind(t.kind)) return false;
  if (t.device == "light") {
    if (t.kind == TARGET_KIND_DEVICE_LEVEL && currentState.lightModeType >= 0 && currentState.lightModeType != 0) return false;
    if (t.on >= 0 && currentState.lightOn != t.on) return false;
    if (t.level >= 0 && currentState.lightLevel != t.level) return false;
    return true;
  }
  if (t.device == "blower") {
    if (t.on >= 0 && currentState.blowerOn != t.on) return false;
    if (t.level >= 0 && currentState.blowerLevel != t.level) return false;
    return true;
  }
  if (t.device == "fan" && t.kind == TARGET_KIND_FAN_GEAR) {
    if (t.on >= 0 && currentState.fanOn != t.on) return false;
    if (t.level >= 0 && t.level > 0 && currentState.fanLevel != t.level) return false;
    return true;
  }
  return false;
}

bool targetRequiresDevVerification(const DeviceTarget& t) {
  if (t.device == "fan" && t.kind == TARGET_KIND_FAN_OSCILLATION) return false;
  if (t.device == "light" && isLightConfigOnlyKind(t.kind)) return false;
  return true;
}

bool targetAlreadySatisfied(const DeviceTarget& t) { return targetMatchesConfig(t) || targetMatchesDevState(t); }

bool sendSetConfigFileFromCache() {
  if (!configCacheValid || !validateCachedConfigCompleteness("before_setConfigFile")) return false;
  String configFileJson;
  serializeJson(cachedConfigDoc, configFileJson);
  if (configFileJson.length() == 0 || configFileJson.length() > MAX_AAAA_PAYLOAD_SIZE) return false;

  pendingSetMsgId = nextMsgId();
  pendingSetJson = "{";
  pendingSetJson += "\"method\":\"setConfigFile\",\"params\":{\"configFile\":";
  pendingSetJson += configFileJson;
  pendingSetJson += "},\"msgId\":\"" + pendingSetMsgId + "\",";
  pendingSetJson += "\"pid\":\"" + String(GGS_PID) + "\",\"pcode\":" + String(GGS_PCODE) + ",\"uid\":\"" + String(GGS_UID) + "\"";
  pendingSetJson += "}";

  opRetryCount = 0;
  setOperationState(OP_SETTING_CONFIG, "sending_setConfigFile");
  return sendJsonAuto(pendingSetJson);
}

void finishOperationSuccess(const String& reason) {
  Serial.println("[OP DONE] success reason=" + reason + " target=" + targetToString(activeTarget));
  activeTargetValid = false;
  pendingSetMsgId = "";
  pendingSetJson = "";
  verifyDevSeen = false;
  verifyConfigSeen = false;
  verifyRequestsSent = false;
  opRetryCount = 0;
  setOperationState(OP_IDLE, "operation_success");
  requestStatePublish("operation_success");
  queuedStartPending = targetQueueCount > 0;
  queuedStartDueMs = millis() + QUEUED_START_DELAY_MS;
}

void failOperation(const String& reason) {
  Serial.println("[OP FAILED] reason=" + reason + " target=" + targetToString(activeTarget));
  activeTargetValid = false;
  pendingSetMsgId = "";
  pendingSetJson = "";
  verifyDevSeen = false;
  verifyConfigSeen = false;
  verifyRequestsSent = false;
  opRetryCount = 0;
  configCacheValid = false;
  cachedConfigDoc.clear();
  setOperationState(OP_IDLE, "operation_failed");
  requestStatePublish("operation_failed");
  queuedStartPending = targetQueueCount > 0;
  queuedStartDueMs = millis() + QUEUED_START_DELAY_MS;
}

bool sameTargetSlot(const DeviceTarget& a, const DeviceTarget& b) { return a.device == b.device && a.kind == b.kind; }

bool enqueueTarget(const DeviceTarget& rawTarget) {
  DeviceTarget t = rawTarget;
  normalizeTarget(t);
  for (int i = 0; i < targetQueueCount; i++) {
    if (sameTargetSlot(targetQueue[i], t)) { targetQueue[i] = t; queuedStartPending = true; queuedStartDueMs = millis() + QUEUED_START_DELAY_MS; return true; }
  }
  if (targetQueueCount >= TARGET_QUEUE_SIZE) return false;
  targetQueue[targetQueueCount++] = t;
  queuedStartPending = true;
  queuedStartDueMs = millis() + QUEUED_START_DELAY_MS;
  return true;
}

bool dequeueTarget(DeviceTarget& out) {
  if (targetQueueCount <= 0) return false;
  out = targetQueue[0];
  for (int i = 1; i < targetQueueCount; i++) targetQueue[i - 1] = targetQueue[i];
  targetQueue[targetQueueCount - 1] = DeviceTarget();
  targetQueueCount--;
  return true;
}

bool applyDeviceTarget(const String& device, TargetKind kind, int on, int level) {
  DeviceTarget t;
  t.device = device;
  t.kind = kind;
  t.on = on;
  t.level = level;
  if (!(device == "light" || device == "fan" || device == "blower")) return false;
  normalizeTarget(t);
  if (!connected || pWriteChar == nullptr) { requestStatePublish("reject_not_ble_connected"); return false; }
  if (opState != OP_IDLE || bleTxInProgress || parserProcessing || !rxStream.empty()) return enqueueTarget(t);
  if (targetAlreadySatisfied(t)) { requestStatePublish("noop_before_start"); return true; }
  activeTarget = t;
  activeTargetValid = true;
  opRetryCount = 0;
  if (!configCacheValid) {
    setOperationState(OP_WAITING_CONFIG, "cache_missing");
    if (!requestGetConfigFile()) failOperation("request_getConfigFile_failed");
    return true;
  }
  if (!updateCachedConfigDevice(activeTarget)) { failOperation("update_cache_failed"); return false; }
  if (!sendSetConfigFileFromCache()) { failOperation("send_setConfigFile_failed"); return false; }
  return true;
}

void verifyOperation() {
  if (opState != OP_VERIFYING || !activeTargetValid) return;
  bool needDev = targetRequiresDevVerification(activeTarget);
  if (!verifyConfigSeen || (needDev && !verifyDevSeen)) return;
  bool cfgOk = targetMatchesConfig(activeTarget);
  bool devOk = needDev ? targetMatchesDevState(activeTarget) : true;
  if (cfgOk && devOk) finishOperationSuccess(needDev ? "verified_config_and_dev" : "verified_config_only");
  else if (opRetryCount < MAX_VERIFY_RETRIES) {
    opRetryCount++;
    verifyRequestsSent = false;
    verifyDevSeen = false;
    verifyConfigSeen = false;
    verifyRequestDueMs = millis() + POST_SET_VERIFY_DELAY_MS;
  } else failOperation("verification_mismatch");
}

void operationTick() {
  if (opState == OP_IDLE) return;
  unsigned long now = millis();
  if (bleTxInProgress) return;
  if (parserProcessing || !rxStream.empty()) return;

  if (opState == OP_WAITING_CONFIG) {
    if (configCacheValid && activeTargetValid) {
      if (targetAlreadySatisfied(activeTarget)) { finishOperationSuccess("noop_after_config_fetch"); return; }
      if (!updateCachedConfigDevice(activeTarget)) { failOperation("update_cache_failed"); return; }
      if (!sendSetConfigFileFromCache()) failOperation("send_setConfigFile_failed");
      return;
    }
    if (now - opStartedAtMs > CONFIG_TIMEOUT_MS) {
      if (opRetryCount++ < MAX_CONFIG_RETRIES) { opStartedAtMs = now; requestGetConfigFile(); }
      else failOperation("getConfigFile_timeout");
    }
    return;
  }

  if (opState == OP_SETTING_CONFIG) {
    if (now - opStartedAtMs > SET_ACK_TIMEOUT_MS) {
      if (opRetryCount++ < MAX_SET_RETRIES && pendingSetJson.length() > 0) { opStartedAtMs = now; sendJsonAuto(pendingSetJson); }
      else failOperation("setConfigFile_ack_timeout");
    }
    return;
  }

  if (opState == OP_VERIFYING) {
    if (!verifyRequestsSent) {
      if (now < verifyRequestDueMs) return;
      verifyRequestsSent = true;
      verifyDevSeen = false;
      verifyConfigSeen = false;
      opStartedAtMs = now;
      bool ok = true;
      if (targetRequiresDevVerification(activeTarget)) ok = requestGetDevSta();
      if (ok) ok = requestGetConfigFile();
      if (!ok) failOperation("send_verify_probe_failed");
      return;
    }
    if (now - opStartedAtMs > VERIFY_TIMEOUT_MS) {
      if (opRetryCount++ < MAX_VERIFY_RETRIES) { verifyRequestsSent = false; verifyRequestDueMs = now + POST_SET_VERIFY_DELAY_MS; }
      else failOperation("verify_timeout");
    }
  }
}

void queuedStartTick() {
  if (!queuedStartPending || targetQueueCount <= 0 || millis() < queuedStartDueMs) return;
  if (!connected || pWriteChar == nullptr || opState != OP_IDLE || bleTxInProgress || parserProcessing || !rxStream.empty()) return;
  DeviceTarget next;
  if (dequeueTarget(next)) {
    queuedStartPending = false;
    applyDeviceTarget(next.device, next.kind, next.on, next.level);
  }
}

// -----------------------------------------------------------------------------
// JSON handlers
// -----------------------------------------------------------------------------
void updateStateFromDeviceRoot(JsonVariant rootVar, bool markDevState) {
  JsonObject root = rootVar.as<JsonObject>();
  if (root.isNull()) return;
  JsonObject sensor = root["sensor"].as<JsonObject>();
  if (!sensor.isNull()) {
    if (!sensor["temp"].isNull()) currentState.temp = sensor["temp"].as<float>();
    if (!sensor["humi"].isNull()) currentState.humi = sensor["humi"].as<float>();
    if (!sensor["vpd"].isNull()) currentState.vpd = sensor["vpd"].as<float>();
  }
  JsonObject fan = root["fan"].as<JsonObject>();
  JsonObject blower = root["blower"].as<JsonObject>();
  JsonObject light = root["light"].as<JsonObject>();
  int v;
  v = readOnFromDeviceObject(fan); if (v != -999) currentState.fanOn = v;
  v = readFanLevelFromObject(fan); if (v != -999) { currentState.fanLevel = v; currentState.fanPercentage = fanLevelToPercentage(v); }
  v = readOnFromDeviceObject(blower); if (v != -999) currentState.blowerOn = v;
  v = readBlowerLevelFromObject(blower); if (v != -999) { currentState.blowerLevel = v; currentState.blowerPercentage = blowerLevelToPercentage(v); }
  v = readOnFromDeviceObject(light); if (v != -999) currentState.lightOn = v;
  v = readLightLevelFromObject(light); if (v != -999) { currentState.lightLevel = v; currentState.lightBrightness = lightLevelToBrightness255(v); }
  v = readLightModeTypeFromObject(light); if (v != -999) currentState.lightModeType = v;
  if (markDevState) { currentState.devStateValid = true; currentState.lastStateRxMs = millis(); }
}

void markInitialConfigReceived(const String& reason) {
  initialConfigReceived = true;
  initialBleBootstrapDone = true;
  bootstrapBleActive = false;
  requestAvailabilityPublish(connected);
  requestStatePublish("initial_config_received");
  requestDiscoveryPublish("initial_config_received");
  requestStatusPublish(reason);
}

void handleGetDevSta(JsonDocument& doc) {
  JsonVariant data = doc["data"];
  if (data.isNull()) return;
  if (!data["device"].isNull()) updateStateFromDeviceRoot(data["device"], true);
  else updateStateFromDeviceRoot(data, true);
  requestStatePublish("getDevSta_received");
  if (opState == OP_VERIFYING) { verifyDevSeen = true; verifyOperation(); }
}

void handleGetConfigFile(JsonDocument& doc) {
  bool ok = cacheConfigFileFromDoc(doc);
  if (!ok) return;
  if (!initialConfigReceived) markInitialConfigReceived("valid_getConfigFile");
  if (opState == OP_WAITING_CONFIG && activeTargetValid) return;
  if (opState == OP_VERIFYING) { verifyConfigSeen = true; verifyOperation(); }
}

void handleSetConfigFileAck(JsonDocument& doc) {
  int code = doc["code"] | -1;
  if (opState != OP_SETTING_CONFIG) return;
  if (code != 200) { failOperation("setConfigFile_bad_ack_code_" + String(code)); return; }
  opRetryCount = 0;
  verifyDevSeen = false;
  verifyConfigSeen = false;
  verifyRequestsSent = false;
  verifyRequestDueMs = millis() + POST_SET_VERIFY_DELAY_MS;
  setOperationState(OP_VERIFYING, "setConfigFile_ack_ok_wait_commit");
}

void processValidatedJson(const String& payload) {
  rxJsonDoc.clear();
  DeserializationError err = deserializeJson(rxJsonDoc, payload);
  if (err || rxJsonDoc.overflowed()) {
    mqttPubDebug("parser", String("invalid_json:") + (err ? err.c_str() : "overflow") + " len=" + String(payload.length()));
    rxJsonDoc.clear();
    return;
  }
  const char* method = rxJsonDoc["method"] | "";
  if (!method || strlen(method) == 0) { rxJsonDoc.clear(); return; }
  Serial.println("----- JSON VALIDATED -----");
  Serial.println(truncateForLog(payload, 1800));
  Serial.println("--------------------------");

  if (MQTT_PUBLISH_RX_RAW_JSON) mqttPubDebug("rx_raw", truncateForLog(payload));
  if (strcmp(method, "getDevSta") == 0) handleGetDevSta(rxJsonDoc);
  else if (strcmp(method, "getConfigFile") == 0) handleGetConfigFile(rxJsonDoc);
  else if (strcmp(method, "setConfigFile") == 0) handleSetConfigFileAck(rxJsonDoc);
  rxJsonDoc.clear();
}

// -----------------------------------------------------------------------------
// RX parsers
// -----------------------------------------------------------------------------
void parserReset(const char* reason) {
  Serial.println(String("[PARSER RESET] ") + reason);
  jsonParser.buf = "";
  jsonParser.collecting = false;
  jsonParser.inString = false;
  jsonParser.escapeNext = false;
  jsonParser.braceDepth = 0;
  jsonParser.frameStartMs = 0;
}

void parserFeedRawByte(uint8_t b) {
  char ch = (char)b;
  if (!jsonParser.collecting) {
    if (ch == '{') { jsonParser.collecting = true; jsonParser.buf = "{"; jsonParser.braceDepth = 1; jsonParser.inString = false; jsonParser.escapeNext = false; jsonParser.frameStartMs = millis(); }
    return;
  }
  if (b < 32 && ch != '\r' && ch != '\n' && ch != '\t') return;
  jsonParser.buf += ch;
  if (jsonParser.escapeNext) { jsonParser.escapeNext = false; return; }
  if (ch == '\\' && jsonParser.inString) { jsonParser.escapeNext = true; return; }
  if (ch == '"') { jsonParser.inString = !jsonParser.inString; return; }
  if (!jsonParser.inString) {
    if (ch == '{') jsonParser.braceDepth++;
    else if (ch == '}') jsonParser.braceDepth--;
  }
  if (jsonParser.braceDepth == 0) {
    String completed = jsonParser.buf;
    parserReset("json_complete");
    processValidatedJson(completed);
  } else if (jsonParser.buf.length() > MAX_AAAA_PAYLOAD_SIZE) parserReset("json_buffer_too_large");
}

void dropRxStreamPrefix(size_t n) {
  if (n == 0) return;
  if (n >= rxStream.size()) { rxStream.clear(); return; }
  size_t remain = rxStream.size() - n;
  memmove(rxStream.data(), rxStream.data() + n, remain);
  rxStream.resize(remain);
}

void processAaaaFrame(const std::vector<uint8_t>& frame) {
  if (frame.size() < 22 || frame[0] != 0xAA || frame[1] != 0xAA) return;
  uint16_t frameType = readU16BE(&frame[2]);
  uint16_t bodyLen = readU16BE(&frame[4]);
  size_t expectedLen = 2 + 2 + 2 + bodyLen + 2;
  if (expectedLen != frame.size() || frameType != 0x0003 || bodyLen < 14) return;

  uint16_t gotFrameCrc = readU16BE(&frame[frame.size() - 2]);
  uint16_t calcFullCrc = crc16Modbus(frame.data(), frame.size() - 2);
  uint16_t calcNoPrefixCrc = crc16Modbus(&frame[2], 4 + bodyLen);
  if (gotFrameCrc != calcFullCrc && gotFrameCrc != calcNoPrefixCrc) { mqttPubDebug("rx_frame_error", "frame_crc_error"); return; }

  const uint8_t* body = &frame[6];
  uint16_t payloadCrc = readU16BE(body + 2);
  uint32_t totalLen = readU32BE(body + 4);
  uint32_t offset = readU32BE(body + 8);
  uint16_t chunkLen = readU16BE(body + 12);
  if ((uint32_t)14 + chunkLen != bodyLen || totalLen == 0 || totalLen > MAX_AAAA_PAYLOAD_SIZE || offset + chunkLen > totalLen) return;

  if (!aaaaRx.active || aaaaRx.payloadCrc != payloadCrc || aaaaRx.totalLen != totalLen) {
    aaaaRx.reset();
    aaaaRx.active = true;
    aaaaRx.payloadCrc = payloadCrc;
    aaaaRx.totalLen = totalLen;
    aaaaRx.startedAtMs = millis();
    aaaaRx.data.assign(totalLen, 0);
    aaaaRx.seen.assign(totalLen, 0);
  }

  const uint8_t* chunk = body + 14;
  for (uint16_t i = 0; i < chunkLen; i++) {
    uint32_t pos = offset + i;
    if (aaaaRx.seen[pos] == 0) { aaaaRx.seen[pos] = 1; aaaaRx.received++; }
    aaaaRx.data[pos] = chunk[i];
  }

  if (aaaaRx.received < aaaaRx.totalLen) return;
  if (crc16Modbus(aaaaRx.data.data(), aaaaRx.totalLen) != aaaaRx.payloadCrc) { aaaaRx.reset(); return; }

  String payload;
  payload.reserve(aaaaRx.totalLen + 1);
  for (uint32_t i = 0; i < aaaaRx.totalLen; i++) payload += (char)aaaaRx.data[i];
  aaaaRx.reset();
  processValidatedJson(payload);
}

void parseRxStream() {
  size_t pos = 0;
  while (pos < rxStream.size()) {
    if (rxStream[pos] == 0xAA) {
      if (pos + 2 > rxStream.size()) break;
      if (rxStream[pos + 1] != 0xAA) { parserFeedRawByte(rxStream[pos++]); continue; }
      if (pos + 6 > rxStream.size()) break;
      uint16_t bodyLen = readU16BE(&rxStream[pos + 4]);
      if (bodyLen > 4096) { pos++; continue; }
      size_t totalFrameLen = 2 + 2 + 2 + bodyLen + 2;
      if (pos + totalFrameLen > rxStream.size()) break;
      std::vector<uint8_t> frame(rxStream.begin() + pos, rxStream.begin() + pos + totalFrameLen);
      pos += totalFrameLen;
      processAaaaFrame(frame);
    } else parserFeedRawByte(rxStream[pos++]);
  }
  if (pos > 0) dropRxStreamPrefix(pos);
}

void parserTick() {
  if (jsonParser.collecting && jsonParser.frameStartMs > 0 && millis() - jsonParser.frameStartMs > JSON_RX_TIMEOUT_MS) parserReset("json_timeout");
  if (aaaaRx.active && aaaaRx.startedAtMs > 0 && millis() - aaaaRx.startedAtMs > AAAA_RX_TIMEOUT_MS) { aaaaRx.reset(); mqttPubDebug("rx_frame_error", "assembly_timeout"); }
  if (rxStream.size() > MAX_RX_STREAM_BUFFER) { rxStream.clear(); aaaaRx.reset(); parserReset("rx_stream_too_large_reset"); }
}

size_t rxNotifyNextIndex(size_t idx) { return (idx + 1) % RX_NOTIFY_QUEUE_SIZE; }

void clearNotifyQueue() {
  portENTER_CRITICAL(&rxNotifyMux);
  rxNotifyHead = rxNotifyTail = 0;
  rxNotifyOverflow = false;
  portEXIT_CRITICAL(&rxNotifyMux);
}

void drainNotifyQueueToRxStream() {
  bool hadOverflow = false;
  portENTER_CRITICAL(&rxNotifyMux);
  if (rxNotifyOverflow) { hadOverflow = true; rxNotifyOverflow = false; rxNotifyHead = rxNotifyTail = 0; }
  portEXIT_CRITICAL(&rxNotifyMux);
  if (hadOverflow) { rxStream.clear(); aaaaRx.reset(); parserReset("notify_queue_overflow"); return; }
  while (true) {
    bool hasByte = false;
    uint8_t b = 0;
    portENTER_CRITICAL(&rxNotifyMux);
    if (rxNotifyTail != rxNotifyHead) { b = rxNotifyQueue[rxNotifyTail]; rxNotifyTail = rxNotifyNextIndex(rxNotifyTail); hasByte = true; }
    portEXIT_CRITICAL(&rxNotifyMux);
    if (!hasByte) break;
    rxStream.push_back(b);
    if (rxStream.size() > MAX_RX_STREAM_BUFFER) { rxStream.clear(); aaaaRx.reset(); parserReset("rx_stream_too_large_during_drain"); break; }
  }
}

void processRxStreamInLoop() {
  if (rxStream.empty()) return;
  parserProcessing = true;
  parseRxStream();
  parserProcessing = false;
}

// -----------------------------------------------------------------------------
// MQTT callback
// -----------------------------------------------------------------------------
String normalizePayloadText(const String& msg) {
  String m = msg;
  m.trim();
  m.replace("\"", "");
  m.replace("'", "");
  m.toLowerCase();
  return m;
}

bool mqttPayloadMeansOn(const String& msg) {
  String m = normalizePayloadText(msg);
  return m == "on" || m == "1" || m == "true" || m.indexOf("state:on") >= 0 || m.indexOf("state:true") >= 0;
}

bool mqttPayloadMeansOff(const String& msg) {
  String m = normalizePayloadText(msg);
  return m == "off" || m == "0" || m == "false" || m.indexOf("state:off") >= 0 || m.indexOf("state:false") >= 0;
}

String extractJsonNumber(const String& src, const String& key) {
  int p = src.indexOf("\"" + key + "\":");
  if (p < 0) return "";
  p += key.length() + 3;
  while (p < src.length() && src[p] == ' ') p++;
  int end = p;
  while (end < src.length()) {
    char c = src[end];
    if (!(isDigit(c) || c == '.' || c == '-')) break;
    end++;
  }
  return end > p ? src.substring(p, end) : "";
}

bool parseLightModePayload(const String& msg, int& outMode) {
  String m = normalizePayloadText(msg);
  if (m == "manual" || m == "0" || m.indexOf("manual") >= 0) { outMode = 0; return true; }
  if (m == "time_slot" || m == "timeslot" || m == "auto" || m == "automatic" || m == "1" || m.indexOf("time_slot") >= 0 || m.indexOf("timeslot") >= 0 || m.indexOf("auto") >= 0) { outMode = 1; return true; }
  return false;
}

bool isKnownMqttCommandTopic(const String& t) {
  for (size_t i = 0; i < MQTT_COMMAND_TOPIC_COUNT; i++) if (t == mqttCommandTopic(i)) return true;
  return false;
}

void mqttCallback(char* topicPtr, byte* payload, unsigned int length) {
  String msg;
  msg.reserve(length + 1);
  for (unsigned int i = 0; i < length; i++) msg += (char)payload[i];
  String t(topicPtr);
  Serial.println("[MQTT RX] " + t + " => " + truncateForLog(msg, 260));
  if (isKnownMqttCommandTopic(t) && msg.length() == 0) return;

  if (t == topic("/cmd/raw")) { if (opState == OP_IDLE && !bleTxInProgress && rxStream.empty()) sendJsonAuto(msg); return; }
  if (t == topic("/cmd/getDevSta")) { if (opState != OP_SETTING_CONFIG && !bleTxInProgress && rxStream.empty()) requestGetDevSta(); return; }
  if (t == topic("/cmd/getConfigFile")) { if (opState != OP_SETTING_CONFIG && !bleTxInProgress && rxStream.empty()) requestGetConfigFile(); return; }
  if (t == topic("/cmd/discovery/republish")) { requestDiscoveryPublish("manual_republish"); return; }
  if (t == "homeassistant/status") { if (msg == "online") requestDiscoveryPublish("homeassistant_online"); return; }

  if (t == topic("/cmd/light/mode/set")) { int mode = -1; if (parseLightModePayload(msg, mode)) applyDeviceTarget("light", TARGET_KIND_LIGHT_MODE, -1, mode); return; }
  if (t == topic("/cmd/light/schedule/enabled/set")) { if (mqttPayloadMeansOn(msg)) applyDeviceTarget("light", TARGET_KIND_LIGHT_SCHEDULE_ENABLED, -1, 1); else if (mqttPayloadMeansOff(msg)) applyDeviceTarget("light", TARGET_KIND_LIGHT_SCHEDULE_ENABLED, -1, 0); return; }
  if (t == topic("/cmd/light/schedule/start_hour/set")) { applyDeviceTarget("light", TARGET_KIND_LIGHT_SCHEDULE_START_HOUR, -1, clampInt(msg.toInt(), 0, 23)); return; }
  if (t == topic("/cmd/light/schedule/end_hour/set")) { applyDeviceTarget("light", TARGET_KIND_LIGHT_SCHEDULE_END_HOUR, -1, clampInt(msg.toInt(), 0, 23)); return; }
  if (t == topic("/cmd/light/schedule/brightness/set")) { applyDeviceTarget("light", TARGET_KIND_LIGHT_SCHEDULE_BRIGHTNESS, -1, clampInt(msg.toInt(), 0, 100)); return; }
  if (t == topic("/cmd/light/brightness/set")) { int level = brightness255ToLightLevel(clampInt(msg.toInt(), 0, 255)); applyDeviceTarget("light", TARGET_KIND_DEVICE_LEVEL, level > 0 ? 1 : 0, level); return; }
  if (t == topic("/cmd/fan/percentage/set")) { int pct = clampInt(msg.toInt(), 0, 100); int gear = percentageToFanLevel(pct); applyDeviceTarget("fan", TARGET_KIND_FAN_GEAR, pct > 0 ? 1 : 0, gear); return; }
  if (t == topic("/cmd/fan/gear/set")) { applyDeviceTarget("fan", TARGET_KIND_FAN_GEAR, 1, clampInt(msg.toInt(), 1, 10)); return; }
  if (t == topic("/cmd/fan/oscillation/set")) { applyDeviceTarget("fan", TARGET_KIND_FAN_OSCILLATION, -1, normalizeFanOscillation(msg.toInt())); return; }
  if (t == topic("/cmd/blower/percentage/set")) { int lvl = percentageToBlowerLevel(msg.toInt()); applyDeviceTarget("blower", TARGET_KIND_DEVICE_LEVEL, lvl > 0 ? 1 : 0, lvl); return; }
  if (t == topic("/cmd/light/set")) { String levelS = extractJsonNumber(msg, "level"); if (levelS.length()) { int level = clampInt(levelS.toInt(), 0, 100); applyDeviceTarget("light", TARGET_KIND_DEVICE_LEVEL, level > 0 ? 1 : 0, level); } else if (mqttPayloadMeansOn(msg)) applyDeviceTarget("light", TARGET_KIND_DEVICE_LEVEL, 1, -1); else if (mqttPayloadMeansOff(msg)) applyDeviceTarget("light", TARGET_KIND_DEVICE_LEVEL, 0, -1); return; }
  if (t == topic("/cmd/fan/set")) { String levelS = extractJsonNumber(msg, "level"); if (levelS.length()) { int gear = clampInt(levelS.toInt(), 0, 10); applyDeviceTarget("fan", TARGET_KIND_FAN_GEAR, gear > 0 ? 1 : 0, gear); } else if (mqttPayloadMeansOn(msg)) applyDeviceTarget("fan", TARGET_KIND_FAN_GEAR, 1, -1); else if (mqttPayloadMeansOff(msg)) applyDeviceTarget("fan", TARGET_KIND_FAN_GEAR, 0, -1); return; }
  if (t == topic("/cmd/blower/set")) { String levelS = extractJsonNumber(msg, "level"); if (levelS.length()) { int level = clampInt(levelS.toInt(), 0, 100); applyDeviceTarget("blower", TARGET_KIND_DEVICE_LEVEL, level > 0 ? 1 : 0, level); } else if (mqttPayloadMeansOn(msg)) applyDeviceTarget("blower", TARGET_KIND_DEVICE_LEVEL, 1, -1); else if (mqttPayloadMeansOff(msg)) applyDeviceTarget("blower", TARGET_KIND_DEVICE_LEVEL, 0, -1); return; }
}

// -----------------------------------------------------------------------------
// Wi-Fi / MQTT
// -----------------------------------------------------------------------------
void connectWifi() {
  if (WiFi.status() == WL_CONNECTED) return;
  Serial.print("[WIFI] Connecting to ");
  Serial.println(WIFI_SSID);
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  int tries = 0;
  while (WiFi.status() != WL_CONNECTED && tries < 40) { delay(500); Serial.print("."); tries++; }
  Serial.println();
  if (WiFi.status() == WL_CONNECTED) { Serial.print("[WIFI] OK, IP = "); Serial.println(WiFi.localIP()); }
  else Serial.println("[WIFI] failed");
}

bool clearRetainedCommandTopic(const char* t) {
  bool ok = mqttClient.publish(t, (const uint8_t*)"", 0, true);
  Serial.println(String("[MQTT RETAIN CLEANUP] ") + t + " -> " + (ok ? "OK" : "FAIL"));
  delay(20);
  return ok;
}

bool clearRetainedCommandTopicsBeforeSubscribe() {
  bool allOk = true;
  for (size_t i = 0; i < MQTT_COMMAND_TOPIC_COUNT; i++) {
    String commandTopic = mqttCommandTopic(i);
    allOk &= clearRetainedCommandTopic(commandTopic.c_str());
  }
  return allOk;
}

bool subscribeTopics() {
  bool ok = true;
  ok &= mqttClient.subscribe(topic("/cmd/raw").c_str());
  ok &= mqttClient.subscribe(topic("/cmd/getDevSta").c_str());
  ok &= mqttClient.subscribe(topic("/cmd/getConfigFile").c_str());
  ok &= mqttClient.subscribe(topic("/cmd/light/set").c_str());
  ok &= mqttClient.subscribe(topic("/cmd/light/brightness/set").c_str());
  ok &= mqttClient.subscribe(topic("/cmd/light/mode/set").c_str());
  ok &= mqttClient.subscribe(topic("/cmd/light/schedule/enabled/set").c_str());
  ok &= mqttClient.subscribe(topic("/cmd/light/schedule/start_hour/set").c_str());
  ok &= mqttClient.subscribe(topic("/cmd/light/schedule/end_hour/set").c_str());
  ok &= mqttClient.subscribe(topic("/cmd/light/schedule/brightness/set").c_str());
  ok &= mqttClient.subscribe(topic("/cmd/fan/set").c_str());
  ok &= mqttClient.subscribe(topic("/cmd/fan/percentage/set").c_str());
  ok &= mqttClient.subscribe(topic("/cmd/fan/gear/set").c_str());
  ok &= mqttClient.subscribe(topic("/cmd/fan/oscillation/set").c_str());
  ok &= mqttClient.subscribe(topic("/cmd/blower/set").c_str());
  ok &= mqttClient.subscribe(topic("/cmd/blower/percentage/set").c_str());
  ok &= mqttClient.subscribe(topic("/cmd/discovery/republish").c_str());
  ok &= mqttClient.subscribe("homeassistant/status");
  return ok;
}

bool mqttConnectAllowedNow() {
  if (WiFi.status() != WL_CONNECTED) return false;
  if (bootstrapBleActive) return false;
  if (opState != OP_IDLE || bleTxInProgress || parserProcessing || !rxStream.empty()) return false;
  return initialBleBootstrapDone || !connected;
}

void connectMqtt() {
  if (mqttClient.connected()) return;
  if (!mqttConnectAllowedNow()) return;
  if (millis() - lastMqttRetry < MQTT_RECONNECT_INTERVAL_MS) return;
  lastMqttRetry = millis();

  if (!mqttBufferConfigured) mqttBufferConfigured = mqttClient.setBufferSize(MQTT_BUFFER_SIZE);
  String clientId = "ESP32_GGS_" + String((uint32_t)ESP.getEfuseMac(), HEX);
  bool ok;
  if (strlen(MQTT_USER) > 0 && String(MQTT_USER) != "YOUR_MQTT_USER") {
    ok = mqttClient.connect(clientId.c_str(), MQTT_USER, MQTT_PASS, topic("/availability").c_str(), 0, true, "offline");
  } else {
    ok = mqttClient.connect(clientId.c_str(), topic("/availability").c_str(), 0, true, "offline");
  }
  if (ok) {
    Serial.println("[MQTT] connected");
    mqttConsecutiveFails = 0;
    mqttPublishCooldownUntilMs = 0;
    clearRetainedCommandTopicsBeforeSubscribe();
    if (!subscribeTopics()) { mqttClient.disconnect(); return; }
    requestAvailabilityPublish(connected);
    requestStatusPublish("mqtt_connect");
    requestDiscoveryPublish("mqtt_connect");
    requestStatePublish("mqtt_connect");
  } else Serial.println("[MQTT] failed rc=" + String(mqttClient.state()));
}

void logMqttStateTransitions() {
  bool nowMqtt = mqttClient.connected();
  if (nowMqtt != prevMqttConnected) { Serial.println(String("[MQTT STATE] ") + (nowMqtt ? "connected" : "disconnected")); prevMqttConnected = nowMqtt; }
}

// -----------------------------------------------------------------------------
// BLE callbacks/core
// -----------------------------------------------------------------------------
class MyClientCallback : public BLEClientCallbacks {
  void onConnect(BLEClient* pclient) override { Serial.println("[BLE] Client connected callback"); }
  void onDisconnect(BLEClient* pclient) override { connected = false; currentState.bleConnected = false; bleDisconnectPending = true; Serial.println("[BLE] Disconnected callback"); }
};

class MyAdvertisedDeviceCallbacks : public BLEAdvertisedDeviceCallbacks {
  void onResult(BLEAdvertisedDevice advertisedDevice) override {
    String addr = advertisedDevice.getAddress().toString().c_str();
    String name = advertisedDevice.haveName() ? advertisedDevice.getName().c_str() : "";
    Serial.println("[SCAN] " + addr + " name=" + name);
    bool macMatches = strlen(GGS_TARGET_MAC) > 0 && String(GGS_TARGET_MAC) != "AA:BB:CC:DD:EE:FF" && addr.equalsIgnoreCase(GGS_TARGET_MAC);
    if (name == GGS_TARGET_NAME || macMatches) {
      Serial.println("[SCAN] Target found");
      if (foundDevice != nullptr) delete foundDevice;
      foundDevice = new BLEAdvertisedDevice(advertisedDevice);
      BLEDevice::getScan()->stop();
      doConnect = true;
    }
  }
};

void notifyCallback(BLERemoteCharacteristic* c, uint8_t* pData, size_t length, bool isNotify) {
  portENTER_CRITICAL(&rxNotifyMux);
  rxNotifyCallbacks++;
  rxNotifyBytes += length;
  for (size_t i = 0; i < length; i++) {
    size_t nextHead = rxNotifyNextIndex(rxNotifyHead);
    if (nextHead == rxNotifyTail) { rxNotifyOverflow = true; rxNotifyDroppedBytes++; break; }
    rxNotifyQueue[rxNotifyHead] = pData[i];
    rxNotifyHead = nextHead;
  }
  portEXIT_CRITICAL(&rxNotifyMux);
}

void cleanupFoundDevice() { if (foundDevice != nullptr) { delete foundDevice; foundDevice = nullptr; } }

void cleanupBleClient(const char* reason) {
  Serial.println(String("[BLE CLEANUP] reason=") + reason);
  if (pClient != nullptr) {
    if (pClient->isConnected()) { pClient->disconnect(); delay(150); }
    delete pClient;
    pClient = nullptr;
  }
  pNotifyChar = nullptr;
  pWriteChar = nullptr;
}

void startScan() {
  if (connected || doConnect) return;
  if ((long)(millis() - bleConnectRetryAfterMs) < 0) return;
  BLEScan* pScan = BLEDevice::getScan();
  static MyAdvertisedDeviceCallbacks advertisedCallbacks;
  pScan->setAdvertisedDeviceCallbacks(&advertisedCallbacks);
  pScan->setActiveScan(true);
  pScan->setInterval(120);
  pScan->setWindow(60);
  pScan->clearResults();
  pScan->start(10, false);
  lastScanAt = millis();
}

void startBleBootstrap() {
  bootstrapBleActive = true;
  initialConfigReceived = false;
  initialBleBootstrapDone = false;
  bootstrapStep = 0;
  bootstrapStartedAtMs = millis();
  bootstrapNextActionAtMs = millis() + BLE_BOOTSTRAP_START_DELAY_MS;
  requestStatusPublish("ble_bootstrap_started");
}

void completeBootstrapByTimeout() {
  if (!bootstrapBleActive) return;
  bootstrapBleActive = false;
  initialBleBootstrapDone = true;
  requestAvailabilityPublish(connected);
  requestStatusPublish("bootstrap_timeout");
  requestStatePublish("bootstrap_timeout");
  requestDiscoveryPublish("bootstrap_timeout");
}

bool canSendBootstrapProbeNow() {
  return connected && pWriteChar != nullptr && opState == OP_IDLE && !bleTxInProgress && !parserProcessing && rxStream.empty();
}

void bootstrapTick() {
  if (!bootstrapBleActive || !connected) return;
  unsigned long now = millis();
  if (initialConfigReceived) { bootstrapBleActive = false; initialBleBootstrapDone = true; return; }
  if (now - bootstrapStartedAtMs > BLE_BOOTSTRAP_TIMEOUT_MS) { completeBootstrapByTimeout(); return; }
  if (now < bootstrapNextActionAtMs || !canSendBootstrapProbeNow()) return;
  if (bootstrapStep == 0) {
    if (requestGetDevSta()) { bootstrapStep = 1; bootstrapNextActionAtMs = millis() + BLE_BOOTSTRAP_BETWEEN_REQ_MS; }
    else bootstrapNextActionAtMs = millis() + 1000;
  } else if (bootstrapStep == 1) {
    if (requestGetConfigFile()) { bootstrapStep = 2; bootstrapNextActionAtMs = millis() + 1000; }
    else bootstrapNextActionAtMs = millis() + 1000;
  }
}

bool connectToServer() {
  if (!foundDevice) return false;
  if ((long)(millis() - bleConnectRetryAfterMs) < 0) return false;
  Serial.print("[BLE] Connecting to ");
  Serial.println(foundDevice->getAddress().toString().c_str());
  BLEDevice::getScan()->stop();
  delay(350);
  cleanupBleClient("before_new_connect_attempt");
  pClient = BLEDevice::createClient();
  if (pClient == nullptr) { bleConnectRetryAfterMs = millis() + BLE_CONNECT_RETRY_BACKOFF_MS; return false; }
  static MyClientCallback clientCallbacks;
  pClient->setClientCallbacks(&clientCallbacks);
  bool okConnect = pClient->connect(foundDevice);
  if (!okConnect) {
    bleConsecutiveConnectFailures++;
    cleanupBleClient("connect_failed");
    cleanupFoundDevice();
    bleConnectRetryAfterMs = millis() + BLE_CONNECT_RETRY_BACKOFF_MS;
    if (bleConsecutiveConnectFailures >= BLE_CONNECT_REINIT_AFTER_FAILS) {
      BLEDevice::deinit(true);
      delay(1000);
      BLEDevice::init("ESP32-GGS");
      bleConsecutiveConnectFailures = 0;
    }
    return false;
  }
  bleConsecutiveConnectFailures = 0;
  pClient->setMTU(517);
  delay(500);

  std::map<std::string, BLERemoteService*>* services = pClient->getServices();
  if (services == nullptr) { cleanupBleClient("services_null"); cleanupFoundDevice(); return false; }
  pNotifyChar = nullptr;
  pWriteChar = nullptr;
  for (auto const& svcPair : *services) {
    BLERemoteService* svc = svcPair.second;
    if (pNotifyChar == nullptr) pNotifyChar = svc->getCharacteristic(NOTIFY_UUID);
    if (pWriteChar == nullptr) pWriteChar = svc->getCharacteristic(WRITE_UUID);
  }
  if (pNotifyChar == nullptr || pWriteChar == nullptr || !pNotifyChar->canNotify()) { cleanupBleClient("missing_chars"); cleanupFoundDevice(); return false; }
  pNotifyChar->registerForNotify(notifyCallback);
  BLERemoteDescriptor* p2902 = pNotifyChar->getDescriptor(BLEUUID((uint16_t)0x2902));
  if (p2902 != nullptr) { uint8_t val[] = {0x01, 0x00}; p2902->writeValue(val, 2, true); }

  connected = true;
  currentState.bleConnected = true;
  cleanupFoundDevice();
  clearNotifyQueue();
  rxStream.clear();
  aaaaRx.reset();
  parserReset("ble_connected");
  startBleBootstrap();
  return true;
}

void handleBleDisconnectEvent() {
  if (!bleDisconnectPending) return;
  bleDisconnectPending = false;
  connected = false;
  currentState.bleConnected = false;
  bleTxInProgress = false;
  bootstrapBleActive = false;
  clearNotifyQueue();
  rxStream.clear();
  aaaaRx.reset();
  parserReset("ble_disconnect");
  if (opState != OP_IDLE) failOperation("ble_disconnect");
  requestAvailabilityPublish(false);
  requestStatusPublish("ble_disconnect");
}

// -----------------------------------------------------------------------------
// Arduino setup/loop
// -----------------------------------------------------------------------------
void serviceBleRxFirst() {
  drainNotifyQueueToRxStream();
  processRxStreamInLoop();
}

void setup() {
  Serial.begin(115200);
  delay(1500);
  bootStartedAtMs = millis();
  Serial.println();
  Serial.println("=================================");
  Serial.println("[BOOT] ESP32 GGS Bridge");
  Serial.println("=================================");
  Serial.println("[BOOT] FW_VERSION=" + String(FW_VERSION));
  initializeLocalStateDefaults();
  rxStream.reserve(2048);
  jsonParser.buf.reserve(1024);
  connectWifi();
  mqttClient.setServer(MQTT_HOST, MQTT_PORT);
  mqttClient.setCallback(mqttCallback);
  mqttClient.setKeepAlive(30);
  mqttClient.setSocketTimeout(5);
  BLEDevice::init("ESP32-GGS");
  startScan();
}

void loop() {
  handleBleDisconnectEvent();
  serviceBleRxFirst();
  parserTick();
  bleTxWatchdogTick();
  if (parserProcessing || !rxStream.empty()) { delay(1); return; }

  if (doConnect && !connected) {
    doConnect = false;
    if (!connectToServer()) startScan();
  }

  bootstrapTick();
  serviceBleRxFirst();
  parserTick();
  if (parserProcessing || !rxStream.empty()) { delay(1); return; }

  operationTick();
  queuedStartTick();
  serviceBleRxFirst();
  parserTick();
  if (parserProcessing || !rxStream.empty() || bleTxInProgress) { delay(1); return; }

  if (WiFi.status() != WL_CONNECTED) connectWifi();
  if (!mqttClient.connected()) connectMqtt();
  if (mqttClient.connected() && !bleTxInProgress && !parserProcessing && rxStream.empty()) mqttClient.loop();
  logMqttStateTransitions();

  availabilityPublishTick();
  statusPublishTick();
  discoveryTick();
  statePublishTick();

  if (!connected && !doConnect && millis() - lastScanAt > 15000) startScan();

  if (connected && initialBleBootstrapDone) {
    if (opState == OP_IDLE && !bleTxInProgress && !parserProcessing && rxStream.empty() && millis() - lastProbeAt > PROBE_INTERVAL_MS) requestGetDevSta();
    if (currentState.lastStateRxMs > 0 && millis() - currentState.lastStateRxMs > STATE_STALE_TIMEOUT_MS) {
      if (pClient != nullptr) pClient->disconnect();
      connected = false;
      currentState.bleConnected = false;
      requestAvailabilityPublish(false);
      requestStatusPublish("state_stale_reconnect");
      delay(1000);
      startScan();
    }
  }

  delay(5);
}
