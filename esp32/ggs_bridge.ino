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

#include <esp_task_wdt.h>
#include <esp_system.h>

#if __has_include(<esp_arduino_version.h>)
#include <esp_arduino_version.h>
#endif

#include <vector>
#include <map>
#include <math.h>
#include <string.h>

// =====================================================
// LOCAL CONFIG
// =====================================================
// Real credentials must stay in config.h, which is ignored by Git.
// config.example.h contains safe placeholders and documents the expected fields.
#if __has_include("config.h")
#include "config.h"
#else
#include "config.example.h"
#endif

// =====================================================
// GGS / BLE UUIDs
// =====================================================
static BLEUUID NOTIFY_UUID("0000ff01-0000-1000-8000-00805f9b34fb");
static BLEUUID WRITE_UUID ("0000ff02-0000-1000-8000-00805f9b34fb");

// =====================================================
// MQTT TOPICS / DEVICE METADATA
// =====================================================
const char* TOPIC_BASE                    = MQTT_TOPIC_BASE;

const char* TOPIC_LEGACY_AVAILABILITY     = MQTT_TOPIC_BASE "/availability";
const char* TOPIC_BRIDGE_AVAILABILITY     = MQTT_TOPIC_BASE "/bridge/availability";
const char* TOPIC_BLE_AVAILABILITY        = MQTT_TOPIC_BASE "/ble/availability";

const char* TOPIC_STATUS_JSON             = MQTT_TOPIC_BASE "/status/json";
const char* TOPIC_DIAGNOSTICS_JSON        = MQTT_TOPIC_BASE "/diagnostics/json";
const char* TOPIC_COMMAND_STATUS          = MQTT_TOPIC_BASE "/command/status";
const char* TOPIC_LAST_ERROR              = MQTT_TOPIC_BASE "/status/last_error";

const char* DISCOVERY_PREFIX              = HA_DISCOVERY_PREFIX;
const char* DEVICE_ID                     = HA_DEVICE_ID;
const char* DEVICE_NAME                   = HA_DEVICE_NAME;
const char* DEVICE_MODEL                  = HA_DEVICE_MODEL;
const char* DEVICE_MANUFACTURER           = HA_DEVICE_MANUFACTURER;
const char* FW_VERSION                    = "1.2.0-esp32-ggs-setconfigfield-primary";
const char* TOPIC_HA_STATUS                = HA_DISCOVERY_PREFIX "/status";

// =====================================================
// LIMITES / DEFAULTS
// =====================================================
const int FAN_OSC_MIN  = 1;
const int FAN_OSC_MAX  = 10;

const int DEFAULT_LIGHT_LEVEL_ON  = 100;
const int DEFAULT_FAN_GEAR_ON     = 5;
const int DEFAULT_BLOWER_LEVEL_ON = 50;

const int DEFAULT_LIGHT_SCHEDULE_ENABLED    = 1;
const int DEFAULT_LIGHT_SCHEDULE_WEEKMASK   = 127;
const int DEFAULT_LIGHT_SCHEDULE_START_H    = 20;
const int DEFAULT_LIGHT_SCHEDULE_END_H      = 8;
const int DEFAULT_LIGHT_SCHEDULE_BRIGHTNESS = 100;

// =====================================================
// BUFFERS / TIMEOUTS
// =====================================================
const size_t MQTT_BUFFER_SIZE        = 4096;
const size_t RX_JSON_DOC_CAPACITY    = 16384;
const size_t CONFIG_DOC_CAPACITY     = 16384;

const size_t RAW_WRITE_LIMIT         = 500;
const size_t AAAA_CHUNK_SIZE         = 80;
const size_t MAX_RX_STREAM_BUFFER    = 8192;
const size_t MAX_AAAA_PAYLOAD_SIZE   = 16384;
const size_t RX_NOTIFY_QUEUE_SIZE    = 8192;

const unsigned long PROBE_INTERVAL_MS          = 30000;
const unsigned long STATE_STALE_TIMEOUT_MS     = 90000;
const unsigned long JSON_RX_TIMEOUT_MS         = 2500;
const unsigned long AAAA_RX_TIMEOUT_MS         = 7000;

const unsigned long CONFIG_TIMEOUT_MS          = 8000;
const unsigned long SET_ACK_TIMEOUT_MS         = 8000;
const unsigned long VERIFY_TIMEOUT_MS          = 10000;
const int MAX_CONFIG_RETRIES = 2;
const int MAX_SET_RETRIES    = 1;
const int MAX_VERIFY_RETRIES = 2;

const unsigned long BLE_FRAME_DELAY_MS         = 35;
const unsigned long BLE_TX_STUCK_TIMEOUT_MS    = 15000;
const unsigned long POST_SET_VERIFY_DELAY_MS   = 1500;
const unsigned long MQTT_RECONNECT_INTERVAL_MS = 5000;
const unsigned long QUEUED_START_DELAY_MS      = 120;

const unsigned long BLE_BOOTSTRAP_START_DELAY_MS = 900;
const unsigned long BLE_BOOTSTRAP_BETWEEN_REQ_MS = 650;
const unsigned long BLE_BOOTSTRAP_TIMEOUT_MS     = 30000;
const unsigned long MQTT_OFFLINE_FALLBACK_MS     = 60000;
const unsigned long MQTT_STEP_INTERVAL_MS        = 160;
const unsigned long MQTT_FAIL_COOLDOWN_MS        = 3000;
const unsigned long FORCE_CONFIG_SAFE_DELAY_MS   = 30000;
const unsigned long BLE_CONNECT_RETRY_BACKOFF_MS = 5000;
const int BLE_CONNECT_REINIT_AFTER_FAILS         = 3;

const unsigned long MQTT_RETAIN_CLEANUP_STEP_DELAY_MS = 25;
const int MQTT_RETAIN_CLEANUP_ATTEMPTS = 2;
const bool MQTT_RETAIN_CLEANUP_REQUIRED = true;


// =====================================================
// ROBUSTEZ / WATCHDOG / RECOVERY
// =====================================================
const unsigned long TASK_WDT_TIMEOUT_MS = 30000;

const unsigned long RX_STREAM_IDLE_PARTIAL_TIMEOUT_MS = 1500;
const unsigned long BLE_BUSY_HARD_TIMEOUT_MS          = 20000;

const unsigned long WIFI_DOWN_RECOVERY_MS = 120000;
const unsigned long MQTT_DOWN_RECOVERY_MS = 300000;
const unsigned long BLE_DOWN_REINIT_MS    = 300000;

const unsigned long BLE_STACK_REINIT_COOLDOWN_MS = 120000;
const int BLE_STALE_RECONNECTS_BEFORE_REINIT = 3;

const uint32_t LOW_HEAP_LIMIT        = 16000;
const uint32_t CRITICAL_HEAP_LIMIT   = 10000;
const unsigned long LOW_HEAP_HOLD_MS = 180000;
unsigned long lastLowHeapWarningAtMs = 0;
const unsigned long LOW_HEAP_WARNING_COOLDOWN_MS = 60000;

bool taskWdtEnabled = false;

unsigned long rxStreamLastAppendAtMs = 0;
unsigned long bleBusySinceMs = 0;

unsigned long lastWifiOkAtMs = 0;
unsigned long lastMqttOkAtMs = 0;
unsigned long lastBleOkAtMs = 0;
unsigned long lowHeapSinceMs = 0;
unsigned long lastWifiRecoveryAtMs = 0;
unsigned long lastBleStackReinitAtMs = 0;

int bleStaleReconnectCount = 0;

// =====================================================
// DEBUG
// =====================================================
const bool MQTT_DEBUG_ENABLED = true;
const bool MQTT_DEBUG_DROP_DURING_BLE_TX = true;
const bool MQTT_DEBUG_DROP_WHEN_BLE_BUSY = true;
const size_t MQTT_DEBUG_MAX_LEN = 360;
const bool MQTT_PUBLISH_RX_RAW_JSON = false;
const bool SERIAL_VERBOSE_CONFIG_DUMP = false;

// =====================================================
// MQTT COMMAND TOPICS CONHECIDOS
// =====================================================
const char* TOPIC_CMD_RAW                         = MQTT_TOPIC_BASE "/cmd/raw";
const char* TOPIC_CMD_GET_DEV_STA                 = MQTT_TOPIC_BASE "/cmd/getDevSta";
const char* TOPIC_CMD_GET_CONFIG_FILE             = MQTT_TOPIC_BASE "/cmd/getConfigFile";
const char* TOPIC_CMD_LIGHT_SET                   = MQTT_TOPIC_BASE "/cmd/light/set";
const char* TOPIC_CMD_LIGHT_BRIGHTNESS_SET        = MQTT_TOPIC_BASE "/cmd/light/brightness/set";
const char* TOPIC_CMD_LIGHT_MODE_SET              = MQTT_TOPIC_BASE "/cmd/light/mode/set";
const char* TOPIC_CMD_LIGHT_SCHEDULE_ENABLED_SET  = MQTT_TOPIC_BASE "/cmd/light/schedule/enabled/set";
const char* TOPIC_CMD_LIGHT_SCHEDULE_START_HOUR_SET = MQTT_TOPIC_BASE "/cmd/light/schedule/start_hour/set";
const char* TOPIC_CMD_LIGHT_SCHEDULE_END_HOUR_SET = MQTT_TOPIC_BASE "/cmd/light/schedule/end_hour/set";
const char* TOPIC_CMD_LIGHT_SCHEDULE_BRIGHTNESS_SET = MQTT_TOPIC_BASE "/cmd/light/schedule/brightness/set";
const char* TOPIC_CMD_FAN_SET                     = MQTT_TOPIC_BASE "/cmd/fan/set";
const char* TOPIC_CMD_FAN_PERCENTAGE_SET          = MQTT_TOPIC_BASE "/cmd/fan/percentage/set";
const char* TOPIC_CMD_FAN_GEAR_SET                = MQTT_TOPIC_BASE "/cmd/fan/gear/set";
const char* TOPIC_CMD_FAN_OSCILLATION_SET         = MQTT_TOPIC_BASE "/cmd/fan/oscillation/set";
const char* TOPIC_CMD_BLOWER_SET                  = MQTT_TOPIC_BASE "/cmd/blower/set";
const char* TOPIC_CMD_BLOWER_PERCENTAGE_SET       = MQTT_TOPIC_BASE "/cmd/blower/percentage/set";
const char* TOPIC_CMD_DIAGNOSTICS                 = MQTT_TOPIC_BASE "/cmd/diagnostics";
const char* TOPIC_CMD_STRATEGY_SET                = MQTT_TOPIC_BASE "/cmd/strategy/set";
const char* TOPIC_CMD_DISCOVERY_REPUBLISH         = MQTT_TOPIC_BASE "/cmd/discovery/republish";

const char* MQTT_COMMAND_TOPICS[] = {
  TOPIC_CMD_RAW,
  TOPIC_CMD_GET_DEV_STA,
  TOPIC_CMD_GET_CONFIG_FILE,
  TOPIC_CMD_LIGHT_SET,
  TOPIC_CMD_LIGHT_BRIGHTNESS_SET,
  TOPIC_CMD_LIGHT_MODE_SET,
  TOPIC_CMD_LIGHT_SCHEDULE_ENABLED_SET,
  TOPIC_CMD_LIGHT_SCHEDULE_START_HOUR_SET,
  TOPIC_CMD_LIGHT_SCHEDULE_END_HOUR_SET,
  TOPIC_CMD_LIGHT_SCHEDULE_BRIGHTNESS_SET,
  TOPIC_CMD_FAN_SET,
  TOPIC_CMD_FAN_PERCENTAGE_SET,
  TOPIC_CMD_FAN_GEAR_SET,
  TOPIC_CMD_FAN_OSCILLATION_SET,
  TOPIC_CMD_BLOWER_SET,
  TOPIC_CMD_BLOWER_PERCENTAGE_SET,
  TOPIC_CMD_DIAGNOSTICS,
  TOPIC_CMD_STRATEGY_SET,
  TOPIC_CMD_DISCOVERY_REPUBLISH
};

const size_t MQTT_COMMAND_TOPIC_COUNT = sizeof(MQTT_COMMAND_TOPICS) / sizeof(MQTT_COMMAND_TOPICS[0]);


// =====================================================
// GLOBAIS
// =====================================================
WiFiClient wifiClient;
PubSubClient mqttClient(wifiClient);

BLEAdvertisedDevice* foundDevice = nullptr;
BLEClient* pClient = nullptr;
BLERemoteCharacteristic* pNotifyChar = nullptr;
BLERemoteCharacteristic* pWriteChar  = nullptr;

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

bool forceInitialConfigDumpPending = false;
unsigned long forceInitialConfigDumpAtMs = 0;

bool discoveryPending = false;
String discoveryPendingReason = "";
int discoveryStep = 0;
const int DISCOVERY_TOTAL_STEPS = 15;

bool availabilityPublishPending = false;
bool availabilityValuePending = false;
bool statusPublishPending = false;
bool statePublishPending = false;
bool statePublishFull = false;
int statePublishStep = 0;
const int STATE_PUBLISH_TOTAL_STEPS = 20;
unsigned long lastMqttPublishStepAtMs = 0;
unsigned long mqttPublishCooldownUntilMs = 0;
int mqttConsecutiveFails = 0;

bool queuedStartPending = false;
unsigned long queuedStartDueMs = 0;
String queuedStartReason = "";

bool runtimeStateRefreshPending = false;
unsigned long runtimeStateRefreshDueMs = 0;
String runtimeStateRefreshReason = "";

// =====================================================
// RX notify queue
// =====================================================
static uint8_t rxNotifyQueue[RX_NOTIFY_QUEUE_SIZE];
volatile size_t rxNotifyHead = 0;
volatile size_t rxNotifyTail = 0;
volatile bool rxNotifyOverflow = false;
volatile unsigned long rxNotifyDroppedBytes = 0;
volatile unsigned long rxNotifyCallbacks = 0;
volatile unsigned long rxNotifyBytes = 0;
portMUX_TYPE rxNotifyMux = portMUX_INITIALIZER_UNLOCKED;

// =====================================================
// STATE
// =====================================================
struct GgsState {
  bool bleConnected = false;
  float temp = NAN;
  float humi = NAN;
  float vpd  = NAN;

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
GgsState publishedState;

// =====================================================
// OPERATION
// =====================================================
enum OperationState {
  OP_IDLE,
  OP_WAITING_CONFIG,
  OP_SETTING_CONFIG,
  OP_VERIFYING
};

enum ConfigWriteStrategy {
  WRITE_STRATEGY_CONFIG_FIELD_PRIMARY,
  WRITE_STRATEGY_CONFIG_FILE_ONLY
};

enum PendingWriteMethod {
  PENDING_WRITE_NONE,
  PENDING_WRITE_CONFIG_FIELD,
  PENDING_WRITE_CONFIG_FILE
};

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
size_t configCacheJsonSize = 0;
uint16_t configCacheCrc16 = 0;
uint16_t pendingConfigCrc16 = 0;
uint16_t confirmedConfigCrc16 = 0;

PendingWriteMethod pendingWriteMethod = PENDING_WRITE_NONE;
ConfigWriteStrategy activeWriteStrategy = WRITE_STRATEGY_CONFIG_FIELD_PRIMARY;

bool configFieldSupported = true;
bool configFieldSupportKnown = false;
int configFieldFailureCount = 0;
const int CONFIG_FIELD_DISABLE_AFTER_FAILURES = 3;

bool usedFallbackForActiveTarget = false;

bool bridgeAvailabilityPublishPending = false;
bool bridgeAvailabilityValuePending = false;
bool bleAvailabilityPublishPending = false;
bool bleAvailabilityValuePending = false;

bool diagnosticsPublishPending = false;
unsigned long lastDiagnosticsPublishAtMs = 0;
const unsigned long DIAGNOSTICS_INTERVAL_MS = 60000;

String lastError = "";

// =====================================================
// PARSERS
// =====================================================
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

// =====================================================
// PROTOTYPES
// =====================================================
void processValidatedJson(const String& payload);
void requestStatusPublish(const String& reason);
void requestStatePublish(bool full, const String& reason);
void publishAllState();
void publishStateIfChanged();
void mqttPubDebug(const char* subtopic, const String& value);
bool requestGetDevSta();
bool requestGetConfigFile();
void requestRuntimeStateRefresh(const String& reason, unsigned long delayMs = 250);
void runtimeStateRefreshTick();
void scheduleStartQueued(const String& reason);
void setOperationState(OperationState st, const String& reason);
bool applyDeviceTarget(const String& device, TargetKind kind, int on, int level);
bool targetAlreadySatisfied(const DeviceTarget& t);
void verifyOperation();
void clearTargetQueue();
void parserReset(const char* reason);
void drainNotifyQueueToRxStream();
void processRxStreamInLoop();
bool notifyQueueHasData();
bool bleRxBusy();
bool mqttCanDoStepNow();
void requestDiscoveryPublish(const String& reason);
void startScan();
void cleanupBleClient(const char* reason);
void cleanupFoundDevice();
void setupTaskWatchdog();
void kickTaskWatchdog();
void safeDelayMs(unsigned long ms);
void resetBleRxPipeline(const char* reason);
void forceBleReconnect(const char* reason);
void reinitBleStack(const char* reason);
void supervisorTick();
bool safeForSystemRecovery();
void recoverWifiStack(const char* reason);
void restartEsp32Now(const String& reason);

const char* pendingWriteMethodName(PendingWriteMethod method);
const char* configWriteStrategyName(ConfigWriteStrategy strategy);
uint16_t crc16Modbus(const uint8_t* data, size_t len);
String crc16Hex(uint16_t v);
String jsonEscape(const String& s);
void computeConfigCacheMetadata();
void updatePendingConfigCrcFromCache();
void setLastError(const String& reason);
void clearLastError();
void requestBridgeAvailabilityPublish(bool online);
void requestBleAvailabilityPublish(bool online);
void requestDiagnosticsPublish(const String& reason);
void publishDiagnosticsJsonNow();
void diagnosticsPublishTick();
void publishCommandStatusRaw(const String& payload);
void publishCommandQueued(const DeviceTarget& t);
void publishCommandRunning(const String& phase, const DeviceTarget& t);
void publishCommandSent(const String& phase, const String& msgId, size_t payloadLen);
void publishCommandFallback(const String& reason);
void publishCommandSuccess(const DeviceTarget& t, bool verifiedConfig, bool verifiedDev);
void publishCommandFailed(const DeviceTarget& t, const String& reason);
bool parseWriteStrategyPayload(const String& msg, ConfigWriteStrategy& out);
void markConfigFieldFailure(const String& reason, bool disableImmediately);
void noteConfigFieldSuccess();
bool patchConfigForTarget(DeviceTarget& t);
bool patchLightConfig(JsonObject light, DeviceTarget& t);
bool patchFanConfig(JsonObject fan, DeviceTarget& t);
bool patchBlowerConfig(JsonObject blower, DeviceTarget& t);
String buildSetConfigFieldJson(const DeviceTarget& t);
String buildSetConfigFileJson();
bool sendSetConfigFieldFromCache(const DeviceTarget& t);
bool sendSetConfigFileFromCache();
bool fallbackToSetConfigFile(const String& reason);
void finishOperationSuccess(const String& reason);
void failOperation(const String& reason);
void handleConfigWriteAck(JsonDocument& doc, PendingWriteMethod method);
void handleSetConfigFileAck(JsonDocument& doc);
void handleSetConfigFieldAck(JsonDocument& doc);

// =====================================================
// HELPERS
// =====================================================
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

bool floatChanged(float a, float b, float eps = 0.0001f) {
  if (isnan(a) && isnan(b)) return false;
  if (isnan(a) != isnan(b)) return true;
  return fabs(a - b) > eps;
}

String boolJson(bool v) { return v ? "true" : "false"; }

String nextMsgId() {
  msgCounter++;
  return String(millis()) + "_" + String(msgCounter);
}

bool isLightConfigOnlyKind(TargetKind kind) {
  return kind == TARGET_KIND_LIGHT_MODE ||
         kind == TARGET_KIND_LIGHT_SCHEDULE_ENABLED ||
         kind == TARGET_KIND_LIGHT_SCHEDULE_START_HOUR ||
         kind == TARGET_KIND_LIGHT_SCHEDULE_END_HOUR ||
         kind == TARGET_KIND_LIGHT_SCHEDULE_BRIGHTNESS;
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

const char* pendingWriteMethodName(PendingWriteMethod method) {
  switch (method) {
    case PENDING_WRITE_NONE: return "none";
    case PENDING_WRITE_CONFIG_FIELD: return "setConfigField";
    case PENDING_WRITE_CONFIG_FILE: return "setConfigFile";
    default: return "unknown";
  }
}

const char* configWriteStrategyName(ConfigWriteStrategy strategy) {
  switch (strategy) {
    case WRITE_STRATEGY_CONFIG_FIELD_PRIMARY: return "config_field_primary";
    case WRITE_STRATEGY_CONFIG_FILE_ONLY: return "config_file_only";
    default: return "unknown";
  }
}

String crc16Hex(uint16_t v) {
  char buf[8];
  snprintf(buf, sizeof(buf), "0x%04X", v);
  return String(buf);
}

String jsonEscape(const String& s) {
  String out;
  out.reserve(s.length() + 8);

  for (size_t i = 0; i < s.length(); i++) {
    char c = s[i];

    if (c == '\\') {
      out += "\\\\";
    } else if (c == '"') {
      out += "\\\"";
    } else if (c == '\n') {
      out += "\\n";
    } else if (c == '\r') {
      out += "\\r";
    } else if (c == '\t') {
      out += "\\t";
    } else if ((uint8_t)c < 32) {
      out += " ";
    } else {
      out += c;
    }
  }

  return out;
}

void computeConfigCacheMetadata() {
  String compact;
  serializeJson(cachedConfigDoc, compact);

  configCacheJsonSize = compact.length();
  configCacheCrc16 = crc16Modbus((const uint8_t*)compact.c_str(), compact.length());
}

void updatePendingConfigCrcFromCache() {
  String compact;
  serializeJson(cachedConfigDoc, compact);

  pendingConfigCrc16 = crc16Modbus((const uint8_t*)compact.c_str(), compact.length());
}


String truncateForMqtt(const String& s, size_t maxLen = MQTT_DEBUG_MAX_LEN) {
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

String extractMethodPreview(const String& json) {
  int p = json.indexOf("\"method\"");
  if (p < 0) return "method=?";
  int colon = json.indexOf(':', p);
  if (colon < 0) return "method=?";
  int q1 = json.indexOf('"', colon + 1);
  if (q1 < 0) return "method=?";
  int q2 = json.indexOf('"', q1 + 1);
  if (q2 < 0) return "method=?";
  return "method=" + json.substring(q1 + 1, q2);
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
  publishedState = GgsState();
}


// =====================================================
// TASK WATCHDOG / SAFE DELAY
// =====================================================
void setupTaskWatchdog() {
#if defined(ESP_ARDUINO_VERSION_MAJOR) && ESP_ARDUINO_VERSION_MAJOR >= 3
  esp_task_wdt_config_t wdtConfig = {};
  wdtConfig.timeout_ms = TASK_WDT_TIMEOUT_MS;
  wdtConfig.idle_core_mask = (1 << portNUM_PROCESSORS) - 1;
  wdtConfig.trigger_panic = true;

  esp_err_t err = esp_task_wdt_init(&wdtConfig);
#else
  esp_err_t err = esp_task_wdt_init(TASK_WDT_TIMEOUT_MS / 1000, true);
#endif

  if (err == ESP_OK || err == ESP_ERR_INVALID_STATE) {
    esp_err_t addErr = esp_task_wdt_add(NULL);

    if (addErr == ESP_OK || addErr == ESP_ERR_INVALID_STATE) {
      taskWdtEnabled = true;
      Serial.println("[WDT] Task watchdog enabled");
    } else {
      Serial.println("[WDT] Failed to add current task err=" + String(addErr));
    }
  } else {
    Serial.println("[WDT] Failed to init watchdog err=" + String(err));
  }
}

void kickTaskWatchdog() {
  if (taskWdtEnabled) {
    esp_task_wdt_reset();
  }
}

void safeDelayMs(unsigned long ms) {
  unsigned long start = millis();

  while (millis() - start < ms) {
    kickTaskWatchdog();
    delay(50);
    yield();
  }
}

// =====================================================
// CRC16 / endian
// =====================================================
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

uint16_t readU16BE(const uint8_t* p) {
  return ((uint16_t)p[0] << 8) | p[1];
}

uint32_t readU32BE(const uint8_t* p) {
  return ((uint32_t)p[0] << 24) |
         ((uint32_t)p[1] << 16) |
         ((uint32_t)p[2] << 8) |
         p[3];
}

// =====================================================
// RX/MQTT priority helpers
// =====================================================
size_t rxNotifyNextIndex(size_t idx) { return (idx + 1) % RX_NOTIFY_QUEUE_SIZE; }

size_t rxNotifyAvailable() {
  size_t head, tail;
  portENTER_CRITICAL(&rxNotifyMux);
  head = rxNotifyHead;
  tail = rxNotifyTail;
  portEXIT_CRITICAL(&rxNotifyMux);
  if (head >= tail) return head - tail;
  return RX_NOTIFY_QUEUE_SIZE - tail + head;
}

bool notifyQueueHasData() { return rxNotifyAvailable() > 0; }

bool bleRxBusy() {
  return notifyQueueHasData() || !rxStream.empty() || aaaaRx.active || jsonParser.collecting;
}

bool mqttQuietEnoughForWork() {
  return mqttClient.connected() &&
         !bootstrapBleActive &&
         initialBleBootstrapDone &&
         opState == OP_IDLE &&
         !bleTxInProgress &&
         !parserProcessing &&
         !bleRxBusy() &&
         !activeTargetValid &&
         targetQueueCount == 0 &&
         !queuedStartPending;
}

bool mqttCanDoStepNow() {
  unsigned long now = millis();
  if (now < mqttPublishCooldownUntilMs) return false;
  if (now - lastMqttPublishStepAtMs < MQTT_STEP_INTERVAL_MS) return false;
  return mqttQuietEnoughForWork();
}

bool mqttConnectAllowedNow() {
  if (WiFi.status() != WL_CONNECTED) return false;
  if (bootstrapBleActive) return false;
  if (opState != OP_IDLE || bleTxInProgress || parserProcessing || bleRxBusy()) return false;
  if (initialBleBootstrapDone) return true;
  if (!connected && !doConnect && millis() - bootStartedAtMs > MQTT_OFFLINE_FALLBACK_MS) return true;
  return false;
}

void noteMqttPublishResult(bool ok) {
  lastMqttPublishStepAtMs = millis();
  if (ok) {
    mqttConsecutiveFails = 0;
  } else {
    mqttConsecutiveFails++;
    mqttPublishCooldownUntilMs = millis() + MQTT_FAIL_COOLDOWN_MS;
    Serial.print("[MQTT BACKOFF] fail_count=");
    Serial.println(mqttConsecutiveFails);
  }
}

// =====================================================
// MQTT helpers
// =====================================================
String getDeviceJson() {
  String s = "{";
  s += "\"identifiers\":[\"" + String(DEVICE_ID) + "\"],";
  s += "\"name\":\"" + String(DEVICE_NAME) + "\",";
  s += "\"manufacturer\":\"" + String(DEVICE_MANUFACTURER) + "\",";
  s += "\"model\":\"" + String(DEVICE_MODEL) + "\",";
  s += "\"sw_version\":\"" + String(FW_VERSION) + "\"";
  s += "}";
  return s;
}

bool mqttPub(const char* topic, const String& value, bool retained = true) {
  if (!mqttClient.connected()) return false;
  bool ok = mqttClient.publish(topic, value.c_str(), retained);
  Serial.print("[MQTT PUB] ");
  Serial.print(topic);
  Serial.print(" = ");
  Serial.print(truncateForMqtt(value, 180));
  Serial.print(" retain=");
  Serial.print(retained ? "true" : "false");
  Serial.print(" -> ");
  Serial.println(ok ? "OK" : "FAIL");
  noteMqttPublishResult(ok);
  return ok;
}

void mqttPubDebug(const char* subtopic, const String& value) {
  if (!MQTT_DEBUG_ENABLED) return;

  String serialMsg = String("[MQTT DEBUG] ") + subtopic + " = " + truncateForMqtt(value, 220);

  if (!mqttClient.connected()) {
    Serial.println(serialMsg + " [not_connected]");
    return;
  }

  if ((bleTxInProgress && MQTT_DEBUG_DROP_DURING_BLE_TX) ||
      (MQTT_DEBUG_DROP_WHEN_BLE_BUSY && (parserProcessing || bleRxBusy() || bootstrapBleActive))) {
    Serial.println(serialMsg + " [mqtt_suppressed_ble_busy]");
    return;
  }

  if (millis() < mqttPublishCooldownUntilMs) {
    Serial.println(serialMsg + " [mqtt_suppressed_backoff]");
    return;
  }

  String topic = String(TOPIC_BASE) + "/debug/" + subtopic;
  mqttPub(topic.c_str(), truncateForMqtt(value), false);
}

void requestBridgeAvailabilityPublish(bool online) {
  bridgeAvailabilityValuePending = online;
  bridgeAvailabilityPublishPending = true;
}

void requestBleAvailabilityPublish(bool online) {
  bleAvailabilityValuePending = online;
  bleAvailabilityPublishPending = true;
}

void requestAvailabilityPublish(bool online) {
  requestBleAvailabilityPublish(online);
}

void publishAvailability(bool online) {
  requestBleAvailabilityPublish(online);
}

void setLastError(const String& reason) {
  lastError = reason;

  if (mqttClient.connected()) {
    mqttPub(TOPIC_LAST_ERROR, reason, true);
  }
}

void clearLastError() {
  lastError = "";

  if (mqttClient.connected()) {
    mqttPub(TOPIC_LAST_ERROR, "", true);
  }
}

void requestDiagnosticsPublish(const String& reason) {
  diagnosticsPublishPending = true;
  Serial.println("[DIAGNOSTICS] scheduled reason=" + reason);
}

void requestStatusPublish(const String& reason) {
  statusPublishPending = true;
  Serial.print("[STATUS] scheduled reason=");
  Serial.println(reason);
}

void publishStatusJson() {
  requestStatusPublish("legacy_call");
}

void publishStatusJsonNow() {
  String payload = "{";
  payload += "\"wifi\":\"" + String(WiFi.status() == WL_CONNECTED ? "connected" : "disconnected") + "\",";
  payload += "\"mqtt\":\"" + String(mqttClient.connected() ? "connected" : "disconnected") + "\",";
  payload += "\"ble\":\"" + String(connected ? "connected" : "disconnected") + "\",";
  payload += "\"op_state\":\"" + String(opStateName(opState)) + "\",";
  payload += "\"pending_write_method\":\"" + String(pendingWriteMethodName(pendingWriteMethod)) + "\",";
  payload += "\"config_write_strategy\":\"" + String(configWriteStrategyName(activeWriteStrategy)) + "\",";
  payload += "\"config_field_supported\":" + boolJson(configFieldSupported) + ",";
  payload += "\"config_field_support_known\":" + boolJson(configFieldSupportKnown) + ",";
  payload += "\"config_field_failure_count\":" + String(configFieldFailureCount) + ",";
  payload += "\"bootstrap_ble_active\":" + boolJson(bootstrapBleActive) + ",";
  payload += "\"initial_ble_bootstrap_done\":" + boolJson(initialBleBootstrapDone) + ",";
  payload += "\"initial_config_received\":" + boolJson(initialConfigReceived) + ",";
  payload += "\"config_cache_valid\":" + boolJson(configCacheValid) + ",";
  payload += "\"config_cache_age_ms\":" + String(configCacheValid ? (long)(millis() - configCacheAtMs) : -1) + ",";
  payload += "\"config_json_size\":" + String(configCacheJsonSize) + ",";
  payload += "\"config_crc16\":\"" + crc16Hex(configCacheCrc16) + "\",";
  payload += "\"pending_config_crc16\":\"" + crc16Hex(pendingConfigCrc16) + "\",";
  payload += "\"confirmed_config_crc16\":\"" + crc16Hex(confirmedConfigCrc16) + "\",";
  payload += "\"dev_state_valid\":" + boolJson(currentState.devStateValid) + ",";
  payload += "\"config_state_valid\":" + boolJson(currentState.configStateValid) + ",";
  payload += "\"target_queue_count\":" + String(targetQueueCount) + ",";
  payload += "\"active_target\":" + boolJson(activeTargetValid) + ",";
  payload += "\"queued_start_pending\":" + boolJson(queuedStartPending) + ",";
  payload += "\"discovery_pending\":" + boolJson(discoveryPending) + ",";
  payload += "\"state_publish_pending\":" + boolJson(statePublishPending) + ",";
  payload += "\"ble_tx_in_progress\":" + boolJson(bleTxInProgress) + ",";
  payload += "\"ble_tx_age_ms\":" + String(bleTxInProgress ? (long)(millis() - bleTxStartedAtMs) : 0) + ",";
  payload += "\"rx_notify_callbacks\":" + String(rxNotifyCallbacks) + ",";
  payload += "\"rx_notify_bytes\":" + String(rxNotifyBytes) + ",";
  payload += "\"rx_notify_pending\":" + String(rxNotifyAvailable()) + ",";
  payload += "\"rx_notify_dropped_bytes\":" + String(rxNotifyDroppedBytes) + ",";
  payload += "\"rx_stream_size\":" + String(rxStream.size()) + ",";
  payload += "\"aaaa_active\":" + boolJson(aaaaRx.active) + ",";
  payload += "\"free_heap\":" + String(ESP.getFreeHeap()) + ",";
  payload += "\"min_free_heap\":" + String(ESP.getMinFreeHeap()) + ",";
  payload += "\"last_error\":" + String(lastError.length() ? (String("\"") + jsonEscape(lastError) + "\"") : String("null")) + ",";
  payload += "\"last_dev_rx_age_ms\":" + String(currentState.lastStateRxMs == 0 ? -1 : (long)(millis() - currentState.lastStateRxMs)) + ",";
  payload += "\"last_config_rx_age_ms\":" + String(currentState.lastConfigRxMs == 0 ? -1 : (long)(millis() - currentState.lastConfigRxMs));
  payload += "}";

  mqttPub(TOPIC_STATUS_JSON, payload, true);
}

void publishDiagnosticsJsonNow() {
  String payload = "{";
  payload += "\"fw_version\":\"" + String(FW_VERSION) + "\",";
  payload += "\"wifi_connected\":" + boolJson(WiFi.status() == WL_CONNECTED) + ",";
  payload += "\"mqtt_connected\":" + boolJson(mqttClient.connected()) + ",";
  payload += "\"ble_connected\":" + boolJson(connected) + ",";
  payload += "\"bridge_uptime_ms\":" + String(millis()) + ",";
  payload += "\"op_state\":\"" + String(opStateName(opState)) + "\",";
  payload += "\"pending_write_method\":\"" + String(pendingWriteMethodName(pendingWriteMethod)) + "\",";
  payload += "\"config_write_strategy\":\"" + String(configWriteStrategyName(activeWriteStrategy)) + "\",";
  payload += "\"config_field_supported\":" + boolJson(configFieldSupported) + ",";
  payload += "\"config_field_support_known\":" + boolJson(configFieldSupportKnown) + ",";
  payload += "\"config_field_failure_count\":" + String(configFieldFailureCount) + ",";
  payload += "\"config_cache_valid\":" + boolJson(configCacheValid) + ",";
  payload += "\"config_cache_age_ms\":" + String(configCacheValid ? (long)(millis() - configCacheAtMs) : -1) + ",";
  payload += "\"config_json_size\":" + String(configCacheJsonSize) + ",";
  payload += "\"config_crc16\":\"" + crc16Hex(configCacheCrc16) + "\",";
  payload += "\"pending_config_crc16\":\"" + crc16Hex(pendingConfigCrc16) + "\",";
  payload += "\"confirmed_config_crc16\":\"" + crc16Hex(confirmedConfigCrc16) + "\",";
  payload += "\"rx_notify_callbacks\":" + String(rxNotifyCallbacks) + ",";
  payload += "\"rx_notify_bytes\":" + String(rxNotifyBytes) + ",";
  payload += "\"rx_notify_dropped_bytes\":" + String(rxNotifyDroppedBytes) + ",";
  payload += "\"rx_stream_size\":" + String(rxStream.size()) + ",";
  payload += "\"aaaa_active\":" + boolJson(aaaaRx.active) + ",";
  payload += "\"target_queue_count\":" + String(targetQueueCount) + ",";
  payload += "\"free_heap\":" + String(ESP.getFreeHeap()) + ",";
  payload += "\"min_free_heap\":" + String(ESP.getMinFreeHeap()) + ",";
  payload += "\"last_error\":" + String(lastError.length() ? (String("\"") + jsonEscape(lastError) + "\"") : String("null"));
  payload += "}";

  mqttPub(TOPIC_DIAGNOSTICS_JSON, payload, true);
  lastDiagnosticsPublishAtMs = millis();
}

void publishCommandStatusRaw(const String& payload) {
  mqttPub(TOPIC_COMMAND_STATUS, payload, true);
}

void publishCommandQueued(const DeviceTarget& t) {
  String p = "{";
  p += "\"state\":\"queued\",";
  p += "\"device\":\"" + jsonEscape(t.device) + "\",";
  p += "\"kind\":\"" + String(targetKindName(t.kind)) + "\",";
  p += "\"on\":" + String(t.on) + ",";
  p += "\"level\":" + String(t.level) + ",";
  p += "\"strategy\":\"" + String(configWriteStrategyName(activeWriteStrategy)) + "\",";
  p += "\"queue_count\":" + String(targetQueueCount);
  p += "}";
  publishCommandStatusRaw(p);
}

void publishCommandRunning(const String& phase, const DeviceTarget& t) {
  String p = "{";
  p += "\"state\":\"running\",";
  p += "\"phase\":\"" + jsonEscape(phase) + "\",";
  p += "\"device\":\"" + jsonEscape(t.device) + "\",";
  p += "\"kind\":\"" + String(targetKindName(t.kind)) + "\"";
  p += "}";
  publishCommandStatusRaw(p);
}

void publishCommandSent(const String& phase, const String& msgId, size_t payloadLen) {
  String p = "{";
  p += "\"state\":\"running\",";
  p += "\"phase\":\"" + jsonEscape(phase) + "\",";
  p += "\"msg_id\":\"" + jsonEscape(msgId) + "\",";
  p += "\"payload_len\":" + String(payloadLen);
  p += "}";
  publishCommandStatusRaw(p);
}

void publishCommandFallback(const String& reason) {
  String p = "{";
  p += "\"state\":\"running\",";
  p += "\"phase\":\"fallback_setConfigFile\",";
  p += "\"reason\":\"" + jsonEscape(reason) + "\"";
  p += "}";
  publishCommandStatusRaw(p);
}

void publishCommandSuccess(const DeviceTarget& t, bool verifiedConfig, bool verifiedDev) {
  String p = "{";
  p += "\"state\":\"success\",";
  p += "\"device\":\"" + jsonEscape(t.device) + "\",";
  p += "\"kind\":\"" + String(targetKindName(t.kind)) + "\",";
  p += "\"verified_config\":" + boolJson(verifiedConfig) + ",";
  p += "\"verified_dev\":" + boolJson(verifiedDev) + ",";
  p += "\"used_fallback\":" + boolJson(usedFallbackForActiveTarget);
  p += "}";
  publishCommandStatusRaw(p);
}

void publishCommandFailed(const DeviceTarget& t, const String& reason) {
  String p = "{";
  p += "\"state\":\"failed\",";
  p += "\"device\":\"" + jsonEscape(t.device) + "\",";
  p += "\"kind\":\"" + String(targetKindName(t.kind)) + "\",";
  p += "\"reason\":\"" + jsonEscape(reason) + "\",";
  p += "\"used_fallback\":" + boolJson(usedFallbackForActiveTarget);
  p += "}";
  publishCommandStatusRaw(p);
}

void markConfigFieldFailure(const String& reason, bool disableImmediately) {
  configFieldFailureCount++;
  configFieldSupportKnown = true;

  Serial.println("[CONFIG FIELD] failure reason=" + reason +
                 " count=" + String(configFieldFailureCount));

  mqttPubDebug("config_field", "failure reason=" + reason +
               " count=" + String(configFieldFailureCount));

  if (disableImmediately || configFieldFailureCount >= CONFIG_FIELD_DISABLE_AFTER_FAILURES) {
    configFieldSupported = false;
    Serial.println("[CONFIG FIELD] temporarily disabled");
    mqttPubDebug("config_field", "temporarily_disabled");
  }

  requestDiagnosticsPublish("config_field_failure");
}

void noteConfigFieldSuccess() {
  configFieldSupported = true;
  configFieldSupportKnown = true;
  configFieldFailureCount = 0;
  requestDiagnosticsPublish("config_field_success");
}


void setOperationState(OperationState st, const String& reason) {
  bool changed = (opState != st);
  opState = st;
  opStartedAtMs = millis();

  String msg = String(opStateName(opState)) + " reason=" + reason;
  Serial.print(changed ? "[OP STATE] " : "[OP STATE REFRESH] ");
  Serial.println(msg);

  mqttPubDebug("op_state", msg);
  requestStatusPublish("op_state");
}

// =====================================================
// MQTT payload helpers
// =====================================================
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

String normalizePayloadText(const String& msg) {
  String m = msg;
  m.trim();
  m.replace("\"", "");
  m.replace("'", "");
  m.toLowerCase();
  return m;
}

bool parseWriteStrategyPayload(const String& msg, ConfigWriteStrategy& out) {
  String m = normalizePayloadText(msg);

  if (m == "config_field_primary" || m == "setconfigfield" || m == "field") {
    out = WRITE_STRATEGY_CONFIG_FIELD_PRIMARY;
    return true;
  }

  if (m == "config_file_only" || m == "setconfigfile" || m == "file") {
    out = WRITE_STRATEGY_CONFIG_FILE_ONLY;
    return true;
  }

  return false;
}


bool mqttPayloadMeansOn(const String& msg) {
  String m = msg;
  m.trim();

  String u = m;
  u.toUpperCase();

  String l = m;
  l.toLowerCase();

  return u == "ON" ||
         l == "1" ||
         l == "true" ||
         m.indexOf("\"state\":\"ON\"") >= 0 ||
         m.indexOf("\"state\": \"ON\"") >= 0 ||
         l.indexOf("\"state\":\"true\"") >= 0 ||
         l.indexOf("\"state\": \"true\"") >= 0;
}

bool mqttPayloadMeansOff(const String& msg) {
  String m = msg;
  m.trim();

  String u = m;
  u.toUpperCase();

  String l = m;
  l.toLowerCase();

  return u == "OFF" ||
         l == "0" ||
         l == "false" ||
         m.indexOf("\"state\":\"OFF\"") >= 0 ||
         m.indexOf("\"state\": \"OFF\"") >= 0 ||
         l.indexOf("\"state\":\"false\"") >= 0 ||
         l.indexOf("\"state\": \"false\"") >= 0;
}

bool parseLightModePayload(const String& msg, int& outMode) {
  String m = normalizePayloadText(msg);

  if (m == "manual" || m == "0" || m.indexOf("manual") >= 0) {
    outMode = 0;
    return true;
  }

  if (m == "time_slot" ||
      m == "timeslot" ||
      m == "auto" ||
      m == "automatic" ||
      m == "1" ||
      m.indexOf("time_slot") >= 0 ||
      m.indexOf("timeslot") >= 0 ||
      m.indexOf("automatic") >= 0 ||
      m.indexOf("auto") >= 0) {
    outMode = 1;
    return true;
  }

  return false;
}

bool isKnownMqttCommandTopic(const String& topic) {
  for (size_t i = 0; i < MQTT_COMMAND_TOPIC_COUNT; i++) {
    if (topic == MQTT_COMMAND_TOPICS[i]) return true;
  }
  return false;
}

// =====================================================
// HOME ASSISTANT DISCOVERY - incremental
// =====================================================
String availabilityJsonFragment() {
  String p = "";
  p += "\"availability\":[";
  p += "{";
  p += "\"topic\":\"" MQTT_TOPIC_BASE "/bridge/availability\",";
  p += "\"payload_available\":\"online\",";
  p += "\"payload_not_available\":\"offline\"";
  p += "},";
  p += "{";
  p += "\"topic\":\"" MQTT_TOPIC_BASE "/ble/availability\",";
  p += "\"payload_available\":\"online\",";
  p += "\"payload_not_available\":\"offline\"";
  p += "}";
  p += "],";
  p += "\"availability_mode\":\"all\"";
  return p;
}


String commandEntityOptionsJsonFragment() {
  return "\"qos\":0,\"retain\":false,\"optimistic\":false";
}

bool publishDiscoveryPayload(const String& component, const String& objectId, const String& payload) {
  String topic = String(DISCOVERY_PREFIX) + "/" + component + "/" + DEVICE_ID + "/" + objectId + "/config";
  return mqttPub(topic.c_str(), payload, true);
}

void publishDiscoverySensor(
  const char* objectId,
  const char* name,
  const char* entityId,
  const char* stateTopic,
  const char* deviceClass,
  const char* unit,
  const char* stateClass
) {
  String p = "{";
  p += "\"name\":\"" + String(name) + "\",";
  p += "\"unique_id\":\"" + String(DEVICE_ID) + "_" + objectId + "\",";
  p += "\"default_entity_id\":\"" + String(entityId) + "\",";
  p += "\"state_topic\":\"" + String(stateTopic) + "\",";
  p += availabilityJsonFragment() + ",";

  if (deviceClass && strlen(deviceClass)) {
    p += "\"device_class\":\"" + String(deviceClass) + "\",";
  }

  if (unit && strlen(unit)) {
    p += "\"unit_of_measurement\":\"" + String(unit) + "\",";
  }

  if (stateClass && strlen(stateClass)) {
    p += "\"state_class\":\"" + String(stateClass) + "\",";
  }

  p += "\"device\":" + getDeviceJson() + "}";
  publishDiscoveryPayload("sensor", objectId, p);
}

void publishDiscoveryNumber(
  const char* objectId,
  const char* name,
  const char* entityId,
  const char* stateTopic,
  const char* cmdTopic,
  int minVal,
  int maxVal,
  int step,
  const char* unit
) {
  String p = "{";
  p += "\"name\":\"" + String(name) + "\",";
  p += "\"unique_id\":\"" + String(DEVICE_ID) + "_" + objectId + "\",";
  p += "\"default_entity_id\":\"" + String(entityId) + "\",";
  p += "\"state_topic\":\"" + String(stateTopic) + "\",";
  p += "\"command_topic\":\"" + String(cmdTopic) + "\",";
  p += "\"min\":" + String(minVal) + ",";
  p += "\"max\":" + String(maxVal) + ",";
  p += "\"step\":" + String(step) + ",";
  p += "\"mode\":\"slider\",";

  if (unit && strlen(unit)) {
    p += "\"unit_of_measurement\":\"" + String(unit) + "\",";
  }

  p += availabilityJsonFragment() + ",";
  p += commandEntityOptionsJsonFragment() + ",";
  p += "\"device\":" + getDeviceJson() + "}";
  publishDiscoveryPayload("number", objectId, p);
}

void publishDiscoverySwitch(
  const char* objectId,
  const char* name,
  const char* entityId,
  const char* stateTopic,
  const char* cmdTopic
) {
  String p = "{";
  p += "\"name\":\"" + String(name) + "\",";
  p += "\"unique_id\":\"" + String(DEVICE_ID) + "_" + objectId + "\",";
  p += "\"default_entity_id\":\"" + String(entityId) + "\",";
  p += "\"state_topic\":\"" + String(stateTopic) + "\",";
  p += "\"command_topic\":\"" + String(cmdTopic) + "\",";
  p += "\"payload_on\":\"ON\",";
  p += "\"payload_off\":\"OFF\",";
  p += availabilityJsonFragment() + ",";
  p += commandEntityOptionsJsonFragment() + ",";
  p += "\"device\":" + getDeviceJson() + "}";
  publishDiscoveryPayload("switch", objectId, p);
}

void publishDiscoveryLight() {
  String p = "{";
  p += "\"name\":\"GGS Light\",";
  p += "\"unique_id\":\"" + String(DEVICE_ID) + "_light\",";
  p += "\"default_entity_id\":\"light.ggs_light\",";
  p += "\"state_topic\":\"" MQTT_TOPIC_BASE "/state/light/on\",";
  p += "\"state_value_template\":\"{% if value | int == 1 %}ON{% else %}OFF{% endif %}\",";
  p += "\"command_topic\":\"" MQTT_TOPIC_BASE "/cmd/light/set\",";
  p += "\"payload_on\":\"ON\",";
  p += "\"payload_off\":\"OFF\",";
  p += "\"brightness_state_topic\":\"" MQTT_TOPIC_BASE "/state/light/brightness\",";
  p += "\"brightness_command_topic\":\"" MQTT_TOPIC_BASE "/cmd/light/brightness/set\",";
  p += "\"brightness_scale\":255,";
  p += availabilityJsonFragment() + ",";
  p += commandEntityOptionsJsonFragment() + ",";
  p += "\"device\":" + getDeviceJson() + "}";
  publishDiscoveryPayload("light", "light", p);
}

void publishDiscoverySelectLightMode() {
  String p = "{";
  p += "\"name\":\"GGS Light Mode\",";
  p += "\"unique_id\":\"" + String(DEVICE_ID) + "_light_mode\",";
  p += "\"default_entity_id\":\"select.ggs_light_mode\",";
  p += "\"state_topic\":\"" MQTT_TOPIC_BASE "/state/light/mode\",";
  p += "\"command_topic\":\"" MQTT_TOPIC_BASE "/cmd/light/mode/set\",";
  p += "\"options\":[\"manual\",\"time_slot\"],";
  p += availabilityJsonFragment() + ",";
  p += commandEntityOptionsJsonFragment() + ",";
  p += "\"device\":" + getDeviceJson() + "}";
  publishDiscoveryPayload("select", "light_mode", p);
}

void publishDiscoveryFanLike(
  const char* objectId,
  const char* name,
  const char* entityId,
  const char* stateTopic,
  const char* cmdTopic,
  const char* pctStateTopic,
  const char* pctCmdTopic
) {
  String p = "{";
  p += "\"name\":\"" + String(name) + "\",";
  p += "\"unique_id\":\"" + String(DEVICE_ID) + "_" + objectId + "\",";
  p += "\"default_entity_id\":\"" + String(entityId) + "\",";
  p += "\"state_topic\":\"" + String(stateTopic) + "\",";
  p += "\"state_value_template\":\"{% if value | int == 1 %}ON{% else %}OFF{% endif %}\",";
  p += "\"command_topic\":\"" + String(cmdTopic) + "\",";
  p += "\"payload_on\":\"ON\",";
  p += "\"payload_off\":\"OFF\",";
  p += "\"percentage_state_topic\":\"" + String(pctStateTopic) + "\",";
  p += "\"percentage_value_template\":\"{{ value | int }}\",";
  p += "\"percentage_command_topic\":\"" + String(pctCmdTopic) + "\",";
  p += "\"speed_range_min\":1,";
  p += "\"speed_range_max\":100,";
  p += availabilityJsonFragment() + ",";
  p += commandEntityOptionsJsonFragment() + ",";
  p += "\"device\":" + getDeviceJson() + "}";
  publishDiscoveryPayload("fan", objectId, p);
}

void publishDiscoveryStep(int step) {
  Serial.print("[DISCOVERY] step=");
  Serial.print(step);
  Serial.print("/");
  Serial.println(DISCOVERY_TOTAL_STEPS);

  switch (step) {
    case 0:
      publishDiscoverySensor("temperature", "GGS Temperature", "sensor.ggs_temperature", MQTT_TOPIC_BASE "/state/sensor/temp", "temperature", "°C", "measurement");
      break;

    case 1:
      publishDiscoverySensor("humidity", "GGS Humidity", "sensor.ggs_humidity", MQTT_TOPIC_BASE "/state/sensor/humi", "humidity", "%", "measurement");
      break;

    case 2:
      publishDiscoverySensor("vpd", "GGS VPD", "sensor.ggs_vpd", MQTT_TOPIC_BASE "/state/sensor/vpd", "", "kPa", "measurement");
      break;

    case 3:
      publishDiscoveryLight();
      break;

    case 4:
      publishDiscoverySelectLightMode();
      break;

    case 5:
      publishDiscoverySwitch(
        "light_schedule_enabled",
        "GGS Light Schedule Enabled",
        "switch.ggs_light_schedule_enabled",
        MQTT_TOPIC_BASE "/state/light/schedule/enabled",
        MQTT_TOPIC_BASE "/cmd/light/schedule/enabled/set"
      );
      break;

    case 6:
      publishDiscoveryNumber(
        "light_schedule_start_hour",
        "GGS Light Schedule Start Hour",
        "number.ggs_light_schedule_start_hour",
        MQTT_TOPIC_BASE "/state/light/schedule/start_hour",
        MQTT_TOPIC_BASE "/cmd/light/schedule/start_hour/set",
        0,
        23,
        1,
        "h"
      );
      break;

    case 7:
      publishDiscoveryNumber(
        "light_schedule_end_hour",
        "GGS Light Schedule End Hour",
        "number.ggs_light_schedule_end_hour",
        MQTT_TOPIC_BASE "/state/light/schedule/end_hour",
        MQTT_TOPIC_BASE "/cmd/light/schedule/end_hour/set",
        0,
        23,
        1,
        "h"
      );
      break;

    case 8:
      publishDiscoveryNumber(
        "light_schedule_brightness",
        "GGS Light Schedule Brightness",
        "number.ggs_light_schedule_brightness",
        MQTT_TOPIC_BASE "/state/light/schedule/brightness",
        MQTT_TOPIC_BASE "/cmd/light/schedule/brightness/set",
        0,
        100,
        1,
        "%"
      );
      break;

    case 9:
      publishDiscoverySensor(
        "light_schedule_start_time",
        "GGS Light Schedule Start Time",
        "sensor.ggs_light_schedule_start_time",
        MQTT_TOPIC_BASE "/state/light/schedule/start_time",
        "",
        "",
        ""
      );
      break;

    case 10:
      publishDiscoverySensor(
        "light_schedule_end_time",
        "GGS Light Schedule End Time",
        "sensor.ggs_light_schedule_end_time",
        MQTT_TOPIC_BASE "/state/light/schedule/end_time",
        "",
        "",
        ""
      );
      break;

    case 11:
      publishDiscoveryFanLike(
        "fan",
        "GGS Fan",
        "fan.ggs_fan",
        MQTT_TOPIC_BASE "/state/fan/on",
        MQTT_TOPIC_BASE "/cmd/fan/set",
        MQTT_TOPIC_BASE "/state/fan/percentage",
        MQTT_TOPIC_BASE "/cmd/fan/percentage/set"
      );
      break;

    case 12:
      publishDiscoveryNumber(
        "fan_gear",
        "GGS Fan Gear",
        "number.ggs_fan_gear",
        MQTT_TOPIC_BASE "/state/fan/level",
        MQTT_TOPIC_BASE "/cmd/fan/gear/set",
        1,
        10,
        1,
        ""
      );
      break;

    case 13:
      publishDiscoveryNumber(
        "fan_oscillation",
        "GGS Fan Oscillation",
        "number.ggs_fan_oscillation",
        MQTT_TOPIC_BASE "/state/fan/oscillation",
        MQTT_TOPIC_BASE "/cmd/fan/oscillation/set",
        1,
        10,
        1,
        ""
      );
      break;

    case 14:
      publishDiscoveryFanLike(
        "blower",
        "GGS Blower",
        "fan.ggs_blower",
        MQTT_TOPIC_BASE "/state/blower/on",
        MQTT_TOPIC_BASE "/cmd/blower/set",
        MQTT_TOPIC_BASE "/state/blower/percentage",
        MQTT_TOPIC_BASE "/cmd/blower/percentage/set"
      );
      break;
  }
}

void requestDiscoveryPublish(const String& reason) {
  discoveryPending = true;
  discoveryPendingReason = reason;
  discoveryStep = 0;

  Serial.print("[DISCOVERY] scheduled_incremental reason=");
  Serial.println(reason);

  mqttPubDebug("discovery", "scheduled reason=" + reason);
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
    Serial.println("[DISCOVERY] complete");
    requestAvailabilityPublish(connected);
    requestStatePublish(true, "discovery_complete");
  }
}

// =====================================================
// STATE PUBLISH incremental
// =====================================================
void requestStatePublish(bool full, const String& reason) {
  if (full) statePublishFull = true;

  statePublishPending = true;

  if (statePublishStep < 0 || statePublishStep >= STATE_PUBLISH_TOTAL_STEPS) {
    statePublishStep = 0;
  }

  Serial.print("[STATE PUB] scheduled full=");
  Serial.print(statePublishFull ? "true" : "false");
  Serial.print(" reason=");
  Serial.println(reason);
}

void publishStateIfChanged() {
  requestStatePublish(false, "state_changed");
}

void publishAllState() {
  requestStatePublish(true, "all_state");
}

bool shouldPublishFloat(bool full, float current, float published) {
  if (isnan(current)) return false;
  return full || floatChanged(current, published);
}

bool shouldPublishInt(bool full, int current, int published, bool requireKnown = false) {
  if (requireKnown && current < 0) return false;
  return full || current != published;
}

void publishStateStep(int step) {
  switch (step) {
    case 0:
      if (shouldPublishFloat(statePublishFull, currentState.temp, publishedState.temp)) {
        mqttPub(MQTT_TOPIC_BASE "/state/sensor/temp", String(currentState.temp, 1), true);
        publishedState.temp = currentState.temp;
      }
      break;

    case 1:
      if (shouldPublishFloat(statePublishFull, currentState.humi, publishedState.humi)) {
        mqttPub(MQTT_TOPIC_BASE "/state/sensor/humi", String(currentState.humi, 1), true);
        publishedState.humi = currentState.humi;
      }
      break;

    case 2:
      if (shouldPublishFloat(statePublishFull, currentState.vpd, publishedState.vpd)) {
        mqttPub(MQTT_TOPIC_BASE "/state/sensor/vpd", String(currentState.vpd, 2), true);
        publishedState.vpd = currentState.vpd;
      }
      break;

    case 3:
      if (shouldPublishInt(statePublishFull, currentState.fanOn, publishedState.fanOn)) {
        mqttPub(MQTT_TOPIC_BASE "/state/fan/on", String(stateOrZero(currentState.fanOn)), true);
        publishedState.fanOn = currentState.fanOn;
      }
      break;

    case 4:
      if (shouldPublishInt(statePublishFull, currentState.fanLevel, publishedState.fanLevel)) {
        mqttPub(MQTT_TOPIC_BASE "/state/fan/level", String(stateOrZero(currentState.fanLevel)), true);
        publishedState.fanLevel = currentState.fanLevel;
      }
      break;

    case 5:
      if (shouldPublishInt(statePublishFull, currentState.fanPercentage, publishedState.fanPercentage)) {
        mqttPub(MQTT_TOPIC_BASE "/state/fan/percentage", String(stateOrZero(currentState.fanPercentage)), true);
        publishedState.fanPercentage = currentState.fanPercentage;
      }
      break;

    case 6:
      if (currentState.fanOscillation >= 0 &&
          shouldPublishInt(statePublishFull, currentState.fanOscillation, publishedState.fanOscillation, true)) {
        mqttPub(MQTT_TOPIC_BASE "/state/fan/oscillation", String(currentState.fanOscillation), true);
        publishedState.fanOscillation = currentState.fanOscillation;
      }
      break;

    case 7:
      if (shouldPublishInt(statePublishFull, currentState.blowerOn, publishedState.blowerOn)) {
        mqttPub(MQTT_TOPIC_BASE "/state/blower/on", String(stateOrZero(currentState.blowerOn)), true);
        publishedState.blowerOn = currentState.blowerOn;
      }
      break;

    case 8:
      if (shouldPublishInt(statePublishFull, currentState.blowerLevel, publishedState.blowerLevel)) {
        mqttPub(MQTT_TOPIC_BASE "/state/blower/level", String(stateOrZero(currentState.blowerLevel)), true);
        publishedState.blowerLevel = currentState.blowerLevel;
      }
      break;

    case 9:
      if (shouldPublishInt(statePublishFull, currentState.blowerPercentage, publishedState.blowerPercentage)) {
        mqttPub(MQTT_TOPIC_BASE "/state/blower/percentage", String(stateOrZero(currentState.blowerPercentage)), true);
        publishedState.blowerPercentage = currentState.blowerPercentage;
      }
      break;

    case 10:
      if (shouldPublishInt(statePublishFull, currentState.lightOn, publishedState.lightOn)) {
        mqttPub(MQTT_TOPIC_BASE "/state/light/on", String(stateOrZero(currentState.lightOn)), true);
        publishedState.lightOn = currentState.lightOn;
      }
      break;

    case 11:
      if (shouldPublishInt(statePublishFull, currentState.lightLevel, publishedState.lightLevel)) {
        mqttPub(MQTT_TOPIC_BASE "/state/light/level", String(stateOrZero(currentState.lightLevel)), true);
        publishedState.lightLevel = currentState.lightLevel;
      }
      break;

    case 12:
      if (shouldPublishInt(statePublishFull, currentState.lightBrightness, publishedState.lightBrightness)) {
        mqttPub(MQTT_TOPIC_BASE "/state/light/brightness", String(stateOrZero(currentState.lightBrightness)), true);
        publishedState.lightBrightness = currentState.lightBrightness;
      }
      break;

    case 13:
      if (currentState.lightModeType >= 0 &&
          shouldPublishInt(statePublishFull, currentState.lightModeType, publishedState.lightModeType, true)) {
        mqttPub(MQTT_TOPIC_BASE "/state/light/mode", currentState.lightModeType == 1 ? "time_slot" : "manual", true);
        publishedState.lightModeType = currentState.lightModeType;
        publishedState.lightLastAutoModeType = currentState.lightLastAutoModeType;
      }
      break;

    case 14:
      if (currentState.lightScheduleEnabled >= 0 &&
          shouldPublishInt(statePublishFull, currentState.lightScheduleEnabled, publishedState.lightScheduleEnabled, true)) {
        mqttPub(MQTT_TOPIC_BASE "/state/light/schedule/enabled", currentState.lightScheduleEnabled == 1 ? "ON" : "OFF", true);
        publishedState.lightScheduleEnabled = currentState.lightScheduleEnabled;
      }
      break;

    case 15:
      if (currentState.lightScheduleStartHour >= 0 &&
          shouldPublishInt(statePublishFull, currentState.lightScheduleStartHour, publishedState.lightScheduleStartHour, true)) {
        mqttPub(MQTT_TOPIC_BASE "/state/light/schedule/start_hour", String(currentState.lightScheduleStartHour), true);
        publishedState.lightScheduleStartHour = currentState.lightScheduleStartHour;
      }
      break;

    case 16:
      if (currentState.lightScheduleEndHour >= 0 &&
          shouldPublishInt(statePublishFull, currentState.lightScheduleEndHour, publishedState.lightScheduleEndHour, true)) {
        mqttPub(MQTT_TOPIC_BASE "/state/light/schedule/end_hour", String(currentState.lightScheduleEndHour), true);
        publishedState.lightScheduleEndHour = currentState.lightScheduleEndHour;
      }
      break;

    case 17:
      if (currentState.lightScheduleStartTime >= 0 &&
          shouldPublishInt(statePublishFull, currentState.lightScheduleStartTime, publishedState.lightScheduleStartTime, true)) {
        mqttPub(MQTT_TOPIC_BASE "/state/light/schedule/start_time", secondsToHHMM(currentState.lightScheduleStartTime), true);
        publishedState.lightScheduleStartTime = currentState.lightScheduleStartTime;
      }
      break;

    case 18:
      if (currentState.lightScheduleEndTime >= 0 &&
          shouldPublishInt(statePublishFull, currentState.lightScheduleEndTime, publishedState.lightScheduleEndTime, true)) {
        mqttPub(MQTT_TOPIC_BASE "/state/light/schedule/end_time", secondsToHHMM(currentState.lightScheduleEndTime), true);
        publishedState.lightScheduleEndTime = currentState.lightScheduleEndTime;
      }
      break;

    case 19:
      if (currentState.lightScheduleBrightness >= 0 &&
          shouldPublishInt(statePublishFull, currentState.lightScheduleBrightness, publishedState.lightScheduleBrightness, true)) {
        mqttPub(MQTT_TOPIC_BASE "/state/light/schedule/brightness", String(currentState.lightScheduleBrightness), true);
        publishedState.lightScheduleBrightness = currentState.lightScheduleBrightness;
      }

      publishedState.lightScheduleWeekmask = currentState.lightScheduleWeekmask;
      publishedState.devStateValid = currentState.devStateValid;
      publishedState.configStateValid = currentState.configStateValid;
      publishedState.bleConnected = currentState.bleConnected;
      break;
  }
}
void availabilityPublishTick() {
  if (!mqttCanDoStepNow()) return;

  if (bridgeAvailabilityPublishPending) {
    if (mqttPub(TOPIC_BRIDGE_AVAILABILITY, bridgeAvailabilityValuePending ? "online" : "offline", true)) {
      bridgeAvailabilityPublishPending = false;
      return;
    }
  }

  if (bleAvailabilityPublishPending) {
    bool online = bleAvailabilityValuePending;

    bool ok1 = mqttPub(TOPIC_BLE_AVAILABILITY, online ? "online" : "offline", true);
    bool ok2 = mqttPub(TOPIC_LEGACY_AVAILABILITY, online ? "online" : "offline", true);

    if (ok1 && ok2) {
      bleAvailabilityPublishPending = false;
    }
  }
}


void statusPublishTick() {
  if (!statusPublishPending || !mqttCanDoStepNow()) return;

  publishStatusJsonNow();
  statusPublishPending = false;
}

void diagnosticsPublishTick() {
  if (!mqttCanDoStepNow()) return;

  if (diagnosticsPublishPending) {
    publishDiagnosticsJsonNow();
    diagnosticsPublishPending = false;
    return;
  }

  if (millis() - lastDiagnosticsPublishAtMs > DIAGNOSTICS_INTERVAL_MS) {
    publishDiagnosticsJsonNow();
  }
}


void statePublishTick() {
  if (!statePublishPending || !mqttCanDoStepNow()) return;

  int beforeFails = mqttConsecutiveFails;
  publishStateStep(statePublishStep);

  if (mqttConsecutiveFails > beforeFails) return;

  statePublishStep++;

  if (statePublishStep >= STATE_PUBLISH_TOTAL_STEPS) {
    statePublishPending = false;
    statePublishFull = false;
    statePublishStep = 0;
    Serial.println("[STATE PUB] complete");
    requestStatusPublish("state_publish_complete");
  }
}

// =====================================================
// BLE TX
// =====================================================
// AA AA framing is used when a JSON command is too large for a raw BLE write.
// Notify callbacks only enqueue bytes; parsing and MQTT publishing happen from loop().
// This keeps BLE RX deterministic, watchdog-safe and less likely to starve the stack.
bool beginBleTx(const String& label) {
  if (bleTxInProgress) {
    mqttPubDebug("tx_busy", "current=" + bleTxLabel + " requested=" + label);
    return false;
  }

  if (!connected || pWriteChar == nullptr) {
    mqttPubDebug("tx_error", "not_connected_or_no_write_char label=" + label);
    return false;
  }

  bleTxInProgress = true;
  bleTxStartedAtMs = millis();
  bleTxLabel = label;
  return true;
}

void endBleTx(const String& reason) {
  Serial.print("[BLE TX] end label=");
  Serial.print(bleTxLabel);
  Serial.print(" reason=");
  Serial.print(reason);
  Serial.print(" age_ms=");
  Serial.println(millis() - bleTxStartedAtMs);

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

  if (pClient != nullptr && connected) {
    pClient->disconnect();
  }
}

void bleTxWatchdogTick() {
  if (bleTxInProgress && millis() - bleTxStartedAtMs > BLE_TX_STUCK_TIMEOUT_MS) {
    handleBleTxStuck("periodic_tick");
  }
}

std::vector<uint8_t> buildAaaaFrame(
  const uint8_t* payload,
  size_t payloadLen,
  uint16_t payloadCrc,
  uint32_t offset,
  uint16_t chunkLen
) {
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

  for (uint16_t i = 0; i < chunkLen; i++) {
    frame.push_back(payload[offset + i]);
  }

  appendU16BE(frame, crc16Modbus(frame.data(), frame.size()));
  return frame;
}

bool sendJsonFramedAAAA(const String& json) {
  const uint8_t* payload = (const uint8_t*)json.c_str();
  size_t payloadLen = json.length();

  if (payloadLen == 0 || payloadLen > MAX_AAAA_PAYLOAD_SIZE) {
    return false;
  }

  String method = extractMethodPreview(json);

  if (!beginBleTx("AAAA:" + method)) {
    return false;
  }

  uint16_t payloadCrc = crc16Modbus(payload, payloadLen);
  int chunks = (payloadLen + AAAA_CHUNK_SIZE - 1) / AAAA_CHUNK_SIZE;

  Serial.println("[AAAA TX] " + method + " payload_len=" + String(payloadLen) + " chunks=" + String(chunks) + " crc=0x" + String(payloadCrc, HEX));
  mqttPubDebug("tx_frame", method + " payload_len=" + String(payloadLen) + " chunks=" + String(chunks));

  for (uint32_t offset = 0; offset < payloadLen; offset += AAAA_CHUNK_SIZE) {
    size_t remaining = payloadLen - offset;
    uint16_t chunkLen = (uint16_t)((remaining < AAAA_CHUNK_SIZE) ? remaining : AAAA_CHUNK_SIZE);

    std::vector<uint8_t> frame = buildAaaaFrame(payload, payloadLen, payloadCrc, offset, chunkLen);

    Serial.println("[AAAA TX FRAME] offset=" + String(offset) +
                   " chunk_len=" + String(chunkLen) +
                   " frame_len=" + String(frame.size()) +
                   " hex=" + bytesToHexPreview(frame.data(), frame.size(), 48));

    bool ok = pWriteChar->writeValue(frame.data(), frame.size(), true);

    if (!ok) {
      endBleTx("aaaa_write_failed_offset_" + String(offset));
      return false;
    }

    delay(BLE_FRAME_DELAY_MS);
    yield();
  }

  endBleTx("aaaa_all_frames_sent");
  return true;
}

bool sendJsonAuto(const String& json) {
  if (json.length() == 0) {
    mqttPubDebug("tx_error", "empty_json_rejected");
    return false;
  }

  if (bleTxInProgress) {
    mqttPubDebug("tx_busy", "reject_sendJsonAuto_busy " + extractMethodPreview(json));
    return false;
  }

  if (!connected || pWriteChar == nullptr) {
    mqttPubDebug("tx_error", "not_connected_or_no_write_char");
    return false;
  }

  const uint8_t* payload = (const uint8_t*)json.c_str();
  size_t payloadLen = json.length();
  String method = extractMethodPreview(json);

  mqttPubDebug("tx_json", method + " len=" + String(payloadLen));
  Serial.println("[BLE TX JSON] " + truncateForMqtt(json, 900));

  if (payloadLen <= RAW_WRITE_LIMIT) {
    if (!beginBleTx("RAW:" + method)) {
      return false;
    }

    bool ok = pWriteChar->writeValue((uint8_t*)payload, payloadLen, true);

    Serial.println(String("[BLE WRITE RAW RESULT] ok=") + (ok ? "true" : "false"));

    endBleTx(ok ? "raw_write_ok" : "raw_write_failed");
    return ok;
  }

  return sendJsonFramedAAAA(json);
}

bool sendJsonCommand(const String& json) {
  return sendJsonAuto(json);
}

// =====================================================
// REQUESTS
// =====================================================
bool requestGetDevSta() {
  String msgId = nextMsgId();
  String json = "{\"method\":\"getDevSta\",\"params\":{},\"msgId\":\"" + msgId + "\"}";

  lastProbeAt = millis();

  Serial.println("[REQ] getDevSta");
  return sendJsonCommand(json);
}

bool requestGetConfigFile() {
  String msgId = nextMsgId();

  String json = "{";
  json += "\"method\":\"getConfigFile\",";
  json += "\"params\":{},";
  json += "\"msgId\":\"" + msgId + "\",";
  json += "\"pid\":\"" + String(GGS_PID) + "\",";
  json += "\"pcode\":" + String(GGS_PCODE) + ",";
  json += "\"uid\":\"" + String(GGS_UID) + "\"";
  json += "}";

  Serial.println("[REQ] getConfigFile");
  return sendJsonCommand(json);
}

void requestRuntimeStateRefresh(const String& reason, unsigned long delayMs) {
  runtimeStateRefreshPending = true;
  runtimeStateRefreshDueMs = millis() + delayMs;
  runtimeStateRefreshReason = reason;
}

void runtimeStateRefreshTick() {
  if (!runtimeStateRefreshPending) return;

  if ((long)(millis() - runtimeStateRefreshDueMs) < 0) return;

  if (!connected ||
      pWriteChar == nullptr ||
      opState != OP_IDLE ||
      bleTxInProgress ||
      parserProcessing ||
      bleRxBusy()) {
    return;
  }

  String reason = runtimeStateRefreshReason;
  runtimeStateRefreshPending = false;
  runtimeStateRefreshReason = "";

  Serial.println("[RUNTIME REFRESH] getDevSta reason=" + reason);

  if (!requestGetDevSta()) {
    requestRuntimeStateRefresh("retry_after_failed_getDevSta_" + reason, 1000);
  }
}

// =====================================================
// CONFIG READ/UPDATE
// =====================================================
int readOnFromDeviceObject(JsonObject dev) {
  if (dev.isNull()) return -999;
  if (!dev["mOnOff"].isNull()) return dev["mOnOff"].as<int>() ? 1 : 0;
  if (!dev["on"].isNull()) return dev["on"].as<int>() ? 1 : 0;
  return -999;
}

int readRuntimeLightLevelFromDeviceObject(JsonObject light) {
  if (light.isNull()) return -999;

  // getDevSta exposes the real current output as "level".
  // This must have priority over mLevel because mLevel belongs to config/cache.
  if (!light["level"].isNull()) {
    return clampInt(light["level"].as<int>(), 0, 100);
  }

  // Fallback only for firmware variants that expose runtime level as mLevel in getDevSta.
  if (!light["mLevel"].isNull()) {
    return clampInt(light["mLevel"].as<int>(), 0, 100);
  }

  return -999;
}

int readRuntimeLightOnFallbackFromDeviceObject(JsonObject light) {
  if (light.isNull()) return -999;

  if (!light["on"].isNull()) {
    return light["on"].as<int>() ? 1 : 0;
  }

  // Fallback only if no runtime level exists.
  // Do not use this when level is present, because mOnOff can be config state.
  if (!light["mOnOff"].isNull()) {
    return light["mOnOff"].as<int>() ? 1 : 0;
  }

  return -999;
}

void applyRuntimeLightLevelToCurrentState(int level100) {
  int lvl = clampInt(level100, 0, 100);

  currentState.lightLevel = lvl;
  currentState.lightOn = lvl > 0 ? 1 : 0;
  currentState.lightBrightness = currentState.lightOn ? lightLevelToBrightness255(lvl) : 0;
}

void applyRuntimeLightOnOnlyToCurrentState(int on) {
  int normalizedOn = on ? 1 : 0;

  currentState.lightOn = normalizedOn;

  if (normalizedOn == 0) {
    currentState.lightLevel = 0;
    currentState.lightBrightness = 0;
  }
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

  if (periods.isNull() || periods.size() < 1) {
    return JsonObject();
  }

  return periods[0].as<JsonObject>();
}

JsonObject ensureLightScheduleObject(JsonObject light) {
  JsonArray periods = light["timePeriod"].as<JsonArray>();

  if (periods.isNull()) {
    periods = light.createNestedArray("timePeriod");
  }

  JsonObject period0;

  if (periods.size() < 1) {
    period0 = periods.createNestedObject();
  } else {
    period0 = periods[0].as<JsonObject>();

    if (period0.isNull()) {
      periods.remove(0);
      period0 = periods.createNestedObject();
    }
  }

  if (period0["enabled"].isNull()) {
    period0["enabled"] = DEFAULT_LIGHT_SCHEDULE_ENABLED;
  }

  if (period0["weekmask"].isNull()) {
    period0["weekmask"] = DEFAULT_LIGHT_SCHEDULE_WEEKMASK;
  }

  if (period0["startTime"].isNull()) {
    period0["startTime"] = hourToSeconds(DEFAULT_LIGHT_SCHEDULE_START_H);
  }

  if (period0["endTime"].isNull()) {
    period0["endTime"] = hourToSeconds(DEFAULT_LIGHT_SCHEDULE_END_H);
  }

  if (period0["brightness"].isNull()) {
    period0["brightness"] = DEFAULT_LIGHT_SCHEDULE_BRIGHTNESS;
  }

  return period0;
}

int readLightModeTypeFromObject(JsonObject light) {
  if (light.isNull()) return -999;
  if (!light["modeType"].isNull()) return light["modeType"].as<int>();
  return -999;
}

int readLightLastAutoModeTypeFromObject(JsonObject light) {
  if (light.isNull()) return -999;
  if (!light["lastAutoModeType"].isNull()) return light["lastAutoModeType"].as<int>();
  return -999;
}

int readLightScheduleEnabledFromObject(JsonObject light) {
  JsonObject p = getLightScheduleObject(light);
  if (p.isNull()) return -999;
  if (!p["enabled"].isNull()) return p["enabled"].as<int>() ? 1 : 0;
  return -999;
}

int readLightScheduleWeekmaskFromObject(JsonObject light) {
  JsonObject p = getLightScheduleObject(light);
  if (p.isNull()) return -999;
  if (!p["weekmask"].isNull()) return p["weekmask"].as<int>();
  return -999;
}

int readLightScheduleStartTimeFromObject(JsonObject light) {
  JsonObject p = getLightScheduleObject(light);
  if (p.isNull()) return -999;
  if (!p["startTime"].isNull()) return clampInt(p["startTime"].as<int>(), 0, 86399);
  return -999;
}

int readLightScheduleEndTimeFromObject(JsonObject light) {
  JsonObject p = getLightScheduleObject(light);
  if (p.isNull()) return -999;
  if (!p["endTime"].isNull()) return clampInt(p["endTime"].as<int>(), 0, 86399);
  return -999;
}

int readLightScheduleBrightnessFromObject(JsonObject light) {
  JsonObject p = getLightScheduleObject(light);
  if (p.isNull()) return -999;
  if (!p["brightness"].isNull()) return clampInt(p["brightness"].as<int>(), 0, 100);
  return -999;
}
bool validateCachedConfigCompleteness(const char* where) {
  JsonObject root = cachedConfigDoc.as<JsonObject>();

  if (root.isNull()) {
    mqttPubDebug("config_validate", String(where) + ":missing_root");
    return false;
  }

  JsonObject devices = root["device"].as<JsonObject>();

  if (devices.isNull()) {
    mqttPubDebug("config_validate", String(where) + ":missing_device_root");
    return false;
  }

  JsonObject light = devices["light"].as<JsonObject>();
  JsonObject blower = devices["blower"].as<JsonObject>();
  JsonObject fan = devices["fan"].as<JsonObject>();

  if (light.isNull()) {
    mqttPubDebug("config_validate", String(where) + ":missing_device_light");
    return false;
  }

  if (fan.isNull()) {
    mqttPubDebug("config_validate", String(where) + ":missing_device_fan");
    return false;
  }

  if (blower.isNull()) {
    mqttPubDebug("config_validate", String(where) + ":missing_device_blower");
    return false;
  }

  if (measureJson(light) < 12) {
    mqttPubDebug("config_validate", String(where) + ":light_block_too_small");
    return false;
  }

  if (measureJson(fan) < 12) {
    mqttPubDebug("config_validate", String(where) + ":fan_block_too_small");
    return false;
  }

  if (measureJson(blower) < 12) {
    mqttPubDebug("config_validate", String(where) + ":blower_block_too_small");
    return false;
  }

  if (readOnFromDeviceObject(light) == -999 || readLightLevelFromObject(light) == -999) {
    mqttPubDebug("config_validate", String(where) + ":light_missing_mOnOff_mLevel");
    return false;
  }

  if (readOnFromDeviceObject(fan) == -999 || readFanLevelFromObject(fan) == -999) {
    mqttPubDebug("config_validate", String(where) + ":fan_missing_mOnOff_mLevel");
    return false;
  }

  if (readOnFromDeviceObject(blower) == -999 || readBlowerLevelFromObject(blower) == -999) {
    mqttPubDebug("config_validate", String(where) + ":blower_missing_mOnOff_level");
    return false;
  }

  return true;
}


void applyConfigCacheToCurrentState() {
  if (!configCacheValid) return;

  JsonObject devices = cachedConfigDoc["device"].as<JsonObject>();
  JsonObject light = devices["light"].as<JsonObject>();
  JsonObject blower = devices["blower"].as<JsonObject>();
  JsonObject fan = devices["fan"].as<JsonObject>();

  int v;

  // IMPORTANT:
  // Do not copy light.mOnOff or light.mLevel from getConfigFile into runtime light state.
  // getConfigFile is configuration/cache. light.ggs_light must represent real output from getDevSta only.

  v = readLightModeTypeFromObject(light);
  if (v != -999) currentState.lightModeType = v;

  v = readLightLastAutoModeTypeFromObject(light);
  if (v != -999) currentState.lightLastAutoModeType = v;

  v = readLightScheduleEnabledFromObject(light);
  if (v != -999) currentState.lightScheduleEnabled = v;

  v = readLightScheduleWeekmaskFromObject(light);
  if (v != -999) currentState.lightScheduleWeekmask = v;

  v = readLightScheduleStartTimeFromObject(light);
  if (v != -999) {
    currentState.lightScheduleStartTime = v;
    currentState.lightScheduleStartHour = secondsToHour(v);
  }

  v = readLightScheduleEndTimeFromObject(light);
  if (v != -999) {
    currentState.lightScheduleEndTime = v;
    currentState.lightScheduleEndHour = secondsToHour(v);
  }

  v = readLightScheduleBrightnessFromObject(light);
  if (v != -999) currentState.lightScheduleBrightness = v;

  // Fan/blower are kept as before because their current implementation still uses config fields
  // as the best available cache. If their getDevSta also exposes reliable runtime fields,
  // they should eventually receive the same runtime/config split as light.

  v = readOnFromDeviceObject(blower);
  if (v != -999) currentState.blowerOn = v;

  v = readBlowerLevelFromObject(blower);
  if (v != -999) {
    currentState.blowerLevel = v;
    currentState.blowerPercentage = blowerLevelToPercentage(v);
  }

  v = readOnFromDeviceObject(fan);
  if (v != -999) currentState.fanOn = v;

  v = readFanLevelFromObject(fan);
  if (v != -999) {
    currentState.fanLevel = v;
    currentState.fanPercentage = fanLevelToPercentage(v);
  }

  v = readFanOscillationFromObject(fan);
  if (v != -999) currentState.fanOscillation = v;

  currentState.configStateValid = true;
  currentState.lastConfigRxMs = millis();
}

bool cacheConfigFileFromDoc(JsonDocument& doc) {
  JsonVariant cfg;

  if (!doc["data"]["configFile"].isNull()) {
    cfg = doc["data"]["configFile"];
  } else if (!doc["params"]["configFile"].isNull()) {
    cfg = doc["params"]["configFile"];
  } else if (!doc["configFile"].isNull()) {
    cfg = doc["configFile"];
  } else if (!doc["data"]["device"].isNull()) {
    cfg = doc["data"];
  } else if (!doc["device"].isNull()) {
    cfg = doc.as<JsonVariant>();
  }

  if (cfg.isNull()) {
    mqttPubDebug("config_cache", "missing_configFile_or_device");
    return false;
  }

  cachedConfigDoc.clear();
  cachedConfigDoc.set(cfg);

  if (cachedConfigDoc.overflowed() || !validateCachedConfigCompleteness("cache")) {
    configCacheValid = false;
    configCacheJsonSize = 0;
    configCacheCrc16 = 0;
    cachedConfigDoc.clear();

    mqttPubDebug("config_cache", "copy_or_validate_failed free_heap=" + String(ESP.getFreeHeap()));
    return false;
  }

  configCacheValid = true;
  configCacheAtMs = millis();

  computeConfigCacheMetadata();
  confirmedConfigCrc16 = configCacheCrc16;

  applyConfigCacheToCurrentState();

  Serial.println("[CONFIG CACHE] valid config_len=" + String(configCacheJsonSize) +
                 " crc=" + crc16Hex(configCacheCrc16) +
                 " free_heap=" + String(ESP.getFreeHeap()));

  mqttPubDebug("config_cache", "valid config_len=" + String(configCacheJsonSize) +
               " crc=" + crc16Hex(configCacheCrc16) +
               " free_heap=" + String(ESP.getFreeHeap()));

  requestDiagnosticsPublish("config_cache_updated");
  requestStatePublish(false, "config_cache_updated");
  return true;
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

  if (t.device == "light" && t.kind == TARGET_KIND_LIGHT_MODE) {
    return readLightModeTypeFromObject(dev);
  }

  if (t.device == "light" && t.kind == TARGET_KIND_LIGHT_SCHEDULE_ENABLED) {
    return readLightScheduleEnabledFromObject(dev);
  }

  if (t.device == "light" && t.kind == TARGET_KIND_LIGHT_SCHEDULE_START_HOUR) {
    int startTime = readLightScheduleStartTimeFromObject(dev);
    return startTime == -999 ? -999 : secondsToHour(startTime);
  }

  if (t.device == "light" && t.kind == TARGET_KIND_LIGHT_SCHEDULE_END_HOUR) {
    int endTime = readLightScheduleEndTimeFromObject(dev);
    return endTime == -999 ? -999 : secondsToHour(endTime);
  }

  if (t.device == "light" && t.kind == TARGET_KIND_LIGHT_SCHEDULE_BRIGHTNESS) {
    return readLightScheduleBrightnessFromObject(dev);
  }

  if (t.kind == TARGET_KIND_FAN_OSCILLATION) {
    return readFanOscillationFromObject(dev);
  }

  if (t.kind == TARGET_KIND_FAN_GEAR) {
    return readFanLevelFromObject(dev);
  }

  if (t.device == "blower") {
    return readBlowerLevelFromObject(dev);
  }

  if (t.device == "light") {
    return readLightLevelFromObject(dev);
  }

  return -999;
}

void normalizeTarget(DeviceTarget& t) {
  t.on = (t.on >= 0) ? (t.on ? 1 : 0) : -1;

  if (t.device == "light") {
    if (t.kind == TARGET_KIND_LIGHT_MODE) {
      t.on = -1;
      if (t.level >= 0) t.level = clampInt(t.level, 0, 1);
    } else if (t.kind == TARGET_KIND_LIGHT_SCHEDULE_ENABLED) {
      t.on = -1;
      if (t.level >= 0) t.level = t.level > 0 ? 1 : 0;
    } else if (t.kind == TARGET_KIND_LIGHT_SCHEDULE_START_HOUR ||
               t.kind == TARGET_KIND_LIGHT_SCHEDULE_END_HOUR) {
      t.on = -1;
      if (t.level >= 0) t.level = clampInt(t.level, 0, 23);
    } else if (t.kind == TARGET_KIND_LIGHT_SCHEDULE_BRIGHTNESS) {
      t.on = -1;
      if (t.level >= 0) t.level = clampInt(t.level, 0, 100);
    } else {
      t.kind = TARGET_KIND_DEVICE_LEVEL;
      if (t.level >= 0) t.level = clampInt(t.level, 0, 100);
    }
  } else if (t.device == "blower") {
    t.kind = TARGET_KIND_DEVICE_LEVEL;
    if (t.level >= 0) t.level = clampInt(t.level, 0, 100);
  } else if (t.device == "fan") {
    if (t.kind == TARGET_KIND_FAN_OSCILLATION) {
      t.on = -1;
      if (t.level >= 0) t.level = normalizeFanOscillation(t.level);
    } else {
      t.kind = TARGET_KIND_FAN_GEAR;
      if (t.level >= 0) t.level = clampInt(t.level, 0, 10);
    }
  }
}

void completeImplicitOnLevelFromCache(DeviceTarget& t) {
  if (t.on != 1 || t.level >= 0) return;

  int cachedLevel = cachedDeviceLevelForTarget(t);

  if (cachedLevel > 0) return;

  if (t.device == "light" && t.kind == TARGET_KIND_DEVICE_LEVEL) {
    t.level = DEFAULT_LIGHT_LEVEL_ON;
  } else if (t.device == "fan" && t.kind == TARGET_KIND_FAN_GEAR) {
    t.level = DEFAULT_FAN_GEAR_ON;
  } else if (t.device == "blower") {
    t.level = DEFAULT_BLOWER_LEVEL_ON;
  }
}
bool patchLightConfig(JsonObject light, DeviceTarget& t) {
  if (light.isNull()) return false;

  if (t.kind == TARGET_KIND_DEVICE_LEVEL) {
    // Manual light control must force manual mode, otherwise the controller may keep using schedule logic.
    light["modeType"] = 0;

    if (t.on >= 0) {
      light["mOnOff"] = t.on ? 1 : 0;
    }

    if (t.level >= 0) {
      int lvl = clampInt(t.level, 0, 100);
      light["mLevel"] = lvl;
      t.level = lvl;

      if (lvl > 0 && t.on < 0) {
        light["mOnOff"] = 1;
        t.on = 1;
      }

      if (lvl == 0 && t.on < 0) {
        light["mOnOff"] = 0;
        t.on = 0;
      }
    }

    return true;
  }

  if (t.kind == TARGET_KIND_LIGHT_MODE) {
    int mode = clampInt(t.level, 0, 1);

    light["modeType"] = mode;

    if (mode == 1) {
      if (light["lastAutoModeType"].isNull()) {
        light["lastAutoModeType"] = 1;
      }

      ensureLightScheduleObject(light);
    }

    t.level = mode;
    return true;
  }

  if (t.kind == TARGET_KIND_LIGHT_SCHEDULE_ENABLED) {
    JsonObject p = ensureLightScheduleObject(light);
    t.level = t.level > 0 ? 1 : 0;
    p["enabled"] = t.level;
    return true;
  }

  if (t.kind == TARGET_KIND_LIGHT_SCHEDULE_START_HOUR) {
    JsonObject p = ensureLightScheduleObject(light);
    t.level = clampInt(t.level, 0, 23);
    p["startTime"] = hourToSeconds(t.level);
    return true;
  }

  if (t.kind == TARGET_KIND_LIGHT_SCHEDULE_END_HOUR) {
    JsonObject p = ensureLightScheduleObject(light);
    t.level = clampInt(t.level, 0, 23);
    p["endTime"] = hourToSeconds(t.level);
    return true;
  }

  if (t.kind == TARGET_KIND_LIGHT_SCHEDULE_BRIGHTNESS) {
    JsonObject p = ensureLightScheduleObject(light);
    t.level = clampInt(t.level, 0, 100);
    p["brightness"] = t.level;
    return true;
  }

  return false;
}

bool patchFanConfig(JsonObject fan, DeviceTarget& t) {
  if (fan.isNull()) return false;

  if (t.kind == TARGET_KIND_FAN_OSCILLATION) {
    // Oscillation is a config-only field on many GGS firmwares.
    // Do not force fan power or speed here.
    int osc = normalizeFanOscillation(t.level);
    fan["shakeLevel"] = osc;
    t.level = osc;
    t.on = -1;
    return true;
  }

  if (t.kind == TARGET_KIND_FAN_GEAR) {
    if (t.level >= 0) {
      int lvl = clampInt(t.level, 0, 10);

      if (lvl <= 0) {
        fan["mOnOff"] = 0;
        t.on = 0;
      } else {
        fan["mOnOff"] = 1;
        fan["mLevel"] = lvl;
        t.on = 1;
      }

      t.level = lvl;
      return true;
    }

    if (t.on >= 0) {
      fan["mOnOff"] = t.on ? 1 : 0;
      return true;
    }
  }

  return false;
}

bool patchBlowerConfig(JsonObject blower, DeviceTarget& t) {
  if (blower.isNull()) return false;

  if (t.on >= 0) {
    blower["mOnOff"] = t.on ? 1 : 0;
  }

  if (t.level >= 0) {
    int lvl = clampInt(t.level, 0, 100);

    if (!blower["mLevel"].isNull()) {
      blower["mLevel"] = lvl;
    }

    if (!blower["maxSpeed"].isNull()) {
      blower["maxSpeed"] = lvl;
    }

    if (blower["mLevel"].isNull() && blower["maxSpeed"].isNull()) {
      blower["mLevel"] = lvl;
    }

    if (lvl > 0 && t.on < 0) {
      blower["mOnOff"] = 1;
      t.on = 1;
    }

    if (lvl == 0 && t.on < 0) {
      blower["mOnOff"] = 0;
      t.on = 0;
    }

    t.level = lvl;
  }

  return true;
}

bool patchConfigForTarget(DeviceTarget& t) {
  if (!configCacheValid || !validateCachedConfigCompleteness("before_patch")) {
    return false;
  }

  normalizeTarget(t);
  completeImplicitOnLevelFromCache(t);

  JsonObject devices = cachedConfigDoc["device"].as<JsonObject>();

  if (devices.isNull()) {
    mqttPubDebug("config_patch", "missing_device_root");
    return false;
  }

  bool ok = false;

  if (t.device == "light") {
    JsonObject light = devices["light"].as<JsonObject>();
    ok = patchLightConfig(light, t);
  } else if (t.device == "fan") {
    JsonObject fan = devices["fan"].as<JsonObject>();
    ok = patchFanConfig(fan, t);
  } else if (t.device == "blower") {
    JsonObject blower = devices["blower"].as<JsonObject>();
    ok = patchBlowerConfig(blower, t);
  }

  if (!ok) {
    mqttPubDebug("config_patch", "patch_failed target=" + targetToString(t));
    return false;
  }

  if (!validateCachedConfigCompleteness("after_patch")) {
    return false;
  }

  updatePendingConfigCrcFromCache();
  applyConfigCacheToCurrentState();

  Serial.println("[CONFIG PATCH] target=" + targetToString(t) +
                 " pending_crc=" + crc16Hex(pendingConfigCrc16));

  mqttPubDebug("config_patch", "ok target=" + targetToString(t) +
               " pending_crc=" + crc16Hex(pendingConfigCrc16));

  requestDiagnosticsPublish("config_patched");
  return true;
}


bool targetMatchesConfig(const DeviceTarget& t) {
  if (!configCacheValid) return false;

  if (t.device == "light" && t.kind == TARGET_KIND_DEVICE_LEVEL) {
    JsonObject light = cachedConfigDoc["device"]["light"].as<JsonObject>();
    int mode = readLightModeTypeFromObject(light);

    if (mode != 0) return false;
  }

  if (t.on >= 0 && cachedDeviceOn(t.device) != t.on) {
    return false;
  }

  if (t.level >= 0) {
    if (t.device == "fan" && t.kind == TARGET_KIND_FAN_GEAR && t.level <= 0) {
      return true;
    }

    if (cachedDeviceLevelForTarget(t) != t.level) {
      return false;
    }
  }

  return true;
}

bool targetMatchesDevState(const DeviceTarget& t) {
  // Runtime verification must be based on getDevSta only.
  // Config cache must never validate a real output target.
  if (!currentState.devStateValid) {
    return false;
  }

  if (t.device == "light" && isLightConfigOnlyKind(t.kind)) {
    return false;
  }

  if (t.device == "light") {
    if (t.kind == TARGET_KIND_DEVICE_LEVEL &&
        currentState.lightModeType >= 0 &&
        currentState.lightModeType != 0) {
      return false;
    }

    if (t.on >= 0 && currentState.lightOn != t.on) {
      return false;
    }

    if (t.level >= 0 && currentState.lightLevel != t.level) {
      return false;
    }

    return true;
  }

  if (t.device == "blower") {
    if (t.on >= 0 && currentState.blowerOn != t.on) {
      return false;
    }

    if (t.level >= 0 && currentState.blowerLevel != t.level) {
      return false;
    }

    return true;
  }

  if (t.device == "fan" && t.kind == TARGET_KIND_FAN_GEAR) {
    if (t.on >= 0 && currentState.fanOn != t.on) {
      return false;
    }

    if (t.level >= 0 && t.level > 0 && currentState.fanLevel != t.level) {
      return false;
    }

    return true;
  }

  return false;
}


bool targetRequiresDevVerification(const DeviceTarget& t) {
  if (t.device == "fan" && t.kind == TARGET_KIND_FAN_OSCILLATION) return false;
  if (t.device == "light" && isLightConfigOnlyKind(t.kind)) return false;
  return true;
}

bool targetAlreadySatisfied(const DeviceTarget& t) {
  if (targetRequiresDevVerification(t)) {
    return targetMatchesDevState(t);
  }

  return targetMatchesConfig(t);
}


bool sameTargetSlot(const DeviceTarget& a, const DeviceTarget& b) {
  return a.device == b.device && a.kind == b.kind;
}

bool targetCoveredBy(const DeviceTarget& existing, const DeviceTarget& incoming) {
  if (!sameTargetSlot(existing, incoming)) return false;

  if (incoming.on >= 0 && (existing.on < 0 || existing.on != incoming.on)) {
    return false;
  }

  if (incoming.level >= 0 && (existing.level < 0 || existing.level != incoming.level)) {
    return false;
  }

  return true;
}

// =====================================================
// TARGET QUEUE
// =====================================================
void clearTargetQueue() {
  for (int i = 0; i < TARGET_QUEUE_SIZE; i++) {
    targetQueue[i] = DeviceTarget();
  }

  targetQueueCount = 0;
  queuedStartPending = false;
}

bool enqueueTarget(const DeviceTarget& rawTarget) {
  DeviceTarget t = rawTarget;
  normalizeTarget(t);

  if (activeTargetValid && targetCoveredBy(activeTarget, t)) {
    return true;
  }

  for (int i = 0; i < targetQueueCount; i++) {
    if (targetCoveredBy(targetQueue[i], t)) {
      return true;
    }
  }

  for (int i = 0; i < targetQueueCount; i++) {
    if (sameTargetSlot(targetQueue[i], t)) {
      targetQueue[i] = t;
      scheduleStartQueued("replace_queued_same_device_kind");
      return true;
    }
  }

  if (targetQueueCount >= TARGET_QUEUE_SIZE) {
    mqttPubDebug("operation", "reject_busy_queue_full " + targetToString(t));
    return false;
  }

  targetQueue[targetQueueCount++] = t;

  publishCommandQueued(t);
  scheduleStartQueued("enqueue_target");
  requestStatusPublish("queue_enqueue");

  return true;
}

bool dequeueTarget(DeviceTarget& out) {
  if (targetQueueCount <= 0) {
    return false;
  }

  out = targetQueue[0];

  for (int i = 1; i < targetQueueCount; i++) {
    targetQueue[i - 1] = targetQueue[i];
  }

  targetQueue[targetQueueCount - 1] = DeviceTarget();
  targetQueueCount--;

  return true;
}

void scheduleStartQueued(const String& reason) {
  if (targetQueueCount <= 0) {
    queuedStartPending = false;
    return;
  }

  queuedStartPending = true;
  queuedStartDueMs = millis() + QUEUED_START_DELAY_MS;
  queuedStartReason = reason;
}

bool canStartQueuedNow() {
  return connected &&
         pWriteChar != nullptr &&
         opState == OP_IDLE &&
         !bleTxInProgress &&
         !parserProcessing &&
         !bleRxBusy() &&
         targetQueueCount > 0;
}

String buildSetConfigFieldJson(const DeviceTarget& t);
String buildSetConfigFileJson();
bool sendSetConfigFieldFromCache(const DeviceTarget& t);
bool sendSetConfigFileFromCache();
bool fallbackToSetConfigFile(const String& reason);
void finishOperationSuccess(const String& reason);
void failOperation(const String& reason);
bool startSetFromActiveTarget() {
  if (!activeTargetValid) return false;
  if (bleTxInProgress || parserProcessing || bleRxBusy()) return false;

  normalizeTarget(activeTarget);

  if (targetAlreadySatisfied(activeTarget)) {
    publishCommandSuccess(activeTarget, targetMatchesConfig(activeTarget), targetMatchesDevState(activeTarget));

    setOperationState(OP_IDLE, "noop_already_satisfied");
    activeTargetValid = false;
    usedFallbackForActiveTarget = false;
    pendingWriteMethod = PENDING_WRITE_NONE;

    requestStatePublish(true, "noop");
    scheduleStartQueued("noop");
    return true;
  }

  publishCommandRunning("patch_config", activeTarget);

  if (!patchConfigForTarget(activeTarget)) {
    failOperation("patch_config_failed");
    return false;
  }

  bool shouldTryField =
    activeWriteStrategy == WRITE_STRATEGY_CONFIG_FIELD_PRIMARY &&
    configFieldSupported;

  if (shouldTryField) {
    if (sendSetConfigFieldFromCache(activeTarget)) {
      return true;
    }

    markConfigFieldFailure("immediate_send_failed", false);
    return fallbackToSetConfigFile("setConfigField_immediate_send_failed");
  }

  return sendSetConfigFileFromCache();
}


void startQueuedIfAny() {
  if (!canStartQueuedNow()) return;

  DeviceTarget next;

  if (!dequeueTarget(next)) return;

  queuedStartPending = false;
  applyDeviceTarget(next.device, next.kind, next.on, next.level);
}

void queuedStartTick() {
  if (!queuedStartPending || targetQueueCount <= 0) return;
  if ((long)(millis() - queuedStartDueMs) < 0) return;

  if (canStartQueuedNow()) {
    startQueuedIfAny();
  }
}

// =====================================================
// SET CONFIGFILE
// =====================================================
// =====================================================
// CONFIG WRITE LAYER
// =====================================================
// setConfigField is preferred because it sends only the already-known full subdevice block.
// The block must still come from cachedConfigDoc, never from a synthetic partial object.
// setConfigFile remains the safety fallback for unsupported firmware or verification failures.
String buildSetConfigFieldJson(const DeviceTarget& t) {
  if (!configCacheValid) return "";

  JsonVariant block = cachedConfigDoc["device"][t.device.c_str()];

  if (block.isNull()) {
    mqttPubDebug("setConfigField", "missing_device_block " + t.device);
    return "";
  }

  String blockJson;
  serializeJson(block, blockJson);

  if (blockJson.length() == 0) {
    return "";
  }

  String json = "{";
  json += "\"method\":\"setConfigField\",";
  json += "\"params\":{";
  json += "\"keyPath\":[\"device\",\"" + t.device + "\"],";
  json += "\"" + t.device + "\":";
  json += blockJson;
  json += "},";
  json += "\"msgId\":\"" + pendingSetMsgId + "\",";
  json += "\"pid\":\"" + String(GGS_PID) + "\",";
  json += "\"pcode\":" + String(GGS_PCODE) + ",";
  json += "\"uid\":\"" + String(GGS_UID) + "\"";
  json += "}";

  return json;
}

String buildSetConfigFileJson() {
  if (!configCacheValid) return "";

  String configFileJson;
  serializeJson(cachedConfigDoc, configFileJson);

  if (configFileJson.length() == 0) {
    return "";
  }

  String json = "{";
  json += "\"method\":\"setConfigFile\",";
  json += "\"params\":{\"configFile\":";
  json += configFileJson;
  json += "},";
  json += "\"msgId\":\"" + pendingSetMsgId + "\",";
  json += "\"pid\":\"" + String(GGS_PID) + "\",";
  json += "\"pcode\":" + String(GGS_PCODE) + ",";
  json += "\"uid\":\"" + String(GGS_UID) + "\"";
  json += "}";

  return json;
}

bool sendSetConfigFieldFromCache(const DeviceTarget& t) {
  if (!configCacheValid || !validateCachedConfigCompleteness("before_setConfigField")) {
    return false;
  }

  pendingSetMsgId = nextMsgId();
  pendingWriteMethod = PENDING_WRITE_CONFIG_FIELD;
  pendingSetJson = buildSetConfigFieldJson(t);

  if (pendingSetJson.length() == 0 || pendingSetJson.length() > MAX_AAAA_PAYLOAD_SIZE) {
    mqttPubDebug("setConfigField", "invalid_payload_len=" + String(pendingSetJson.length()));
    pendingSetMsgId = "";
    pendingSetJson = "";
    pendingWriteMethod = PENDING_WRITE_NONE;
    return false;
  }

  Serial.println("[SET CONFIG FIELD] msgId=" + pendingSetMsgId +
                 " device=" + t.device +
                 " payload_len=" + String(pendingSetJson.length()) +
                 " pending_crc=" + crc16Hex(pendingConfigCrc16));

  opRetryCount = 0;
  setOperationState(OP_SETTING_CONFIG, "sending_setConfigField");
  publishCommandSent("setConfigField", pendingSetMsgId, pendingSetJson.length());

  return sendJsonCommand(pendingSetJson);
}

bool sendSetConfigFileFromCache() {
  if (!configCacheValid || !validateCachedConfigCompleteness("before_setConfigFile")) {
    return false;
  }

  pendingSetMsgId = nextMsgId();
  pendingWriteMethod = PENDING_WRITE_CONFIG_FILE;
  pendingSetJson = buildSetConfigFileJson();

  if (pendingSetJson.length() == 0 || pendingSetJson.length() > MAX_AAAA_PAYLOAD_SIZE) {
    mqttPubDebug("setConfigFile", "invalid_payload_len=" + String(pendingSetJson.length()));
    pendingSetMsgId = "";
    pendingSetJson = "";
    pendingWriteMethod = PENDING_WRITE_NONE;
    return false;
  }

  Serial.println("[SET CONFIG FILE] msgId=" + pendingSetMsgId +
                 " payload_len=" + String(pendingSetJson.length()) +
                 " pending_crc=" + crc16Hex(pendingConfigCrc16));

  opRetryCount = 0;
  setOperationState(OP_SETTING_CONFIG, "sending_setConfigFile");
  publishCommandSent("setConfigFile", pendingSetMsgId, pendingSetJson.length());

  return sendJsonCommand(pendingSetJson);
}

bool fallbackToSetConfigFile(const String& reason) {
  if (!activeTargetValid) {
    failOperation("fallback_without_active_target_" + reason);
    return false;
  }

  if (!configCacheValid || !validateCachedConfigCompleteness("before_fallback_setConfigFile")) {
    failOperation("fallback_cache_invalid_" + reason);
    return false;
  }

  usedFallbackForActiveTarget = true;
  publishCommandFallback(reason);

  pendingSetMsgId = "";
  pendingSetJson = "";
  pendingWriteMethod = PENDING_WRITE_NONE;
  opRetryCount = 0;

  if (!sendSetConfigFileFromCache()) {
    failOperation("fallback_send_setConfigFile_failed_" + reason);
    return false;
  }

  return true;
}
void finishOperationSuccess(const String& reason) {
  bool verifiedConfig = targetMatchesConfig(activeTarget);
  bool verifiedDev = targetRequiresDevVerification(activeTarget) ? targetMatchesDevState(activeTarget) : true;
  bool shouldRefreshRuntimeAfterSuccess = activeTargetValid && activeTarget.device == "light";

  Serial.println("[OP DONE] success reason=" + reason + " target=" + targetToString(activeTarget));

  if (pendingWriteMethod == PENDING_WRITE_CONFIG_FIELD) {
    noteConfigFieldSuccess();
  }

  publishCommandSuccess(activeTarget, verifiedConfig, verifiedDev);
  clearLastError();

  activeTargetValid = false;
  pendingSetMsgId = "";
  pendingSetJson = "";
  pendingWriteMethod = PENDING_WRITE_NONE;
  verifyDevSeen = false;
  verifyConfigSeen = false;
  verifyRequestsSent = false;
  opRetryCount = 0;
  usedFallbackForActiveTarget = false;

  setOperationState(OP_IDLE, "operation_success");
  requestStatePublish(true, "operation_success");
  requestDiagnosticsPublish("operation_success");

  if (shouldRefreshRuntimeAfterSuccess) {
    requestRuntimeStateRefresh("light_operation_success_" + reason, 350);
  }

  scheduleStartQueued("operation_success");
}
void failOperation(const String& reason) {
  Serial.println("[OP FAILED] reason=" + reason + " target=" + targetToString(activeTarget));

  publishCommandFailed(activeTarget, reason);
  setLastError(reason);

  activeTargetValid = false;
  pendingSetMsgId = "";
  pendingSetJson = "";
  pendingWriteMethod = PENDING_WRITE_NONE;
  verifyDevSeen = false;
  verifyConfigSeen = false;
  verifyRequestsSent = false;
  opRetryCount = 0;
  usedFallbackForActiveTarget = false;

  configCacheValid = false;
  configCacheJsonSize = 0;
  configCacheCrc16 = 0;
  pendingConfigCrc16 = 0;

  currentState.configStateValid = false;
  cachedConfigDoc.clear();

  setOperationState(OP_IDLE, "operation_failed");
  requestStatePublish(true, "operation_failed");
  requestDiagnosticsPublish("operation_failed");
  scheduleStartQueued("operation_failed");
}
void scheduleVerificationAfterSetAck() {
  verifyDevSeen = false;
  verifyConfigSeen = false;
  verifyRequestsSent = false;
  verifyRequestDueMs = millis() + POST_SET_VERIFY_DELAY_MS;

  String reason = String(pendingWriteMethodName(pendingWriteMethod)) + "_ack_ok_wait_commit";
  setOperationState(OP_VERIFYING, reason);
  publishCommandRunning("verify_after_write_ack", activeTarget);
}
void verifyOperation() {
  if (opState != OP_VERIFYING || !activeTargetValid) return;

  bool needDev = targetRequiresDevVerification(activeTarget);

  if (!verifyConfigSeen || (needDev && !verifyDevSeen)) {
    Serial.println("[VERIFY] waiting dev_seen=" + boolJson(verifyDevSeen) +
                   " config_seen=" + boolJson(verifyConfigSeen) +
                   " need_dev=" + boolJson(needDev));
    return;
  }

  bool cfgOk = targetMatchesConfig(activeTarget);
  bool devOk = needDev ? targetMatchesDevState(activeTarget) : true;

  Serial.println("[VERIFY] result cfg_ok=" + boolJson(cfgOk) +
                 " dev_ok=" + boolJson(devOk) +
                 " target=" + targetToString(activeTarget));

  if (cfgOk && devOk) {
    finishOperationSuccess(needDev ? "verified_getDevSta_and_getConfigFile" : "verified_getConfigFile_only");
    return;
  }

  if (opRetryCount < MAX_VERIFY_RETRIES) {
    opRetryCount++;
    verifyRequestsSent = false;
    verifyDevSeen = false;
    verifyConfigSeen = false;
    verifyRequestDueMs = millis() + POST_SET_VERIFY_DELAY_MS;
    publishCommandRunning("verify_retry_" + String(opRetryCount), activeTarget);
    return;
  }

  if (pendingWriteMethod == PENDING_WRITE_CONFIG_FIELD && !usedFallbackForActiveTarget) {
    markConfigFieldFailure("verification_mismatch_after_retries", false);
    fallbackToSetConfigFile("verification_mismatch_after_setConfigField");
    return;
  }

  failOperation("verification_mismatch_after_retries");
}
bool applyDeviceTarget(const String& device, TargetKind kind, int on, int level) {
  DeviceTarget t;
  t.device = device;
  t.kind = kind;
  t.on = on;
  t.level = level;

  if (!(device == "light" || device == "fan" || device == "blower")) {
    return false;
  }

  normalizeTarget(t);

  if (!connected || pWriteChar == nullptr) {
    requestStatePublish(true, "reject_not_ble_connected");
    publishCommandFailed(t, "reject_not_ble_connected");
    return false;
  }

  if (opState != OP_IDLE || bleTxInProgress || parserProcessing || bleRxBusy()) {
    return enqueueTarget(t);
  }

  if (targetAlreadySatisfied(t)) {
    publishCommandSuccess(t, targetMatchesConfig(t), targetMatchesDevState(t));
    requestStatePublish(false, "noop_before_start");
    return true;
  }

  activeTarget = t;
  activeTargetValid = true;
  usedFallbackForActiveTarget = false;
  pendingWriteMethod = PENDING_WRITE_NONE;
  opRetryCount = 0;

  publishCommandRunning("start", activeTarget);

  if (!configCacheValid) {
    setOperationState(OP_WAITING_CONFIG, "cache_missing");

    if (!requestGetConfigFile()) {
      failOperation("request_getConfigFile_failed");
      return false;
    }

    return true;
  }

  startSetFromActiveTarget();
  return true;
}
void operationTick() {
  if (opState == OP_IDLE) return;

  unsigned long now = millis();

  if (bleTxInProgress) {
    if (now - bleTxStartedAtMs > BLE_TX_STUCK_TIMEOUT_MS) {
      handleBleTxStuck("operation_tick");
    }

    return;
  }

  if (parserProcessing || bleRxBusy()) return;

  if (opState == OP_WAITING_CONFIG) {
    if (configCacheValid && activeTargetValid) {
      startSetFromActiveTarget();
      return;
    }

    if (now - opStartedAtMs > CONFIG_TIMEOUT_MS) {
      if (opRetryCount < MAX_CONFIG_RETRIES) {
        opRetryCount++;
        opStartedAtMs = now;
        requestGetConfigFile();
      } else {
        failOperation("getConfigFile_timeout");
      }
    }

    return;
  }

  if (opState == OP_SETTING_CONFIG) {
    if (now - opStartedAtMs > SET_ACK_TIMEOUT_MS) {
      if (pendingWriteMethod == PENDING_WRITE_CONFIG_FIELD) {
        markConfigFieldFailure("setConfigField_ack_timeout", false);
        fallbackToSetConfigFile("setConfigField_ack_timeout");
        return;
      }

      if (opRetryCount < MAX_SET_RETRIES && pendingSetJson.length() > 0) {
        opRetryCount++;
        opStartedAtMs = now;
        publishCommandSent(String(pendingWriteMethodName(pendingWriteMethod)) + "_retry", pendingSetMsgId, pendingSetJson.length());
        sendJsonCommand(pendingSetJson);
      } else {
        failOperation("setConfigFile_ack_timeout");
      }
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

      bool needDev = targetRequiresDevVerification(activeTarget);
      bool ok = true;

      if (needDev) {
        ok = requestGetDevSta();
      }

      if (ok) {
        ok = requestGetConfigFile();
      }

      if (!ok) {
        if (pendingWriteMethod == PENDING_WRITE_CONFIG_FIELD && !usedFallbackForActiveTarget) {
          markConfigFieldFailure("send_verify_probe_failed", false);
          fallbackToSetConfigFile("send_verify_probe_failed_after_setConfigField");
        } else {
          failOperation("send_verify_probe_failed");
        }
      }

      return;
    }

    if (now - opStartedAtMs > VERIFY_TIMEOUT_MS) {
      if (opRetryCount < MAX_VERIFY_RETRIES) {
        opRetryCount++;
        verifyRequestsSent = false;
        verifyDevSeen = false;
        verifyConfigSeen = false;
        verifyRequestDueMs = now + POST_SET_VERIFY_DELAY_MS;
        publishCommandRunning("verify_timeout_retry_" + String(opRetryCount), activeTarget);
      } else {
        if (pendingWriteMethod == PENDING_WRITE_CONFIG_FIELD && !usedFallbackForActiveTarget) {
          markConfigFieldFailure("verify_timeout", false);
          fallbackToSetConfigFile("verify_timeout_after_setConfigField");
        } else {
          failOperation("verify_timeout");
        }
      }
    }
  }
}


// =====================================================
// JSON HANDLERS
// =====================================================
void updateStateFromDeviceRoot(JsonVariant rootVar, bool markDevState) {
  JsonObject root = rootVar.as<JsonObject>();

  if (root.isNull()) return;

  JsonObject sensor = root["sensor"].as<JsonObject>();

  if (!sensor.isNull()) {
    if (!sensor["temp"].isNull()) currentState.temp = sensor["temp"].as<float>();
    if (!sensor["humi"].isNull()) currentState.humi = sensor["humi"].as<float>();
    if (!sensor["vpd"].isNull())  currentState.vpd  = sensor["vpd"].as<float>();
  }

  JsonObject fan = root["fan"].as<JsonObject>();
  JsonObject blower = root["blower"].as<JsonObject>();
  JsonObject light = root["light"].as<JsonObject>();

  int v;

  v = readOnFromDeviceObject(fan);
  if (v != -999) currentState.fanOn = v;

  v = readFanLevelFromObject(fan);
  if (v != -999) {
    currentState.fanLevel = v;
    currentState.fanPercentage = fanLevelToPercentage(v);
  }

  v = readOnFromDeviceObject(blower);
  if (v != -999) currentState.blowerOn = v;

  v = readBlowerLevelFromObject(blower);
  if (v != -999) {
    currentState.blowerLevel = v;
    currentState.blowerPercentage = blowerLevelToPercentage(v);
  }

  // Runtime light output:
  // getDevSta.light.level is the authoritative real output.
  // If level exists, it defines both ON/OFF and brightness.
  int runtimeLightLevel = readRuntimeLightLevelFromDeviceObject(light);

  if (runtimeLightLevel != -999) {
    applyRuntimeLightLevelToCurrentState(runtimeLightLevel);
  } else {
    int runtimeLightOn = readRuntimeLightOnFallbackFromDeviceObject(light);

    if (runtimeLightOn != -999) {
      applyRuntimeLightOnOnlyToCurrentState(runtimeLightOn);
    }
  }

  // modeType can appear in getDevSta too. It is not used as brightness/output,
  // but it is useful to keep select.ggs_light_mode coherent.
  v = readLightModeTypeFromObject(light);
  if (v != -999) currentState.lightModeType = v;

  if (markDevState) {
    currentState.devStateValid = true;
    currentState.lastStateRxMs = millis();
  }
}

void handleGetDevSta(JsonDocument& doc) {
  JsonVariant data = doc["data"];

  if (data.isNull()) return;

  if (!data["device"].isNull()) {
    updateStateFromDeviceRoot(data["device"], true);
  } else {
    updateStateFromDeviceRoot(data, true);
  }

  requestStatePublish(false, "getDevSta_received");

  if (opState == OP_VERIFYING) {
    verifyDevSeen = true;
    verifyOperation();
  }
}

void markInitialConfigReceived(const String& reason) {
  if (!initialConfigReceived) {
    Serial.println("[BOOTSTRAP] initialConfigReceived=true reason=" + reason);
  }

  initialConfigReceived = true;
  initialBleBootstrapDone = true;
  bootstrapBleActive = false;

  requestBleAvailabilityPublish(connected);
  requestDiagnosticsPublish("initial_config_received");
  requestStatePublish(true, "initial_config_received");
  requestDiscoveryPublish("initial_config_received");
  requestStatusPublish("initial_config_received");
}

void handleGetConfigFile(JsonDocument& doc) {
  Serial.println("[GETCONFIGFILE] response received");

  bool ok = cacheConfigFileFromDoc(doc);

  if (!ok) {
    if (opState == OP_WAITING_CONFIG || opState == OP_VERIFYING) {
      mqttPubDebug("operation", "getConfigFile_received_but_cache_failed");
    }

    return;
  }

  if (!initialConfigReceived) {
    markInitialConfigReceived("valid_getConfigFile");
  }
  if (opState == OP_IDLE) {
    requestRuntimeStateRefresh("config_file_received_idle", 350);
  }

  if (opState == OP_WAITING_CONFIG && activeTargetValid) {
    if (targetAlreadySatisfied(activeTarget)) {
      finishOperationSuccess("noop_after_config_fetch");
      return;
    }

    return;
  }

  if (opState == OP_VERIFYING) {
    verifyConfigSeen = true;
    verifyOperation();
  }
}
bool ackMsgIdMatchesPending(const char* msgId) {
  if (pendingSetMsgId.length() == 0) return true;
  if (msgId == nullptr || strlen(msgId) == 0) return true;
  return pendingSetMsgId == String(msgId);
}

void handleConfigWriteAck(JsonDocument& doc, PendingWriteMethod ackMethod) {
  int code = doc["code"] | -1;
  const char* msgId = doc["msgId"] | "";

  Serial.println("[CONFIG WRITE ACK] method=" + String(pendingWriteMethodName(ackMethod)) +
                 " code=" + String(code) +
                 " msgId=" + String(msgId) +
                 " pending=" + pendingSetMsgId);

  if (opState != OP_SETTING_CONFIG) {
    mqttPubDebug("config_ack", "ignored_not_setting method=" + String(pendingWriteMethodName(ackMethod)));
    return;
  }

  if (pendingWriteMethod != ackMethod) {
    mqttPubDebug("config_ack", "ignored_method_mismatch ack=" +
                 String(pendingWriteMethodName(ackMethod)) +
                 " pending=" +
                 String(pendingWriteMethodName(pendingWriteMethod)));
    return;
  }

  if (!ackMsgIdMatchesPending(msgId)) {
    mqttPubDebug("config_ack", "ignored_msgid_mismatch got=" + String(msgId) +
                 " pending=" + pendingSetMsgId);
    return;
  }

  if (code != 200) {
    if (pendingWriteMethod == PENDING_WRITE_CONFIG_FIELD) {
      markConfigFieldFailure("bad_ack_code_" + String(code), true);
      fallbackToSetConfigFile("setConfigField_bad_ack_code_" + String(code));
      return;
    }

    if (!bleTxInProgress && opRetryCount < MAX_SET_RETRIES && pendingSetJson.length() > 0) {
      opRetryCount++;
      opStartedAtMs = millis();
      publishCommandSent("setConfigFile_retry_bad_ack", pendingSetMsgId, pendingSetJson.length());
      sendJsonCommand(pendingSetJson);
      return;
    }

    failOperation("setConfigFile_bad_ack_code_" + String(code));
    return;
  }

  opRetryCount = 0;
  scheduleVerificationAfterSetAck();
}

void handleSetConfigFileAck(JsonDocument& doc) {
  handleConfigWriteAck(doc, PENDING_WRITE_CONFIG_FILE);
}

void handleSetConfigFieldAck(JsonDocument& doc) {
  handleConfigWriteAck(doc, PENDING_WRITE_CONFIG_FIELD);
}


void processValidatedJson(const String& payload) {
  rxJsonDoc.clear();

  DeserializationError err = deserializeJson(rxJsonDoc, payload);

  if (err || rxJsonDoc.overflowed()) {
    Serial.println("[JSON INVALIDO] " + String(err ? err.c_str() : "overflow") +
                   " len=" + String(payload.length()) +
                   " free_heap=" + String(ESP.getFreeHeap()));

    mqttPubDebug("parser", String("invalid_json:") + (err ? err.c_str() : "overflow") + " len=" + String(payload.length()));

    rxJsonDoc.clear();
    return;
  }

  const char* method = rxJsonDoc["method"] | "";

  if (!method || strlen(method) == 0) {
    Serial.println("[JSON INVALIDO] method ausente");
    rxJsonDoc.clear();
    return;
  }

  Serial.println("----- JSON VALIDADO -----");
  Serial.println(truncateForMqtt(payload, 1800));
  Serial.println("-------------------------");

  mqttPubDebug("rx_json", String("method=") + method + " len=" + String(payload.length()));

  if (MQTT_PUBLISH_RX_RAW_JSON) {
    mqttPubDebug("rx_raw", truncateForMqtt(payload));
  }

  if (strcmp(method, "getDevSta") == 0) {
    handleGetDevSta(rxJsonDoc);
  } else if (strcmp(method, "getConfigFile") == 0) {
    handleGetConfigFile(rxJsonDoc);
  } else if (strcmp(method, "setConfigFile") == 0) {
    handleSetConfigFileAck(rxJsonDoc);
  } else if (strcmp(method, "setConfigField") == 0) {
    handleSetConfigFieldAck(rxJsonDoc);
  } else if (strcmp(method, "getSysSta") == 0) {
    Serial.println("[INFO] getSysSta valido recebido");
  } else {
    Serial.println("[INFO] JSON valido recebido method=" + String(method));
  }

  rxJsonDoc.clear();
}

// =====================================================
// RX PARSERS
// =====================================================
void parserReset(const char* reason) {
  Serial.print("[PARSER RESET] ");
  Serial.println(reason);

  mqttPubDebug("parser", String("reset:") + reason);

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
    if (ch == '{') {
      jsonParser.collecting = true;
      jsonParser.buf = "{";
      jsonParser.braceDepth = 1;
      jsonParser.inString = false;
      jsonParser.escapeNext = false;
      jsonParser.frameStartMs = millis();
    }

    return;
  }

  if (b < 32 && ch != '\r' && ch != '\n' && ch != '\t') {
    return;
  }

  jsonParser.buf += ch;

  if (jsonParser.escapeNext) {
    jsonParser.escapeNext = false;
    return;
  }

  if (ch == '\\' && jsonParser.inString) {
    jsonParser.escapeNext = true;
    return;
  }

  if (ch == '"') {
    jsonParser.inString = !jsonParser.inString;
    return;
  }

  if (!jsonParser.inString) {
    if (ch == '{') {
      jsonParser.braceDepth++;
    } else if (ch == '}') {
      jsonParser.braceDepth--;
    }
  }

  if (jsonParser.braceDepth == 0) {
    String completed = jsonParser.buf;
    parserReset("json_complete");
    processValidatedJson(completed);
    return;
  }

  if (jsonParser.buf.length() > MAX_AAAA_PAYLOAD_SIZE) {
    parserReset("json_buffer_too_large");
  }
}

void dropRxStreamPrefix(size_t n) {
  if (n == 0) return;

  if (n >= rxStream.size()) {
    rxStream.clear();
    return;
  }

  size_t remain = rxStream.size() - n;
  memmove(rxStream.data(), rxStream.data() + n, remain);
  rxStream.resize(remain);
}

void processAaaaFrame(const std::vector<uint8_t>& frame) {
  if (frame.size() < 22 || frame[0] != 0xAA || frame[1] != 0xAA) {
    return;
  }

  uint16_t frameType = readU16BE(&frame[2]);
  uint16_t bodyLen   = readU16BE(&frame[4]);
  size_t expectedLen = 2 + 2 + 2 + bodyLen + 2;

  if (expectedLen != frame.size() || frameType != 0x0003 || bodyLen < 14) {
    return;
  }

  uint16_t gotFrameCrc = readU16BE(&frame[frame.size() - 2]);
  uint16_t calcFullCrc = crc16Modbus(frame.data(), frame.size() - 2);
  uint16_t calcNoPrefixCrc = crc16Modbus(&frame[2], 4 + bodyLen);

  if (gotFrameCrc != calcFullCrc && gotFrameCrc != calcNoPrefixCrc) {
    mqttPubDebug("rx_frame_error", "frame_crc_error got=0x" + String(gotFrameCrc, HEX));
    return;
  }

  const uint8_t* body = &frame[6];

  uint8_t fieldA = body[0];
  uint8_t fieldB = body[1];
  uint16_t payloadCrc = readU16BE(body + 2);
  uint32_t totalLen   = readU32BE(body + 4);
  uint32_t offset     = readU32BE(body + 8);
  uint16_t chunkLen   = readU16BE(body + 12);

  if ((uint32_t)14 + chunkLen != bodyLen ||
      totalLen == 0 ||
      totalLen > MAX_AAAA_PAYLOAD_SIZE ||
      offset + chunkLen > totalLen) {
    return;
  }

  Serial.println("[AAAA RX FRAME] field_a=" + String(fieldA) +
                 " field_b=" + String(fieldB) +
                 " payload_crc=0x" + String(payloadCrc, HEX) +
                 " total_len=" + String(totalLen) +
                 " offset=" + String(offset) +
                 " chunk_len=" + String(chunkLen));

  if (!aaaaRx.active || aaaaRx.payloadCrc != payloadCrc || aaaaRx.totalLen != totalLen) {
    aaaaRx.reset();
    aaaaRx.active = true;
    aaaaRx.payloadCrc = payloadCrc;
    aaaaRx.totalLen = totalLen;
    aaaaRx.received = 0;
    aaaaRx.startedAtMs = millis();
    aaaaRx.data.assign(totalLen, 0);
    aaaaRx.seen.assign(totalLen, 0);
  }

  const uint8_t* chunk = body + 14;

  for (uint16_t i = 0; i < chunkLen; i++) {
    uint32_t pos = offset + i;

    if (aaaaRx.seen[pos] == 0) {
      aaaaRx.seen[pos] = 1;
      aaaaRx.received++;
    }

    aaaaRx.data[pos] = chunk[i];
  }

  if (aaaaRx.received < aaaaRx.totalLen) {
    return;
  }

  uint16_t calcPayloadCrc = crc16Modbus(aaaaRx.data.data(), aaaaRx.totalLen);

  if (calcPayloadCrc != aaaaRx.payloadCrc) {
    mqttPubDebug("rx_frame_error", "payload_crc_error");
    aaaaRx.reset();
    return;
  }

  String payload;
  payload.reserve(aaaaRx.totalLen + 1);

  for (uint32_t i = 0; i < aaaaRx.totalLen; i++) {
    payload += (char)aaaaRx.data[i];
  }

  Serial.println("[AAAA RX COMPLETE] len=" + String(payload.length()) +
                 " payload_crc=0x" + String(aaaaRx.payloadCrc, HEX) +
                 " free_heap=" + String(ESP.getFreeHeap()));

  aaaaRx.reset();
  processValidatedJson(payload);
}

void parseRxStream() {
  size_t pos = 0;

  while (pos < rxStream.size()) {
    if (rxStream[pos] == 0xAA) {
      if (pos + 2 > rxStream.size()) break;

      if (rxStream[pos + 1] != 0xAA) {
        parserFeedRawByte(rxStream[pos++]);
        continue;
      }

      if (pos + 6 > rxStream.size()) break;

      uint16_t bodyLen = readU16BE(&rxStream[pos + 4]);

      if (bodyLen > 4096) {
        pos++;
        continue;
      }

      size_t totalFrameLen = 2 + 2 + 2 + bodyLen + 2;

      if (pos + totalFrameLen > rxStream.size()) {
        break;
      }

      std::vector<uint8_t> frame(rxStream.begin() + pos, rxStream.begin() + pos + totalFrameLen);

      pos += totalFrameLen;

      processAaaaFrame(frame);
    } else {
      parserFeedRawByte(rxStream[pos++]);
    }
  }

  if (pos > 0) {
    dropRxStreamPrefix(pos);
  }
}

void parserTick() {
  if (jsonParser.collecting &&
      jsonParser.frameStartMs > 0 &&
      millis() - jsonParser.frameStartMs > JSON_RX_TIMEOUT_MS) {
    parserReset("json_timeout");
  }

  if (aaaaRx.active &&
      aaaaRx.startedAtMs > 0 &&
      millis() - aaaaRx.startedAtMs > AAAA_RX_TIMEOUT_MS) {
    mqttPubDebug("rx_frame_error", "assembly_timeout received=" + String(aaaaRx.received) + "/" + String(aaaaRx.totalLen));
    aaaaRx.reset();
  }

  if (rxStream.size() > MAX_RX_STREAM_BUFFER) {
    rxStream.clear();
    aaaaRx.reset();
    parserReset("rx_stream_too_large_reset");
  }

  // Safety net: an isolated partial byte/frame, especially a lone 0xAA,
  // can otherwise keep bleRxBusy() true forever and starve MQTT/WiFi/BLE recovery.
  if (!rxStream.empty() &&
      !jsonParser.collecting &&
      !aaaaRx.active &&
      rxStreamLastAppendAtMs > 0 &&
      millis() - rxStreamLastAppendAtMs > RX_STREAM_IDLE_PARTIAL_TIMEOUT_MS) {
    Serial.println("[PARSER RESET] rx_stream_idle_partial_clear size=" + String(rxStream.size()));
    rxStream.clear();
  }

  if (bleRxBusy()) {
    if (bleBusySinceMs == 0) {
      bleBusySinceMs = millis();
    } else if (millis() - bleBusySinceMs > BLE_BUSY_HARD_TIMEOUT_MS) {
      Serial.println("[BLE BUSY WATCHDOG] BLE RX busy for too long; resetting RX pipeline");
      resetBleRxPipeline("ble_busy_hard_timeout");
    }
  } else {
    bleBusySinceMs = 0;
  }
}
void clearNotifyQueue() {
  portENTER_CRITICAL(&rxNotifyMux);
  rxNotifyHead = rxNotifyTail = 0;
  rxNotifyOverflow = false;
  portEXIT_CRITICAL(&rxNotifyMux);
}


void resetBleRxPipeline(const char* reason) {
  Serial.println(String("[BLE RX RESET] reason=") + reason);

  clearNotifyQueue();

  rxStream.clear();
  aaaaRx.reset();
  parserReset(reason);

  bleBusySinceMs = 0;
  rxStreamLastAppendAtMs = 0;
}

void drainNotifyQueueToRxStream() {
  bool hadOverflow = false;
  unsigned long dropped = 0;

  portENTER_CRITICAL(&rxNotifyMux);
  if (rxNotifyOverflow) {
    hadOverflow = true;
    dropped = rxNotifyDroppedBytes;
    rxNotifyOverflow = false;
    rxNotifyHead = rxNotifyTail = 0;
  }
  portEXIT_CRITICAL(&rxNotifyMux);

  if (hadOverflow) {
    Serial.println("[NOTIFY QUEUE] overflow dropped_bytes=" + String(dropped));
    rxStream.clear();
    aaaaRx.reset();
    parserReset("notify_queue_overflow");
    return;
  }

  size_t drained = 0;

  while (true) {
    bool hasByte = false;
    uint8_t b = 0;

    portENTER_CRITICAL(&rxNotifyMux);
    if (rxNotifyTail != rxNotifyHead) {
      b = rxNotifyQueue[rxNotifyTail];
      rxNotifyTail = rxNotifyNextIndex(rxNotifyTail);
      hasByte = true;
    }
    portEXIT_CRITICAL(&rxNotifyMux);

    if (!hasByte) break;

    rxStream.push_back(b);
    rxStreamLastAppendAtMs = millis();
    drained++;

    if (rxStream.size() > MAX_RX_STREAM_BUFFER) {
      rxStream.clear();
      aaaaRx.reset();
      parserReset("rx_stream_too_large_during_drain");
      break;
    }
  }

  if (drained > 0) {
    Serial.println("[NOTIFY DRAIN] bytes=" + String(drained) + " rx_stream_size=" + String(rxStream.size()));
  }
}

void processRxStreamInLoop() {
  if (rxStream.empty()) return;

  parserProcessing = true;
  parseRxStream();
  parserProcessing = false;
}

// =====================================================
// MQTT CALLBACK
// =====================================================
void mqttCallback(char* topic, byte* payload, unsigned int length) {
  String msg;
  msg.reserve(length + 1);

  for (unsigned int i = 0; i < length; i++) {
    msg += (char)payload[i];
  }

  String t(topic);

  Serial.println("[MQTT RX] " + t + " => " + truncateForMqtt(msg, 260));

  if (isKnownMqttCommandTopic(t) && msg.length() == 0) {
    Serial.println("[MQTT RX] empty command payload ignored topic=" + t);
    return;
  }

  if (t == TOPIC_CMD_RAW) {
    if (opState != OP_IDLE || bleTxInProgress || parserProcessing || bleRxBusy()) return;
    sendJsonCommand(msg);
    return;
  }

  if (t == TOPIC_CMD_GET_DEV_STA) {
    if (opState == OP_SETTING_CONFIG || bleTxInProgress || parserProcessing || bleRxBusy()) return;
    requestGetDevSta();
    return;
  }

  if (t == TOPIC_CMD_GET_CONFIG_FILE) {
    if (opState == OP_SETTING_CONFIG || bleTxInProgress || parserProcessing || bleRxBusy()) return;
    requestGetConfigFile();
    return;
  }

  if (t == TOPIC_CMD_DISCOVERY_REPUBLISH) {
    requestDiscoveryPublish("manual_republish");
    return;
  }

  if (t == TOPIC_CMD_DIAGNOSTICS) {
    requestDiagnosticsPublish("manual_command");
    return;
  }

  if (t == TOPIC_CMD_STRATEGY_SET) {
    ConfigWriteStrategy parsed;

    if (parseWriteStrategyPayload(msg, parsed)) {
      activeWriteStrategy = parsed;

      if (activeWriteStrategy == WRITE_STRATEGY_CONFIG_FIELD_PRIMARY) {
        configFieldSupported = true;
        configFieldSupportKnown = false;
        configFieldFailureCount = 0;
      }

      requestDiagnosticsPublish("strategy_changed");
      requestStatusPublish("strategy_changed");

      String p = "{";
      p += "\"state\":\"success\",";
      p += "\"device\":\"bridge\",";
      p += "\"kind\":\"strategy_set\",";
      p += "\"strategy\":\"" + String(configWriteStrategyName(activeWriteStrategy)) + "\"";
      p += "}";

      publishCommandStatusRaw(p);
    } else {
      setLastError("invalid_strategy_payload");
      publishCommandStatusRaw("{\"state\":\"failed\",\"device\":\"bridge\",\"kind\":\"strategy_set\",\"reason\":\"invalid_strategy_payload\"}");
    }

    return;
  }

  if (t == TOPIC_HA_STATUS) {
    if (msg == "online") {
      requestDiscoveryPublish("homeassistant_online");
    }

    return;
  }

  if (t == TOPIC_CMD_LIGHT_MODE_SET) {
    int mode = -1;

    if (parseLightModePayload(msg, mode)) {
      applyDeviceTarget("light", TARGET_KIND_LIGHT_MODE, -1, mode);
    }

    return;
  }

  if (t == TOPIC_CMD_LIGHT_SCHEDULE_ENABLED_SET) {
    if (mqttPayloadMeansOn(msg)) {
      applyDeviceTarget("light", TARGET_KIND_LIGHT_SCHEDULE_ENABLED, -1, 1);
    } else if (mqttPayloadMeansOff(msg)) {
      applyDeviceTarget("light", TARGET_KIND_LIGHT_SCHEDULE_ENABLED, -1, 0);
    }

    return;
  }

  if (t == TOPIC_CMD_LIGHT_SCHEDULE_START_HOUR_SET) {
    applyDeviceTarget("light", TARGET_KIND_LIGHT_SCHEDULE_START_HOUR, -1, clampInt(msg.toInt(), 0, 23));
    return;
  }

  if (t == TOPIC_CMD_LIGHT_SCHEDULE_END_HOUR_SET) {
    applyDeviceTarget("light", TARGET_KIND_LIGHT_SCHEDULE_END_HOUR, -1, clampInt(msg.toInt(), 0, 23));
    return;
  }

  if (t == TOPIC_CMD_LIGHT_SCHEDULE_BRIGHTNESS_SET) {
    applyDeviceTarget("light", TARGET_KIND_LIGHT_SCHEDULE_BRIGHTNESS, -1, clampInt(msg.toInt(), 0, 100));
    return;
  }

  if (t == TOPIC_CMD_LIGHT_BRIGHTNESS_SET) {
    int level = brightness255ToLightLevel(clampInt(msg.toInt(), 0, 255));
    applyDeviceTarget("light", TARGET_KIND_DEVICE_LEVEL, level > 0 ? 1 : 0, level);
    return;
  }

  if (t == TOPIC_CMD_FAN_PERCENTAGE_SET) {
    int pct = clampInt(msg.toInt(), 0, 100);
    int gear = percentageToFanLevel(pct);
    applyDeviceTarget("fan", TARGET_KIND_FAN_GEAR, pct > 0 ? 1 : 0, gear);
    return;
  }

  if (t == TOPIC_CMD_FAN_GEAR_SET) {
    applyDeviceTarget("fan", TARGET_KIND_FAN_GEAR, 1, clampInt(msg.toInt(), 1, 10));
    return;
  }

  if (t == TOPIC_CMD_FAN_OSCILLATION_SET) {
    applyDeviceTarget("fan", TARGET_KIND_FAN_OSCILLATION, -1, normalizeFanOscillation(msg.toInt()));
    return;
  }

  if (t == TOPIC_CMD_BLOWER_PERCENTAGE_SET) {
    int lvl = percentageToBlowerLevel(msg.toInt());
    applyDeviceTarget("blower", TARGET_KIND_DEVICE_LEVEL, lvl > 0 ? 1 : 0, lvl);
    return;
  }

  if (t == TOPIC_CMD_LIGHT_SET) {
    String levelS = extractJsonNumber(msg, "level");

    if (levelS.length()) {
      int level = clampInt(levelS.toInt(), 0, 100);
      applyDeviceTarget("light", TARGET_KIND_DEVICE_LEVEL, level > 0 ? 1 : 0, level);
    } else if (mqttPayloadMeansOn(msg)) {
      applyDeviceTarget("light", TARGET_KIND_DEVICE_LEVEL, 1, -1);
    } else if (mqttPayloadMeansOff(msg)) {
      applyDeviceTarget("light", TARGET_KIND_DEVICE_LEVEL, 0, -1);
    }

    return;
  }

  if (t == TOPIC_CMD_FAN_SET) {
    String levelS = extractJsonNumber(msg, "level");

    if (levelS.length()) {
      int gear = clampInt(levelS.toInt(), 0, 10);
      applyDeviceTarget("fan", TARGET_KIND_FAN_GEAR, gear > 0 ? 1 : 0, gear);
    } else if (mqttPayloadMeansOn(msg)) {
      applyDeviceTarget("fan", TARGET_KIND_FAN_GEAR, 1, -1);
    } else if (mqttPayloadMeansOff(msg)) {
      applyDeviceTarget("fan", TARGET_KIND_FAN_GEAR, 0, -1);
    }

    return;
  }

  if (t == TOPIC_CMD_BLOWER_SET) {
    String levelS = extractJsonNumber(msg, "level");

    if (levelS.length()) {
      int level = clampInt(levelS.toInt(), 0, 100);
      applyDeviceTarget("blower", TARGET_KIND_DEVICE_LEVEL, level > 0 ? 1 : 0, level);
    } else if (mqttPayloadMeansOn(msg)) {
      applyDeviceTarget("blower", TARGET_KIND_DEVICE_LEVEL, 1, -1);
    } else if (mqttPayloadMeansOff(msg)) {
      applyDeviceTarget("blower", TARGET_KIND_DEVICE_LEVEL, 0, -1);
    }

    return;
  }
}

// =====================================================
// WIFI/MQTT
// =====================================================
void connectWifi() {
  if (WiFi.status() == WL_CONNECTED) return;

  if (connected && (bleTxInProgress || parserProcessing || bleRxBusy())) return;

  Serial.print("[WIFI] Conectando em ");
  Serial.println(WIFI_SSID);

  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  WiFi.begin(WIFI_SSID, WIFI_PASS);

  int tries = 0;

  while (WiFi.status() != WL_CONNECTED && tries < 40) {
    kickTaskWatchdog();
    delay(500);
    Serial.print(".");
    tries++;

    if (connected && (bleTxInProgress || parserProcessing || bleRxBusy())) {
      break;
    }
  }

  Serial.println();

  if (WiFi.status() == WL_CONNECTED) {
    lastWifiOkAtMs = millis();
    Serial.print("[WIFI] OK, IP = ");
    Serial.println(WiFi.localIP());
  } else {
    Serial.println("[WIFI] Falhou ou adiado por BLE ocupado");
  }
}
bool clearRetainedCommandTopic(const char* topic) {
  bool ok = false;

  for (int attempt = 1; attempt <= MQTT_RETAIN_CLEANUP_ATTEMPTS; attempt++) {
    Serial.print("[MQTT RETAIN CLEANUP] clearing ");
    Serial.print(topic);
    Serial.print(" attempt=");
    Serial.print(attempt);
    Serial.print("/");
    Serial.print(MQTT_RETAIN_CLEANUP_ATTEMPTS);

    ok = mqttClient.publish(topic, (const uint8_t*)"", 0, true);

    Serial.print(" -> ");
    Serial.println(ok ? "OK" : "FAIL");

    noteMqttPublishResult(ok);

    mqttClient.loop();
    delay(MQTT_RETAIN_CLEANUP_STEP_DELAY_MS);
    yield();

    if (ok) break;
  }

  return ok;
}

bool clearRetainedCommandTopicsBeforeSubscribe() {
  if (!mqttClient.connected()) return false;

  Serial.print("[MQTT RETAIN CLEANUP] start count=");
  Serial.println(MQTT_COMMAND_TOPIC_COUNT);

  bool allOk = true;

  for (size_t i = 0; i < MQTT_COMMAND_TOPIC_COUNT; i++) {
    bool ok = clearRetainedCommandTopic(MQTT_COMMAND_TOPICS[i]);

    if (!ok) {
      allOk = false;
    }
  }

  Serial.print("[MQTT RETAIN CLEANUP] done ok=");
  Serial.println(allOk ? "true" : "false");

  return allOk;
}

bool subscribeOne(const char* topic) {
  bool ok = mqttClient.subscribe(topic);

  Serial.print("[MQTT SUB] ");
  Serial.print(topic);
  Serial.print(" -> ");
  Serial.println(ok ? "OK" : "FAIL");

  return ok;
}

bool subscribeTopics() {
  bool ok = true;

  for (size_t i = 0; i < MQTT_COMMAND_TOPIC_COUNT; i++) {
    ok &= subscribeOne(MQTT_COMMAND_TOPICS[i]);
  }

  ok &= subscribeOne(TOPIC_HA_STATUS);

  Serial.print("[MQTT SUB] all_ok=");
  Serial.println(ok ? "true" : "false");

  return ok;
}

void connectMqtt() {
  if (mqttClient.connected()) return;
  if (!mqttConnectAllowedNow()) return;
  if (millis() - lastMqttRetry < MQTT_RECONNECT_INTERVAL_MS) return;

  lastMqttRetry = millis();

  if (!mqttBufferConfigured) {
    bool bufOk = mqttClient.setBufferSize(MQTT_BUFFER_SIZE);

    Serial.print("[MQTT] setBufferSize(");
    Serial.print(MQTT_BUFFER_SIZE);
    Serial.print(") -> ");
    Serial.print(bufOk ? "OK" : "FAIL");
    Serial.print(" free_heap=");
    Serial.println(ESP.getFreeHeap());

    mqttBufferConfigured = bufOk;
  }

  String clientId = "ESP32_GGS_" + String((uint32_t)ESP.getEfuseMac(), HEX);

  Serial.println("[MQTT] Conectando em " + String(MQTT_HOST) + ":" + String(MQTT_PORT));

  bool ok = false;

  if (strlen(MQTT_USER) > 0) {
    ok = mqttClient.connect(
      clientId.c_str(),
      MQTT_USER,
      MQTT_PASS,
      TOPIC_BRIDGE_AVAILABILITY,
      0,
      true,
      "offline"
    );
  } else {
    ok = mqttClient.connect(
      clientId.c_str(),
      TOPIC_BRIDGE_AVAILABILITY,
      0,
      true,
      "offline"
    );
  }

  if (ok) {
    Serial.println("[MQTT] OK");

    mqttConsecutiveFails = 0;
    mqttPublishCooldownUntilMs = 0;

    bool cleanupOk = clearRetainedCommandTopicsBeforeSubscribe();

    if (!cleanupOk && MQTT_RETAIN_CLEANUP_REQUIRED) {
      Serial.println("[MQTT RETAIN CLEANUP] failed and required; disconnecting before subscribe to avoid replay");
      mqttClient.disconnect();
      return;
    }

    bool subOk = subscribeTopics();

    if (!subOk) {
      Serial.println("[MQTT] subscribeTopics failed; disconnecting and retrying later");
      mqttClient.disconnect();
      return;
    }

    requestBridgeAvailabilityPublish(true);
    requestBleAvailabilityPublish(connected);
    publishCommandStatusRaw("{\"state\":\"idle\",\"phase\":\"mqtt_connect\"}");
    requestDiagnosticsPublish("mqtt_connect");
    requestStatusPublish("mqtt_connect");
    requestDiscoveryPublish("mqtt_connect");
    requestStatePublish(true, "mqtt_connect");
  } else {
    Serial.println("[MQTT] Falhou rc=" + String(mqttClient.state()));
  }
}

void logMqttStateTransitions() {
  bool nowMqtt = mqttClient.connected();

  if (nowMqtt != prevMqttConnected) {
    Serial.println(String("[MQTT STATE] ") + (nowMqtt ? "connected" : "disconnected"));
    prevMqttConnected = nowMqtt;
  }
}

// =====================================================
// BLE callbacks/core
// =====================================================
class MyClientCallback : public BLEClientCallbacks {
  void onConnect(BLEClient* pclient) override {
    Serial.println("[BLE] Client connected callback");
  }

  void onDisconnect(BLEClient* pclient) override {
    connected = false;
    currentState.bleConnected = false;
    bleDisconnectPending = true;
    Serial.println("[BLE] Disconnected callback: event deferred to loop");
  }
};

class MyAdvertisedDeviceCallbacks : public BLEAdvertisedDeviceCallbacks {
  void onResult(BLEAdvertisedDevice advertisedDevice) override {
    String addr = advertisedDevice.getAddress().toString().c_str();
    String name = advertisedDevice.haveName() ? advertisedDevice.getName().c_str() : "";

    Serial.println("[SCAN] " + addr + " name=" + name);

    String targetName = String(GGS_TARGET_NAME);
    String targetMac = String(GGS_TARGET_MAC);

    if (name == targetName || (targetMac.length() > 0 && addr.equalsIgnoreCase(targetMac))) {
      Serial.println("[SCAN] Target encontrado");

      if (foundDevice != nullptr) {
        delete foundDevice;
        foundDevice = nullptr;
      }

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

    if (nextHead == rxNotifyTail) {
      rxNotifyOverflow = true;
      rxNotifyDroppedBytes++;
      break;
    }

    rxNotifyQueue[rxNotifyHead] = pData[i];
    rxNotifyHead = nextHead;
  }

  portEXIT_CRITICAL(&rxNotifyMux);
}

void cleanupFoundDevice() {
  if (foundDevice != nullptr) {
    delete foundDevice;
    foundDevice = nullptr;
  }
}

void cleanupBleClient(const char* reason) {
  Serial.println(String("[BLE CLEANUP] reason=") + reason);

  if (pClient != nullptr) {
    if (pClient->isConnected()) {
      pClient->disconnect();
      safeDelayMs(150);
    }

    delete pClient;
    pClient = nullptr;
  }

  pNotifyChar = nullptr;
  pWriteChar = nullptr;
}
void startScan() {
  if (connected || doConnect) return;
  if ((long)(millis() - bleConnectRetryAfterMs) < 0) return;

  Serial.println(String("[SCAN] Iniciando scan BLE... free_heap=") +
                 String(ESP.getFreeHeap()) +
                 " min_free_heap=" +
                 String(ESP.getMinFreeHeap()));

  BLEScan* pScan = BLEDevice::getScan();

  static MyAdvertisedDeviceCallbacks advertisedCallbacks;

  pScan->setAdvertisedDeviceCallbacks(&advertisedCallbacks);
  pScan->setActiveScan(true);
  pScan->setInterval(120);
  pScan->setWindow(60);
  pScan->clearResults();
  pScan->start(3, false);

  lastScanAt = millis();
}

void startBleBootstrap() {
  bootstrapBleActive = true;
  initialConfigReceived = false;
  initialBleBootstrapDone = false;
  bootstrapStep = 0;
  bootstrapStartedAtMs = millis();
  bootstrapNextActionAtMs = millis() + BLE_BOOTSTRAP_START_DELAY_MS;
  forceInitialConfigDumpPending = false;

  Serial.println("[BOOTSTRAP] BLE bootstrap iniciado; MQTT/discovery bloqueados");

  requestStatusPublish("ble_bootstrap_started");
}

void completeBootstrapByTimeout() {
  if (!bootstrapBleActive) return;

  bootstrapBleActive = false;
  initialBleBootstrapDone = true;

  Serial.println("[BOOTSTRAP] timeout seguro: liberando MQTT/discovery");

  requestBleAvailabilityPublish(connected);
  requestDiagnosticsPublish("bootstrap_timeout");
  requestStatusPublish("bootstrap_timeout");
  requestStatePublish(true, "bootstrap_timeout");
  requestDiscoveryPublish("bootstrap_timeout");

  if (!configCacheValid) {
    forceInitialConfigDumpPending = true;
    forceInitialConfigDumpAtMs = millis() + FORCE_CONFIG_SAFE_DELAY_MS;
  }
}

bool canSendBootstrapProbeNow() {
  return connected &&
         pWriteChar != nullptr &&
         opState == OP_IDLE &&
         !bleTxInProgress &&
         !parserProcessing &&
         !bleRxBusy();
}

void bootstrapTick() {
  if (!bootstrapBleActive || !connected) return;

  unsigned long now = millis();

  if (initialConfigReceived) {
    bootstrapBleActive = false;
    initialBleBootstrapDone = true;
    return;
  }

  if (now - bootstrapStartedAtMs > BLE_BOOTSTRAP_TIMEOUT_MS) {
    completeBootstrapByTimeout();
    return;
  }

  if (now < bootstrapNextActionAtMs || !canSendBootstrapProbeNow()) return;

  if (bootstrapStep == 0) {
    Serial.println("[BOOT PROBE] getDevSta inicial");

    if (requestGetDevSta()) {
      bootstrapStep = 1;
      bootstrapNextActionAtMs = millis() + BLE_BOOTSTRAP_BETWEEN_REQ_MS;
    } else {
      bootstrapNextActionAtMs = millis() + 1000;
    }
  } else if (bootstrapStep == 1) {
    Serial.println("[BOOT PROBE] getConfigFile inicial");

    if (requestGetConfigFile()) {
      bootstrapStep = 2;
      bootstrapNextActionAtMs = millis() + 1000;
    } else {
      bootstrapNextActionAtMs = millis() + 1000;
    }
  }
}

bool connectToServer() {
  if (!foundDevice) return false;
  if ((long)(millis() - bleConnectRetryAfterMs) < 0) return false;

  Serial.print("[BLE] Conectando em ");
  Serial.println(foundDevice->getAddress().toString().c_str());

  Serial.println(String("[BLE] free_heap_before_connect=") +
                 String(ESP.getFreeHeap()) +
                 " min_free_heap=" +
                 String(ESP.getMinFreeHeap()));

  BLEDevice::getScan()->stop();
  safeDelayMs(350);

  cleanupBleClient("before_new_connect_attempt");

  pClient = BLEDevice::createClient();

  if (pClient == nullptr) {
    Serial.println("[BLE] createClient retornou nullptr");
    bleConsecutiveConnectFailures++;
    bleConnectRetryAfterMs = millis() + BLE_CONNECT_RETRY_BACKOFF_MS;
    return false;
  }

  static MyClientCallback clientCallbacks;

  pClient->setClientCallbacks(&clientCallbacks);

  bool okConnect = pClient->connect(foundDevice);

  if (!okConnect) {
    bleConsecutiveConnectFailures++;

    Serial.println(String("[BLE] connect() falhou fail_count=") +
                   String(bleConsecutiveConnectFailures) +
                   " free_heap=" +
                   String(ESP.getFreeHeap()) +
                   " min_free_heap=" +
                   String(ESP.getMinFreeHeap()));

    cleanupBleClient("connect_failed");
    cleanupFoundDevice();

    bleConnectRetryAfterMs = millis() + BLE_CONNECT_RETRY_BACKOFF_MS;

    if (bleConsecutiveConnectFailures >= BLE_CONNECT_REINIT_AFTER_FAILS) {
      Serial.println("[BLE] muitas falhas de connect; reinicializando stack BLE");
      BLEDevice::deinit(true);
      safeDelayMs(1000);
      BLEDevice::init("ESP32-GGS");
      bleConsecutiveConnectFailures = 0;
      safeDelayMs(500);
    }

    return false;
  }

  bleConsecutiveConnectFailures = 0;

  Serial.println(String("[BLE] connect() OK free_heap=") +
                 String(ESP.getFreeHeap()) +
                 " min_free_heap=" +
                 String(ESP.getMinFreeHeap()));

  pClient->setMTU(517);
  safeDelayMs(500);

  std::map<std::string, BLERemoteService*>* services = pClient->getServices();

  if (services == nullptr) {
    Serial.println("[BLE] getServices retornou nullptr");
    cleanupBleClient("services_null");
    cleanupFoundDevice();
    bleConnectRetryAfterMs = millis() + BLE_CONNECT_RETRY_BACKOFF_MS;
    return false;
  }

  pNotifyChar = nullptr;
  pWriteChar = nullptr;

  for (auto const& svcPair : *services) {
    BLERemoteService* svc = svcPair.second;

    if (pNotifyChar == nullptr) {
      pNotifyChar = svc->getCharacteristic(NOTIFY_UUID);
    }

    if (pWriteChar == nullptr) {
      pWriteChar = svc->getCharacteristic(WRITE_UUID);
    }
  }

  if (pNotifyChar == nullptr || pWriteChar == nullptr) {
    Serial.println("[BLE] FF01 ou FF02 nao encontrada");
    cleanupBleClient("missing_ff01_or_ff02");
    cleanupFoundDevice();
    bleConnectRetryAfterMs = millis() + BLE_CONNECT_RETRY_BACKOFF_MS;
    return false;
  }

  if (!pNotifyChar->canNotify()) {
    Serial.println("[BLE] FF01 nao suporta notify");
    cleanupBleClient("notify_not_supported");
    cleanupFoundDevice();
    bleConnectRetryAfterMs = millis() + BLE_CONNECT_RETRY_BACKOFF_MS;
    return false;
  }

  pNotifyChar->registerForNotify(notifyCallback);

  BLERemoteDescriptor* p2902 = pNotifyChar->getDescriptor(BLEUUID((uint16_t)0x2902));

  if (p2902 != nullptr) {
    uint8_t val[] = {0x01, 0x00};
    p2902->writeValue(val, 2, true);
  }

  connected = true;
  currentState.bleConnected = true;

  requestBleAvailabilityPublish(true);
  requestDiagnosticsPublish("ble_connected");

  cleanupFoundDevice();
  clearNotifyQueue();

  rxStream.clear();
  aaaaRx.reset();
  parserReset("ble_connected");

  startBleBootstrap();

  Serial.println("[BLE] Conectado e pronto; probes pelo bootstrapTick");

  return true;
}

void handleBleDisconnectEvent() {
  if (!bleDisconnectPending) return;

  bleDisconnectPending = false;

  connected = false;
  currentState.bleConnected = false;

  bleTxInProgress = false;
  bleTxStartedAtMs = 0;
  bleTxLabel = "";
  bootstrapBleActive = false;

  clearNotifyQueue();

  rxStream.clear();
  aaaaRx.reset();
  parserReset("ble_disconnect");

  if (opState != OP_IDLE) {
    activeTargetValid = false;
    clearTargetQueue();

    pendingSetMsgId = "";
    pendingSetJson = "";
    pendingWriteMethod = PENDING_WRITE_NONE;
    usedFallbackForActiveTarget = false;

    verifyDevSeen = false;
    verifyConfigSeen = false;
    verifyRequestsSent = false;
    opRetryCount = 0;

    setOperationState(OP_IDLE, "ble_disconnect");
  }

  requestBleAvailabilityPublish(false);
  requestDiagnosticsPublish("ble_disconnect");
  requestStatusPublish("ble_disconnect");
}


// =====================================================
// RECOVERY BLE / WIFI / MQTT / HEAP SUPERVISOR
// =====================================================
void forceBleReconnect(const char* reason) {
  Serial.println(String("[BLE RECOVERY] force reconnect reason=") + reason);

  bleTxInProgress = false;
  bleTxStartedAtMs = 0;
  bleTxLabel = "";

  bootstrapBleActive = false;
  doConnect = false;
  connected = false;
  currentState.bleConnected = false;

  clearTargetQueue();

  activeTargetValid = false;
  pendingSetMsgId = "";
  pendingSetJson = "";
  pendingWriteMethod = PENDING_WRITE_NONE;
  usedFallbackForActiveTarget = false;

  verifyDevSeen = false;
  verifyConfigSeen = false;
  verifyRequestsSent = false;
  opRetryCount = 0;

  if (opState != OP_IDLE) {
    setOperationState(OP_IDLE, String("ble_recovery_") + reason);
  }

  resetBleRxPipeline(reason);

  cleanupBleClient(reason);
  cleanupFoundDevice();

  bleConnectRetryAfterMs = millis() + 1000;

  requestBleAvailabilityPublish(false);
  requestDiagnosticsPublish(String("ble_recovery_") + reason);
  requestStatusPublish(String("ble_recovery_") + reason);
}

void reinitBleStack(const char* reason) {
  unsigned long now = millis();

  if (now - lastBleStackReinitAtMs < BLE_STACK_REINIT_COOLDOWN_MS) {
    Serial.println("[BLE REINIT] skipped by cooldown");
    return;
  }

  lastBleStackReinitAtMs = now;

  Serial.println(String("[BLE REINIT] reason=") + reason);

  forceBleReconnect(reason);

  BLEDevice::getScan()->stop();
  safeDelayMs(300);

  BLEDevice::deinit(true);
  safeDelayMs(1000);

  BLEDevice::init("ESP32-GGS");
  safeDelayMs(500);

  bleConsecutiveConnectFailures = 0;
  bleStaleReconnectCount = 0;

  startScan();
}

bool safeForSystemRecovery() {
  return !bleTxInProgress &&
         !parserProcessing &&
         !bleRxBusy();
}

void restartEsp32Now(const String& reason) {
  Serial.println("[ESP RESTART] reason=" + reason);

  if (mqttClient.connected()) {
    mqttClient.publish(TOPIC_BRIDGE_AVAILABILITY, "offline", true);
    mqttClient.publish(TOPIC_BLE_AVAILABILITY, "offline", true);
    mqttClient.publish(TOPIC_LEGACY_AVAILABILITY, "offline", true);
    mqttClient.loop();
    safeDelayMs(200);
  }

  ESP.restart();
}

void recoverWifiStack(const char* reason) {
  unsigned long now = millis();

  if (now - lastWifiRecoveryAtMs < 60000) {
    Serial.println("[WIFI RECOVERY] skipped by cooldown");
    return;
  }

  lastWifiRecoveryAtMs = now;

  Serial.println(String("[WIFI RECOVERY] reason=") + reason);

  if (mqttClient.connected()) {
    mqttClient.disconnect();
  }

  WiFi.disconnect(false);
  safeDelayMs(500);

  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  WiFi.begin(WIFI_SSID, WIFI_PASS);

  lastMqttRetry = 0;
}

void supervisorTick() {
  unsigned long now = millis();

  if (WiFi.status() == WL_CONNECTED) {
    lastWifiOkAtMs = now;
  }

  if (mqttClient.connected()) {
    lastMqttOkAtMs = now;
  }

  if (connected) {
    lastBleOkAtMs = now;
  }

  uint32_t freeHeap = ESP.getFreeHeap();

  if (freeHeap < CRITICAL_HEAP_LIMIT) {
    restartEsp32Now("critical_heap_" + String(freeHeap));
    return;
  }

  if (freeHeap < LOW_HEAP_LIMIT) {
    if (lowHeapSinceMs == 0) {
      lowHeapSinceMs = now;
    }

    if (now - lastLowHeapWarningAtMs > LOW_HEAP_WARNING_COOLDOWN_MS) {
      lastLowHeapWarningAtMs = now;

      Serial.println("[HEAP WARNING] low heap free_heap=" + String(freeHeap) +
                    " min_free_heap=" + String(ESP.getMinFreeHeap()) +
                    " low_heap_age_ms=" + String(now - lowHeapSinceMs));

      requestDiagnosticsPublish("low_heap_warning");
    }
  } else {
    lowHeapSinceMs = 0;
  }

  if (freeHeap < CRITICAL_HEAP_LIMIT) {
    restartEsp32Now("critical_heap_" + String(freeHeap));
    return;
  }


  if (!safeForSystemRecovery()) {
    return;
  }

  if (WiFi.status() != WL_CONNECTED &&
      lastWifiOkAtMs > 0 &&
      now - lastWifiOkAtMs > WIFI_DOWN_RECOVERY_MS) {
    recoverWifiStack("wifi_down_too_long");
    return;
  }

  if (WiFi.status() == WL_CONNECTED &&
      !mqttClient.connected() &&
      lastMqttOkAtMs > 0 &&
      now - lastMqttOkAtMs > MQTT_DOWN_RECOVERY_MS) {
    recoverWifiStack("mqtt_down_too_long");
    return;
  }

  if (!connected &&
      !doConnect &&
      lastBleOkAtMs > 0 &&
      now - lastBleOkAtMs > BLE_DOWN_REINIT_MS) {
    reinitBleStack("ble_down_too_long");
    return;
  }
}

// =====================================================
// setup/loop
// =====================================================
void setup() {
  Serial.begin(115200);
  delay(1500);

  setupTaskWatchdog();

  bootStartedAtMs = millis();
  lastWifiOkAtMs = millis();
  lastMqttOkAtMs = millis();
  lastBleOkAtMs = millis();

  Serial.println();
  Serial.println("=================================");
  Serial.println("[BOOT] ESP32 GGS Bridge iniciando");
  Serial.println("=================================");
  Serial.println("[BOOT] FW_VERSION=" + String(FW_VERSION));

  initializeLocalStateDefaults();
  clearTargetQueue();

  rxStream.reserve(2048);
  jsonParser.buf.reserve(1024);

  WiFi.persistent(false);
  WiFi.setAutoReconnect(true);
  WiFi.setSleep(false);

  connectWifi();

  mqttClient.setServer(MQTT_HOST, MQTT_PORT);
  mqttClient.setCallback(mqttCallback);
  mqttClient.setKeepAlive(30);
  mqttClient.setSocketTimeout(5);

  BLEDevice::init("ESP32-GGS");
  Serial.println("[BLE] BLEDevice init OK");

  startScan();
}

void serviceBleRxFirst() {
  drainNotifyQueueToRxStream();
  processRxStreamInLoop();
}

void loop() {
  kickTaskWatchdog();

  handleBleDisconnectEvent();

  serviceBleRxFirst();
  parserTick();
  bleTxWatchdogTick();

  if (parserProcessing || bleRxBusy()) {
    delay(1);
    return;
  }

  if (doConnect && !connected) {
    doConnect = false;

    if (!connectToServer()) {
      startScan();
    }
  }

  bootstrapTick();

  serviceBleRxFirst();
  parserTick();

  if (parserProcessing || bleRxBusy()) {
    delay(1);
    return;
  }

  operationTick();
  queuedStartTick();
  runtimeStateRefreshTick();

  serviceBleRxFirst();
  parserTick();

  if (parserProcessing || bleRxBusy() || bleTxInProgress) {
    delay(1);
    return;
  }

  if (WiFi.status() != WL_CONNECTED) {
    connectWifi();
  }

  if (!mqttClient.connected()) {
    connectMqtt();
  }

  if (mqttClient.connected() && !bleTxInProgress && !parserProcessing && !bleRxBusy()) {
    mqttClient.loop();
  }

  logMqttStateTransitions();

  availabilityPublishTick();
  statusPublishTick();
  diagnosticsPublishTick();
  discoveryTick();
  statePublishTick();

  if (!connected && !doConnect && millis() - lastScanAt > 15000) {
    startScan();
  }

  if (connected && initialBleBootstrapDone) {
    if (opState == OP_IDLE &&
        !bleTxInProgress &&
        !parserProcessing &&
        !bleRxBusy() &&
        millis() - lastProbeAt > PROBE_INTERVAL_MS) {
      Serial.println("[BLE] Probe periodico getDevSta...");
      requestGetDevSta();
    }

    if (currentState.lastStateRxMs > 0 &&
        millis() - currentState.lastStateRxMs > STATE_STALE_TIMEOUT_MS) {
      Serial.println("[BLE] Timeout sem getDevSta valido; recovery BLE");

      bleStaleReconnectCount++;

      if (bleStaleReconnectCount >= BLE_STALE_RECONNECTS_BEFORE_REINIT) {
        reinitBleStack("state_stale_repeated");
      } else {
        forceBleReconnect("state_stale_reconnect");
        safeDelayMs(500);
        startScan();
      }
    } else {
      bleStaleReconnectCount = 0;
    }
  }

  if (forceInitialConfigDumpPending &&
      connected &&
      initialBleBootstrapDone &&
      !configCacheValid &&
      opState == OP_IDLE &&
      !activeTargetValid &&
      targetQueueCount == 0 &&
      !queuedStartPending &&
      !bleTxInProgress &&
      !parserProcessing &&
      !bleRxBusy() &&
      millis() >= forceInitialConfigDumpAtMs) {
    forceInitialConfigDumpPending = false;

    Serial.println("[FORCE INITIAL CONFIG DUMP - SAFE FALLBACK] getConfigFile");

    requestGetConfigFile();
  }

  supervisorTick();

  kickTaskWatchdog();
  delay(5);
}