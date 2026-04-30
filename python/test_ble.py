#!/usr/bin/env python3
"""
Interactive BLE test tool for a Spider Farmer / GGS controller.

This script is intentionally separate from the ESP32 firmware. It is useful for
validating the protocol, testing setConfigFile changes, and collecting logs before
changing the embedded bridge.

No real MAC address, PID, UID, IP address, or password is stored in this file.
Pass device-specific values by CLI arguments or environment variables.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import json
import os
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any, Optional

from bleak import BleakClient, BleakScanner

NOTIFY_UUID = "0000ff01-0000-1000-8000-00805f9b34fb"
WRITE_UUID = "0000ff02-0000-1000-8000-00805f9b34fb"

DEFAULT_TARGET_NAME = "SF-GGS-CB"
MAX_RAW_JSON_WRITE_BYTES = 500

# The controller sends large JSON payloads over an AA AA binary stream. The same
# framing is used here for large writes such as setConfigFile.
AAAA_FRAME_MAGIC = b"\xAA\xAA"
AAAA_TX_FRAME_TYPE_DEFAULT = 0x0003
AAAA_TX_FIELD_A_DEFAULT = 0x00
AAAA_TX_FIELD_B_DEFAULT = 0x01
AAAA_TX_CHUNK_SIZE_DEFAULT = 80
AAAA_INTER_FRAME_DELAY_SECONDS = 0.035

DEFAULT_LEVELS = {
    "light": 100,
    "blower": 50,
    "fan": 5,
}

_msg_counter = 0


def now_msg_id() -> str:
    global _msg_counter
    _msg_counter += 1
    return f"{int(time.time())}{_msg_counter:03d}"


def compact_json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def crc16_modbus(data: bytes, init: int = 0xFFFF) -> int:
    crc = init & 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
            crc &= 0xFFFF
    return crc & 0xFFFF


def u16be(value: int) -> bytes:
    return int(value).to_bytes(2, "big")


def u32be(value: int) -> bytes:
    return int(value).to_bytes(4, "big")


def build_command(
    method: str,
    params: Optional[dict] = None,
    *,
    pid: Optional[str] = None,
    uid: Optional[str] = None,
    pcode: Optional[int] = None,
    include_device_ids: bool = True,
) -> tuple[bytes, str, OrderedDict]:
    msg_id = now_msg_id()
    command = OrderedDict()
    command["method"] = method
    command["params"] = params if params is not None else OrderedDict()
    command["msgId"] = msg_id

    if include_device_ids:
        if not pid or not uid:
            raise ValueError("PID and UID are required for this command. Use --pid and --uid or environment variables.")
        command["pid"] = pid
        if pcode is not None:
            command["pcode"] = int(pcode)
        command["uid"] = uid

    return compact_json_bytes(command), msg_id, command


def cmd_get_dev_sta() -> tuple[bytes, str, OrderedDict]:
    return build_command("getDevSta", OrderedDict(), include_device_ids=False)


def cmd_get_config_file(pid: str, uid: str, pcode: Optional[int]) -> tuple[bytes, str, OrderedDict]:
    return build_command("getConfigFile", OrderedDict(), pid=pid, uid=uid, pcode=pcode, include_device_ids=True)


def cmd_set_config_file(config_file: dict, *, pid: str, uid: str, pcode: Optional[int]) -> tuple[bytes, str, OrderedDict]:
    params = OrderedDict()
    params["configFile"] = config_file
    return build_command("setConfigFile", params, pid=pid, uid=uid, pcode=pcode, include_device_ids=True)


def build_aaaa_write_frames(payload: bytes, *, chunk_size: int = AAAA_TX_CHUNK_SIZE_DEFAULT) -> list[bytes]:
    if not payload:
        raise ValueError("Refusing to send an empty AA AA payload")
    if chunk_size <= 0 or chunk_size > 480:
        raise ValueError("Invalid chunk size. Use a conservative value between 40 and 180 bytes.")

    payload_crc = crc16_modbus(payload)
    frames: list[bytes] = []

    for offset in range(0, len(payload), chunk_size):
        chunk = payload[offset:offset + chunk_size]
        body = (
            bytes([AAAA_TX_FIELD_A_DEFAULT, AAAA_TX_FIELD_B_DEFAULT])
            + u16be(payload_crc)
            + u32be(len(payload))
            + u32be(offset)
            + u16be(len(chunk))
            + chunk
        )
        header = AAAA_FRAME_MAGIC + u16be(AAAA_TX_FRAME_TYPE_DEFAULT) + u16be(len(body))
        frame_crc = crc16_modbus(header + body)
        frames.append(header + body + u16be(frame_crc))

    return frames


async def write_payload(client: BleakClient, payload: bytes) -> None:
    try:
        await client.write_gatt_char(WRITE_UUID, payload, response=True)
    except Exception as exc:
        print(f"[WRITE] response=True failed, retrying response=False: {exc!r}")
        await client.write_gatt_char(WRITE_UUID, payload, response=False)


async def write_payload_auto(client: BleakClient, payload: bytes, *, force_framed: bool = False, chunk_size: int = AAAA_TX_CHUNK_SIZE_DEFAULT) -> dict[str, Any]:
    if not force_framed and len(payload) <= MAX_RAW_JSON_WRITE_BYTES:
        print(f"[WRITE] raw JSON len={len(payload)}")
        await write_payload(client, payload)
        return {"transport": "raw", "payload_len": len(payload), "frames": 0}

    frames = build_aaaa_write_frames(payload, chunk_size=chunk_size)
    print(f"[WRITE] AA AA framed payload_len={len(payload)} frames={len(frames)} chunk_size={chunk_size}")

    for index, frame in enumerate(frames, start=1):
        print(f"[AAAA TX] {index}/{len(frames)} frame_len={len(frame)} hex={frame.hex(' ')}")
        await write_payload(client, frame)
        await asyncio.sleep(AAAA_INTER_FRAME_DELAY_SECONDS)

    return {"transport": "aaaa", "payload_len": len(payload), "frames": len(frames), "chunk_size": chunk_size}


class NotifyParser:
    """Incrementally reconstructs raw JSON and AA AA fragmented JSON."""

    def __init__(self, *, verbose: bool = True) -> None:
        self.buffer = bytearray()
        self.parts: dict[tuple[int, int, int, int, int], dict[str, Any]] = {}
        self.verbose = verbose
        self.last_frame_meta: Optional[dict[str, Any]] = None

    def feed(self, data: bytes) -> list[dict[str, Any]]:
        self.buffer.extend(data)
        output: list[dict[str, Any]] = []

        while self.buffer:
            if len(self.buffer) >= 2 and self.buffer[0] == 0xAA and self.buffer[1] == 0xAA:
                parsed = self._try_parse_aaaa_frame()
                if parsed is None:
                    break
                if parsed is not False:
                    output.append(parsed)
                continue

            parsed = self._try_parse_raw_json()
            if parsed is None:
                break
            if parsed is not False:
                output.append(parsed)

        return output

    def _try_parse_raw_json(self):
        while self.buffer and self.buffer[0] not in (ord("{"), ord("[")):
            del self.buffer[0]
        if not self.buffer:
            return False

        try:
            text = self.buffer.decode("utf-8")
        except UnicodeDecodeError:
            return None

        decoder = json.JSONDecoder()
        try:
            obj, end = decoder.raw_decode(text)
        except json.JSONDecodeError:
            return None

        consumed = len(text[:end].encode("utf-8"))
        del self.buffer[:consumed]
        return obj if isinstance(obj, dict) else {"_json": obj}

    def _try_parse_aaaa_frame(self):
        if len(self.buffer) < 8:
            return None

        frame_type = int.from_bytes(self.buffer[2:4], "big")
        body_len = int.from_bytes(self.buffer[4:6], "big")
        total_len = 2 + 2 + 2 + body_len + 2

        if len(self.buffer) < total_len:
            return None

        header = bytes(self.buffer[:6])
        body = bytes(self.buffer[6:6 + body_len])
        frame_crc = int.from_bytes(self.buffer[6 + body_len:6 + body_len + 2], "big")
        del self.buffer[:total_len]

        calc_header_body = crc16_modbus(header + body)
        calc_type_len_body = crc16_modbus(header[2:] + body)

        if self.verbose:
            print(
                "[AAAA RX FRAME] "
                f"type=0x{frame_type:04X} body_len={body_len} frame_crc=0x{frame_crc:04X} "
                f"calc_header_body=0x{calc_header_body:04X} calc_type_len_body=0x{calc_type_len_body:04X}"
            )

        if len(body) < 14:
            return self._json_from_bytes_if_possible(body)

        field_a = body[0]
        field_b = body[1]
        payload_crc = int.from_bytes(body[2:4], "big")
        payload_total_len = int.from_bytes(body[4:8], "big")
        offset = int.from_bytes(body[8:12], "big")
        chunk_len = int.from_bytes(body[12:14], "big")

        self.last_frame_meta = {
            "frame_type": frame_type,
            "field_a": field_a,
            "field_b": field_b,
            "payload_crc": payload_crc,
            "payload_total_len": payload_total_len,
            "offset": offset,
            "chunk_len": chunk_len,
        }

        if payload_total_len <= 0 or 14 + chunk_len > len(body):
            return False

        key = (frame_type, field_a, field_b, payload_crc, payload_total_len)
        entry = self.parts.setdefault(key, {"data": bytearray(payload_total_len), "ranges": []})
        chunk = body[14:14 + chunk_len]
        end = min(offset + chunk_len, payload_total_len)
        if end > offset:
            entry["data"][offset:end] = chunk[:end - offset]
            entry["ranges"].append((offset, end))

        if not self._ranges_cover(entry["ranges"], payload_total_len):
            return False

        payload = bytes(entry["data"])
        del self.parts[key]

        if crc16_modbus(payload) != payload_crc:
            return {"_error": "payload_crc_mismatch", "payload_len": len(payload)}

        return self._json_from_bytes_if_possible(payload)

    @staticmethod
    def _ranges_cover(ranges: list[tuple[int, int]], total_len: int) -> bool:
        merged: list[list[int]] = []
        for start, end in sorted(ranges):
            if start >= end:
                continue
            if not merged or start > merged[-1][1]:
                merged.append([start, end])
            else:
                merged[-1][1] = max(merged[-1][1], end)
        return len(merged) == 1 and merged[0][0] == 0 and merged[0][1] >= total_len

    @staticmethod
    def _json_from_bytes_if_possible(data: bytes) -> dict[str, Any]:
        try:
            return json.loads(data.decode("utf-8").strip("\x00\r\n\t "))
        except Exception:
            return {"_raw_frame_hex": data.hex(" "), "_raw_frame_len": len(data)}


async def find_device(target_name: str, target_mac: Optional[str]) -> Any:
    devices = await BleakScanner.discover(timeout=8.0)
    print("[SCAN RESULT]")
    for device in devices:
        print(f"  name={device.name!r} address={device.address}")

    if target_mac:
        wanted = target_mac.upper()
        for device in devices:
            if device.address.upper() == wanted:
                print("[MATCH] by MAC")
                return device

    for device in devices:
        if device.name == target_name:
            print("[MATCH] by advertised name")
            return device

    raise RuntimeError(f"Device not found: name={target_name!r} mac={target_mac!r}")


async def request_json(client: BleakClient, queue: asyncio.Queue, *, method: str, payload: bytes, msg_id: str, timeout: float, force_framed: bool, chunk_size: int) -> dict[str, Any]:
    print()
    if len(payload) <= 2000:
        print(f"[SEND JSON] {payload.decode('utf-8', errors='replace')}")
    else:
        print(f"[SEND JSON] large payload omitted, len={len(payload)}")
    print(f"[SEND LEN] {len(payload)}")

    send_meta = await write_payload_auto(client, payload, force_framed=force_framed, chunk_size=chunk_size)
    deadline = time.monotonic() + timeout
    unrelated = 0

    while time.monotonic() < deadline:
        try:
            obj = await asyncio.wait_for(queue.get(), timeout=max(0.1, deadline - time.monotonic()))
        except asyncio.TimeoutError:
            break

        print(f"[NOTIFY JSON] {json.dumps(obj, ensure_ascii=False, separators=(',', ':'))}")
        if obj.get("msgId") == msg_id and obj.get("method") == method:
            obj["_send_meta"] = send_meta
            return obj
        unrelated += 1

    raise TimeoutError(f"Timeout waiting for method={method!r} msgId={msg_id!r}. unrelated_messages={unrelated} send_meta={send_meta}")


async def get_dev_sta(client: BleakClient, queue: asyncio.Queue, *, force_framed: bool, chunk_size: int) -> dict[str, Any]:
    payload, msg_id, _ = cmd_get_dev_sta()
    return await request_json(client, queue, method="getDevSta", payload=payload, msg_id=msg_id, timeout=8.0, force_framed=force_framed, chunk_size=chunk_size)


async def get_config_file(client: BleakClient, queue: asyncio.Queue, *, pid: str, uid: str, pcode: Optional[int], chunk_size: int) -> dict[str, Any]:
    payload, msg_id, _ = cmd_get_config_file(pid, uid, pcode)
    return await request_json(client, queue, method="getConfigFile", payload=payload, msg_id=msg_id, timeout=12.0, force_framed=False, chunk_size=chunk_size)


def get_config_file_body(config_response: dict[str, Any]) -> dict[str, Any]:
    return config_response.get("data", {}).get("configFile", {})


def get_dev_state(dev_sta: dict[str, Any], device_name: str) -> dict[str, Any]:
    data = dev_sta.get("data", {})
    if "device" in data:
        data = data["device"]
    return data.get(device_name, {})


def get_config_device(config_response: dict[str, Any], device_name: str) -> dict[str, Any]:
    return get_config_file_body(config_response).get("device", {}).get(device_name, {})


def save_config_backup(config_response: dict[str, Any], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = output_dir / f"ggs_config_backup_{int(time.time())}.json"
    filename.write_text(json.dumps(config_response, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[BACKUP] Saved to {filename}")
    return filename


def validate_config_file_complete(config_file: dict[str, Any]) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if not isinstance(config_file, dict):
        return False, ["configFile is not an object"]

    device = config_file.get("device")
    if not isinstance(device, dict):
        errors.append("missing device object")
        return False, errors

    for name in ("light", "fan", "blower"):
        section = device.get(name)
        if not isinstance(section, dict):
            errors.append(f"missing device.{name}")
        elif len(section) < 2:
            errors.append(f"device.{name} looks partial")

    if len(compact_json_bytes(config_file)) < 180:
        errors.append("configFile is suspiciously short")

    if set(config_file.keys()) == {"device"}:
        errors.append("configFile contains only device; refusing to write a partial config")

    return len(errors) == 0, errors


def normalize_on_off_level(device_name: str, *, on: bool, level: Optional[int], previous_level: Optional[int]) -> tuple[int, int]:
    if not on:
        return 0, 0

    chosen = level if level is not None else previous_level
    if chosen is None or int(chosen) <= 0:
        chosen = DEFAULT_LEVELS[device_name]

    if device_name == "fan":
        chosen = max(1, min(10, int(chosen)))
    else:
        chosen = max(0, min(100, int(chosen)))

    return 1, int(chosen)


def build_updated_config_for_device(config_response: dict[str, Any], dev_sta: dict[str, Any], *, device_name: str, on: bool, level: Optional[int]) -> dict[str, Any]:
    config_file = copy.deepcopy(get_config_file_body(config_response))
    ok, errors = validate_config_file_complete(config_file)
    if not ok:
        raise RuntimeError("Unsafe configFile; refusing setConfigFile: " + "; ".join(errors))

    current_cfg = config_file["device"].get(device_name)
    current_sta = get_dev_state(dev_sta, device_name)
    if not isinstance(current_cfg, dict):
        raise RuntimeError(f"Missing or invalid device.{device_name}")

    previous_level = current_cfg.get("mLevel", current_cfg.get("maxSpeed", current_sta.get("level")))
    on_int, target_level = normalize_on_off_level(device_name, on=on, level=level, previous_level=previous_level)

    if device_name == "fan":
        if target_level <= 0:
            current_cfg["mOnOff"] = 0
        else:
            current_cfg["mOnOff"] = on_int
            current_cfg["mLevel"] = target_level
    elif device_name == "blower":
        current_cfg["mOnOff"] = on_int
        if "maxSpeed" in current_cfg:
            current_cfg["maxSpeed"] = target_level
        if "mLevel" in current_cfg:
            current_cfg["mLevel"] = target_level
        if "maxSpeed" not in current_cfg and "mLevel" not in current_cfg:
            current_cfg["maxSpeed"] = target_level
    else:
        current_cfg["modeType"] = 0
        current_cfg["mOnOff"] = on_int
        current_cfg["mLevel"] = target_level

    return config_file


async def set_device_via_config_file(
    client: BleakClient,
    queue: asyncio.Queue,
    *,
    device_name: str,
    on: bool,
    level: Optional[int],
    pid: str,
    uid: str,
    pcode: Optional[int],
    output_dir: Path,
    rollback_on_fail: bool,
    chunk_size: int,
) -> dict[str, Any]:
    before_sta = await get_dev_sta(client, queue, force_framed=False, chunk_size=chunk_size)
    before_cfg = await get_config_file(client, queue, pid=pid, uid=uid, pcode=pcode, chunk_size=chunk_size)
    save_config_backup(before_cfg, output_dir)

    updated_config_file = build_updated_config_for_device(before_cfg, before_sta, device_name=device_name, on=on, level=level)
    payload, msg_id, _ = cmd_set_config_file(updated_config_file, pid=pid, uid=uid, pcode=pcode)

    ack = await request_json(client, queue, method="setConfigFile", payload=payload, msg_id=msg_id, timeout=18.0, force_framed=False, chunk_size=chunk_size)
    await asyncio.sleep(3.0)

    after_sta = await get_dev_sta(client, queue, force_framed=False, chunk_size=chunk_size)
    after_cfg = await get_config_file(client, queue, pid=pid, uid=uid, pcode=pcode, chunk_size=chunk_size)

    before_state = get_dev_state(before_sta, device_name)
    after_state = get_dev_state(after_sta, device_name)
    after_config = get_config_device(after_cfg, device_name)

    expected_on = 1 if on else 0
    expected_level = level if on and level is not None else None
    success_on = after_config.get("mOnOff", after_state.get("on")) == expected_on
    success_level = True if expected_level is None else after_config.get("mLevel", after_config.get("maxSpeed", after_state.get("level"))) == expected_level
    success = bool(success_on and success_level)

    rollback_ack = None
    if rollback_on_fail and not success:
        original = get_config_file_body(before_cfg)
        payload, msg_id, _ = cmd_set_config_file(original, pid=pid, uid=uid, pcode=pcode)
        rollback_ack = await request_json(client, queue, method="setConfigFile", payload=payload, msg_id=msg_id, timeout=18.0, force_framed=False, chunk_size=chunk_size)

    return {
        "device": device_name,
        "strategy": "config_file_full_preserved",
        "target_on": expected_on,
        "target_level": expected_level,
        "before_state": before_state,
        "ack": ack,
        "after_state": after_state,
        "after_config": after_config,
        "success": success,
        "rollback_ack": rollback_ack,
    }


async def run(args: argparse.Namespace) -> None:
    pid = args.pid or os.getenv("GGS_PID")
    uid = args.uid or os.getenv("GGS_UID")
    pcode = args.pcode if args.pcode is not None else int(os.getenv("GGS_PCODE", "1004"))
    if not pid or not uid:
        raise SystemExit("Missing device identifiers. Provide --pid and --uid, or set GGS_PID and GGS_UID.")

    device = await find_device(args.target_name, args.target_mac or os.getenv("GGS_TARGET_MAC"))
    parser = NotifyParser(verbose=not args.quiet_frames)
    queue: asyncio.Queue = asyncio.Queue()

    def on_notify(sender: int, data: bytearray) -> None:
        raw = bytes(data)
        if not args.quiet_raw:
            print(f"[NOTIFY RAW] {raw.hex(' ')}")
        for obj in parser.feed(raw):
            queue.put_nowait(obj)

    async with BleakClient(device) as client:
        print("[CONNECT] connected")
        await client.start_notify(NOTIFY_UUID, on_notify)
        print("[NOTIFY] subscribed")

        try:
            if args.action == "get-dev-sta":
                result = await get_dev_sta(client, queue, force_framed=args.force_framed, chunk_size=args.chunk_size)
            elif args.action == "get-config-file":
                result = await get_config_file(client, queue, pid=pid, uid=uid, pcode=pcode, chunk_size=args.chunk_size)
            else:
                result = await set_device_via_config_file(
                    client,
                    queue,
                    device_name=args.device,
                    on=args.on,
                    level=args.level,
                    pid=pid,
                    uid=uid,
                    pcode=pcode,
                    output_dir=Path(args.output_dir),
                    rollback_on_fail=not args.no_rollback_on_fail,
                    chunk_size=args.chunk_size,
                )

            print("\n[RESULT]")
            print(json.dumps(result, ensure_ascii=False, indent=2))
        finally:
            await client.stop_notify(NOTIFY_UUID)
            print("[NOTIFY] stopped")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BLE test tool for a Spider Farmer / GGS controller.")
    parser.add_argument("--target-name", default=os.getenv("GGS_TARGET_NAME", DEFAULT_TARGET_NAME))
    parser.add_argument("--target-mac", default=None, help="Optional BLE MAC. Prefer leaving it unset in public logs.")
    parser.add_argument("--pid", default=None, help="Device PID. Can also be set with GGS_PID.")
    parser.add_argument("--uid", default=None, help="Device UID. Can also be set with GGS_UID.")
    parser.add_argument("--pcode", type=int, default=None, help="Device pcode. Can also be set with GGS_PCODE.")
    parser.add_argument("--chunk-size", type=int, default=AAAA_TX_CHUNK_SIZE_DEFAULT)
    parser.add_argument("--force-framed", action="store_true", help="Force getDevSta to be sent using AA AA framing.")
    parser.add_argument("--quiet-raw", action="store_true")
    parser.add_argument("--quiet-frames", action="store_true")
    parser.add_argument("--output-dir", default="captures")

    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("get-dev-sta")
    sub.add_parser("get-config-file")

    set_parser = sub.add_parser("set", help="Set light/fan/blower through full setConfigFile.")
    set_parser.add_argument("--device", choices=["light", "fan", "blower"], required=True)
    state = set_parser.add_mutually_exclusive_group(required=True)
    state.add_argument("--on", action="store_true")
    state.add_argument("--off", action="store_true")
    set_parser.add_argument("--level", type=int, default=None)
    set_parser.add_argument("--no-rollback-on-fail", action="store_true")

    args = parser.parse_args()
    if getattr(args, "off", False):
        args.on = False
    return args


if __name__ == "__main__":
    asyncio.run(run(parse_args()))
