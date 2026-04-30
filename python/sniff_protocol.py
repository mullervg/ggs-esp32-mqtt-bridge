#!/usr/bin/env python3
"""
Passive-ish BLE notification sniffer for the GGS protocol.

The script subscribes to ff01 notifications and prints raw bytes plus decoded raw
JSON / AA AA fragmented JSON. It does not send control commands unless --probe is
used, which only sends getDevSta.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from typing import Any, Optional

from bleak import BleakClient, BleakScanner

NOTIFY_UUID = "0000ff01-0000-1000-8000-00805f9b34fb"
WRITE_UUID = "0000ff02-0000-1000-8000-00805f9b34fb"
DEFAULT_TARGET_NAME = "SF-GGS-CB"


def crc16_modbus(data: bytes, init: int = 0xFFFF) -> int:
    crc = init & 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
            crc &= 0xFFFF
    return crc & 0xFFFF


class NotifyParser:
    def __init__(self) -> None:
        self.buffer = bytearray()
        self.parts: dict[tuple[int, int, int, int, int], dict[str, Any]] = {}

    def feed(self, data: bytes) -> list[dict[str, Any]]:
        self.buffer.extend(data)
        out: list[dict[str, Any]] = []
        while self.buffer:
            if len(self.buffer) >= 2 and self.buffer[0:2] == b"\xAA\xAA":
                parsed = self._try_frame()
                if parsed is None:
                    break
                if parsed is not False:
                    out.append(parsed)
                continue
            parsed = self._try_raw_json()
            if parsed is None:
                break
            if parsed is not False:
                out.append(parsed)
        return out

    def _try_raw_json(self):
        while self.buffer and self.buffer[0] not in (ord("{"), ord("[")):
            del self.buffer[0]
        if not self.buffer:
            return False
        try:
            text = self.buffer.decode("utf-8")
            obj, end = json.JSONDecoder().raw_decode(text)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        del self.buffer[:len(text[:end].encode("utf-8"))]
        return {"transport": "raw_json", "payload": obj}

    def _try_frame(self):
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

        meta = {
            "transport": "aaaa_frame",
            "frame_type": f"0x{frame_type:04X}",
            "body_len": body_len,
            "frame_crc": f"0x{frame_crc:04X}",
            "crc_header_body": f"0x{crc16_modbus(header + body):04X}",
            "crc_type_len_body": f"0x{crc16_modbus(header[2:] + body):04X}",
        }

        if len(body) < 14:
            meta["body_hex"] = body.hex(" ")
            return meta

        field_a = body[0]
        field_b = body[1]
        payload_crc = int.from_bytes(body[2:4], "big")
        payload_total_len = int.from_bytes(body[4:8], "big")
        offset = int.from_bytes(body[8:12], "big")
        chunk_len = int.from_bytes(body[12:14], "big")
        chunk = body[14:14 + chunk_len]

        meta.update({
            "field_a": f"0x{field_a:02X}",
            "field_b": f"0x{field_b:02X}",
            "payload_crc": f"0x{payload_crc:04X}",
            "payload_total_len": payload_total_len,
            "offset": offset,
            "chunk_len": chunk_len,
        })

        key = (frame_type, field_a, field_b, payload_crc, payload_total_len)
        entry = self.parts.setdefault(key, {"data": bytearray(payload_total_len), "ranges": []})
        end = min(offset + len(chunk), payload_total_len)
        if end > offset:
            entry["data"][offset:end] = chunk[:end - offset]
            entry["ranges"].append((offset, end))

        if not self._ranges_cover(entry["ranges"], payload_total_len):
            meta["complete"] = False
            return meta

        payload = bytes(entry["data"])
        del self.parts[key]
        meta["complete"] = True
        meta["payload_crc_calc"] = f"0x{crc16_modbus(payload):04X}"
        try:
            meta["payload"] = json.loads(payload.decode("utf-8"))
        except Exception:
            meta["payload_hex"] = payload.hex(" ")
        return meta

    @staticmethod
    def _ranges_cover(ranges: list[tuple[int, int]], total_len: int) -> bool:
        merged: list[list[int]] = []
        for start, end in sorted(ranges):
            if not merged or start > merged[-1][1]:
                merged.append([start, end])
            else:
                merged[-1][1] = max(merged[-1][1], end)
        return len(merged) == 1 and merged[0][0] == 0 and merged[0][1] >= total_len


async def find_device(target_name: str, target_mac: Optional[str]) -> Any:
    devices = await BleakScanner.discover(timeout=8.0)
    for device in devices:
        print(f"[SCAN] name={device.name!r} address={device.address}")
    if target_mac:
        wanted = target_mac.upper()
        for device in devices:
            if device.address.upper() == wanted:
                return device
    for device in devices:
        if device.name == target_name:
            return device
    raise RuntimeError(f"Device not found: {target_name!r} / {target_mac!r}")


async def run(args: argparse.Namespace) -> None:
    device = await find_device(args.target_name, args.target_mac or os.getenv("GGS_TARGET_MAC"))
    parser = NotifyParser()

    def on_notify(sender: int, data: bytearray) -> None:
        raw = bytes(data)
        print(f"[RAW] {raw.hex(' ')}")
        for decoded in parser.feed(raw):
            print("[DECODED]")
            print(json.dumps(decoded, ensure_ascii=False, indent=2))

    async with BleakClient(device) as client:
        print("[CONNECT] connected")
        await client.start_notify(NOTIFY_UUID, on_notify)
        print("[NOTIFY] subscribed")

        if args.probe:
            payload = b'{"method":"getDevSta","params":{},"msgId":"probe"}'
            await client.write_gatt_char(WRITE_UUID, payload, response=True)
            print("[PROBE] getDevSta sent")

        print("[RUNNING] Press Ctrl+C to stop.")
        while True:
            await asyncio.sleep(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Decode GGS BLE notifications.")
    parser.add_argument("--target-name", default=os.getenv("GGS_TARGET_NAME", DEFAULT_TARGET_NAME))
    parser.add_argument("--target-mac", default=None, help="Optional BLE MAC. Prefer using the advertised name for public logs.")
    parser.add_argument("--probe", action="store_true", help="Send a getDevSta probe after subscribing.")
    return parser.parse_args()


if __name__ == "__main__":
    try:
        asyncio.run(run(parse_args()))
    except KeyboardInterrupt:
        print("\n[STOP]")
