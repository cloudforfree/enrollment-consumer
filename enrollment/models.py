"""Message parsing and validation module"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone

_IMEI_RE = re.compile(r"^\d{15}$")


class MalformedMessage(ValueError):
    """The SQS body can never be processed successfully, however often it is retried."""


@dataclass(frozen=True)
class EnrollmentRequest:
    tenant_id: str
    imei: str
    device_type: str
    requested_at: datetime


def _required_str(data: dict, key: str, max_len: int) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise MalformedMessage(f"'{key}' must be a non-empty string")
    if len(value) > max_len:
        raise MalformedMessage(f"'{key}' exceeds {max_len} characters")

    if any(ord(ch) < 32 for ch in value):
        raise MalformedMessage(f"'{key}' contains control characters")
    return value


def parse_body(body: object) -> EnrollmentRequest:
    if not isinstance(body, str):
        raise MalformedMessage("body must be a string")
    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        raise MalformedMessage(f"body is not valid JSON: {exc.msg}") from exc
    if not isinstance(data, dict):
        raise MalformedMessage("body must be a JSON object")

    tenant_id = _required_str(data, "tenant_id", 128)
    imei = _required_str(data, "imei", 32)
    if not _IMEI_RE.match(imei):
        raise MalformedMessage("'imei' must be exactly 15 digits")
    device_type = _required_str(data, "device_type", 64)

    raw_ts = _required_str(data, "requested_at", 64)
    try:
        requested_at = datetime.fromisoformat(raw_ts)
    except ValueError as exc:
        raise MalformedMessage("'requested_at' must be an ISO-8601 timestamp") from exc
    if requested_at.tzinfo is None:
        requested_at = requested_at.replace(tzinfo=timezone.utc)

    return EnrollmentRequest(tenant_id, imei, device_type, requested_at)
