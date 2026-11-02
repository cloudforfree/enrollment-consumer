"""AWS Lambda entry point for the SQS enrollment consumer.

The event source mapping MUST enable `ReportBatchItemFailures`.
Environment:
  DATABASE_URL       postgresql://... (sqlite:///path is accepted for local tests)
  ENROLLMENT_LIMIT   max active devices per tenant (default 5)
"""
from __future__ import annotations

import logging
import os
import threading
from typing import Any

from .models import MalformedMessage, parse_body
from .store import EnrollmentStore, Outcome

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_LIMIT = 5

_local = threading.local()  # one connection per thread; in Lambda that is exactly one


def create_store(url: str) -> EnrollmentStore:
    if url.startswith("sqlite:///"):
        print('testing sqlite')
        from .sqlite_store import SqliteStore

        return SqliteStore(url[len("sqlite:///"):])
    print('testing postgres')
    from .postgres_store import PostgresStore

    return PostgresStore(url)


def get_store() -> EnrollmentStore:
    store = getattr(_local, "store", None)
    if store is None:
        url = os.environ.get("DATABASE_URL")
        if not url:
            raise RuntimeError("DATABASE_URL is not set")
        store = _local.store = create_store(url)
    return store


def close_store() -> None:
    """Close this thread's cached connection (tests; Lambda just lets the sandbox die)."""
    store = getattr(_local, "store", None)
    _local.store = None
    if store is not None:
        store.close()


def get_limit() -> int:
    raw = os.environ.get("ENROLLMENT_LIMIT", str(DEFAULT_LIMIT))
    try:
        limit = int(raw)
    except ValueError:
        raise ValueError(f"ENROLLMENT_LIMIT must be an integer, got {raw!r}") from None
    if limit < 1:
        raise ValueError(f"ENROLLMENT_LIMIT must be >= 1, got {limit}")
    return limit


def _mask(imei: str) -> str:
    return "***" + imei[-4:]


def process_event(event: dict[str, Any], store: EnrollmentStore, limit: int) -> dict[str, Any]:
    failures: list[str] = []
    for record in event.get("Records", []):
        message_id = record["messageId"]
        try:
            request = parse_body(record.get("body"))
            outcome = store.reserve(request, limit)
        except MalformedMessage as exc:
            logger.warning("malformed message %s: %s", message_id, exc)
            failures.append(message_id)
            continue
        except Exception:  # noqa: BLE001 - any DB/transient error must fail only this record
            logger.exception("error processing message %s", message_id)
            failures.append(message_id)
            continue

        if outcome is Outcome.LIMIT_EXCEEDED:
            logger.warning(
                "tenant %s at limit %d; rejecting device %s (message %s)",
                request.tenant_id, limit, _mask(request.imei), message_id,
            )
            failures.append(message_id)
        else:
            logger.info(
                "%s tenant=%s device=%s message=%s",
                outcome.value, request.tenant_id, _mask(request.imei), message_id,
            )
    return {"batchItemFailures": [{"itemIdentifier": mid} for mid in failures]}


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    return process_event(event, get_store(), get_limit())
