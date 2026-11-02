import json
from typing import Any


def body(imei: str, tenant_id: str = "test-int", **overrides: Any) -> dict:
    payload = {
        "imei": imei,
        "tenant_id": tenant_id,
        "device_type": "smartphone",
        "requested_at": "2026-08-19T10:00:00Z",
    }
    payload.update(overrides)
    return payload


def record(message_id: str, payload: Any) -> dict:
    """A record shaped like what the Lambda SQS trigger delivers."""
    raw = payload if isinstance(payload, str) else json.dumps(payload)
    return {
        "messageId": message_id,
        "receiptHandle": f"handle-{message_id}",
        "body": raw,
        "attributes": {
            "ApproximateReceiveCount": "1",
            "SentTimestamp": "1787133600000",
            "SenderId": "AIDAEXAMPLE",
            "ApproximateFirstReceiveTimestamp": "1787133600001",
        },
        "messageAttributes": {},
        "md5OfBody": "ignored",
        "eventSource": "aws:sqs",
        "eventSourceARN": "arn:aws:sqs:eu-west-1:123456789012:device-enrollment",
        "awsRegion": "eu-west-1",
    }


def event(*records: dict) -> dict:
    return {"Records": list(records)}


def failed_ids(response: dict) -> list:
    return [f["itemIdentifier"] for f in response["batchItemFailures"]]


def imei(n: int) -> str:
    """Deterministic 15-digit IMEI-shaped string."""
    return f"{n:015d}"
