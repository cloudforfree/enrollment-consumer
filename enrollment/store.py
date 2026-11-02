"""Storage contract shared by the Postgres and SQLite (for tests) backends."""
from __future__ import annotations

import enum
from typing import Protocol

from .models import EnrollmentRequest


class Outcome(enum.Enum):
    ENROLLED = "enrolled"              # a new slot was consumed
    DUPLICATE = "duplicate"            # device already active for tenant: safe no-op
    LIMIT_EXCEEDED = "limit_exceeded"  # tenant is full; nothing was written


class EnrollmentStore(Protocol):
    def reserve(self, request: EnrollmentRequest, limit: int) -> Outcome: ...

    def release(self, tenant_id: str, imei: str) -> bool:
        """De-enroll a device, freeing its slot. Returns False if it was not active."""

    def active_count(self, tenant_id: str) -> int: ...

    def init_schema(self) -> None: ...

    def truncate_all(self) -> None:
        """Test helper."""

    def close(self) -> None: ...
