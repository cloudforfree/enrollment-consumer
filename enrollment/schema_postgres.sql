CREATE TABLE IF NOT EXISTS enrollments (
    tenant_id    text        NOT NULL,
    imei         text        NOT NULL,
    device_type  text        NOT NULL,
    requested_at timestamptz NOT NULL,
    status       text        NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'released')),
    enrolled_at  timestamptz NOT NULL DEFAULT now(),
    released_at  timestamptz,
    PRIMARY KEY (tenant_id, imei)
);

CREATE INDEX IF NOT EXISTS enrollments_active_idx
    ON enrollments (tenant_id) WHERE status = 'active';
