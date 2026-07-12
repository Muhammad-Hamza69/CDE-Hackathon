-- On-prem-simulation IoT schema.
-- Matches the AWS IoT Device Simulator geoLocation template: device_id, latitude,
-- longitude, timestamp. Written by the Kafka Connect JDBC Sink Connector
-- (topic: iot-events); read by the Debezium CDC source connector in Phase 2.

CREATE SCHEMA IF NOT EXISTS iot;

CREATE TABLE IF NOT EXISTS iot.iot_events (
    id           BIGSERIAL PRIMARY KEY,
    device_id    VARCHAR(64) NOT NULL,
    latitude     DOUBLE PRECISION NOT NULL,
    longitude    DOUBLE PRECISION NOT NULL,
    event_ts     TIMESTAMPTZ NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_iot_events_device_ts ON iot.iot_events (device_id, event_ts);

-- Debezium (Task 2.1) needs full before/after row images for UPDATE/DELETE events.
ALTER TABLE iot.iot_events REPLICA IDENTITY FULL;
