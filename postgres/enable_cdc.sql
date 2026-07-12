-- Phase 2, Task 2.1: publication the Debezium PostgreSQL source connector
-- (plugin.name=pgoutput) replicates from. Run once, after init.sql, and
-- after confirming `SHOW wal_level;` returns `logical`.

CREATE PUBLICATION dbz_publication FOR TABLE iot.iot_events;
