-- Thin 1:1 cast over the Bronze table; no filtering here, validation lives
-- in the silver model (Task 2.3).
select
    id::number as event_id,
    device_id::varchar as device_id,
    latitude::float as latitude,
    longitude::float as longitude,
    event_ts::timestamp_ntz as event_ts,
    ingested_at::timestamp_ntz as ingested_at
from {{ source('raw', 'iot_events') }}
