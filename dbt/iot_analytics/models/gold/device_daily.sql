-- Task 2.3 gold model: daily aggregates per device, consumed by the
-- Streamlit "top-N devices" chart (Task 2.4).
with validated as (
    select * from {{ ref('iot_validated') }}
)

select
    device_id,
    cast(event_ts as date) as event_date,
    count(*) as event_count,
    avg(latitude) as avg_latitude,
    avg(longitude) as avg_longitude,
    min(event_ts) as first_event_ts,
    max(event_ts) as last_event_ts,
    sum(case when severity != 'ok' then 1 else 0 end) as flagged_event_count
from validated
group by device_id, cast(event_ts as date)
