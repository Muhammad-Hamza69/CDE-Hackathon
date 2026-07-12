-- Task 2.3 silver model: validate nulls, parse timestamps (done in staging),
-- add a severity tag. Rows with a null device_id or event_ts are dropped
-- outright (they can't be attributed to anything downstream); everything
-- else is kept and tagged so gold/Streamlit can decide how to treat it.
--
-- severity rules (documented here since the PDF doesn't specify exact
-- thresholds):
--   out_of_bounds - lat/long outside valid WGS84 ranges
--   stale         - ingested more than 60 minutes after the event fired
--   ok            - passes both checks
with staged as (
    select * from {{ ref('stg_iot_events') }}
),

filtered as (
    select *
    from staged
    where device_id is not null
      and event_ts is not null
),

tagged as (
    select
        event_id,
        device_id,
        latitude,
        longitude,
        event_ts,
        ingested_at,
        case
            when latitude is null or longitude is null
                or latitude < -90 or latitude > 90
                or longitude < -180 or longitude > 180
                then 'out_of_bounds'
            when datediff('minute', event_ts, ingested_at) > 60 then 'stale'
            else 'ok'
        end as severity
    from filtered
)

select * from tagged
