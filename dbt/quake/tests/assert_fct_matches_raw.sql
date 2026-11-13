-- Fails if fct_earthquakes doesn't have exactly one row per non-deleted earthquake in raw,
-- judged by each event's latest version. Catches incremental drift in stg_earthquakes.
--
-- Raw is compared only up to the newest row staging has seen. Otherwise a load from another
-- DAG run landing between this run's model build and its tests would fail the test even
-- though the build was correct; those rows are picked up by the next run.

with watermark as (

    select max(loaded_at) as loaded_at from {{ ref('stg_earthquakes') }}

),

latest as (

    select distinct on (event_id)
        payload -> 'properties' ->> 'type' as event_type,
        payload -> 'properties' ->> 'status' as status
    from {{ source('usgs', 'earthquake_events') }}
    where loaded_at <= (select loaded_at from watermark)
    order by event_id, updated_ms desc

),

expected as (

    select count(*) as n
    from latest
    where event_type = 'earthquake'
      and coalesce(status, '') <> 'deleted'

),

actual as (

    select count(*) as n from {{ ref('fct_earthquakes') }}

)

select expected.n as expected, actual.n as actual
from expected, actual
where expected.n <> actual.n
