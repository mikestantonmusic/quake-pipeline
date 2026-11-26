{{ config(materialized='table') }}

-- One row per event: its latest version, flattened from JSON into typed columns.
--
-- Rebuilt in full from stg_earthquake_versions on every run. Picking the latest version
-- across all stored versions means an older version loaded late (e.g. by a backfill) can
-- never replace a newer one. Unlike the incremental Postgres version, a full rebuild needs
-- no lookback logic, and at tens of thousands of rows it takes Snowflake a second or two.
--
-- USGS can also change an event's ID: when another network becomes the preferred source,
-- e.g. aka2026tsyezt becomes us7000ttd7. The new event lists every ID it has had in
-- properties.ids (",aka2026tsyezt,us7000ttd7,"). An event whose ID appears in the ids of a
-- newer event is the same quake under an old ID, so it is dropped here.

with latest as (

    select
        *,
        count(*) over (partition by event_id) as n_versions
    from {{ ref('stg_earthquake_versions') }}
    qualify row_number() over (partition by event_id order by updated_ms desc) = 1

),

old_ids as (

    select
        alias.value::string as old_event_id,
        latest.updated_ms as listed_at_ms
    from latest,
        lateral split_to_table(trim(latest.feature:properties:ids::string, ','), ',') as alias
    where alias.value::string <> latest.event_id

),

current_ids as (

    select latest.*
    from latest
    where not exists (
        select 1
        from old_ids
        where
            old_ids.old_event_id = latest.event_id
            and old_ids.listed_at_ms > latest.updated_ms
    )

)

select
    event_id,
    feature:properties:ids::string as all_ids,
    feature:properties:mag::number(4, 2) as mag,
    feature:properties:magType::string as mag_type,
    feature:properties:place::string as place,
    feature:properties:type::string as event_type,
    feature:properties:status::string as status,
    feature:properties:alert::string as alert,
    coalesce(feature:properties:tsunami::int = 1, false) as has_tsunami_flag,
    feature:properties:sig::int as sig,
    feature:properties:felt::int as felt_reports,
    feature:properties:cdi::number(4, 1) as cdi,
    feature:properties:mmi::number(4, 2) as mmi,
    feature:properties:net::string as network,
    feature:properties:url::string as url,
    -- Epoch milliseconds -> a UTC timestamp, independent of the session's time zone.
    convert_timezone('UTC', to_timestamp_ltz(feature:properties:time::number, 3)) as occurred_at,
    convert_timezone('UTC', to_timestamp_ltz(updated_ms, 3)) as updated_at,
    feature:geometry:coordinates[0]::float as longitude,
    feature:geometry:coordinates[1]::float as latitude,
    feature:geometry:coordinates[2]::float as depth_km,
    coalesce(feature:properties:status::string, '') = 'deleted' as is_deleted,
    n_versions,
    source,
    loaded_at
from current_ids
