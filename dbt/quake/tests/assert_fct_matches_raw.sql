-- Fails if fct_earthquakes doesn't have exactly one row per non-deleted earthquake in raw,
-- judged by each event's latest valid version. Re-derives everything from the raw files,
-- independently of the staging models, so it catches incremental drift in
-- stg_earthquake_versions as well as logic errors further down.
--
-- Raw is compared only up to the newest file staging has seen. Otherwise a COPY landing
-- between this run's model build and its tests would fail the test even though the build
-- was correct; those files are picked up by the next run.

with watermark as (

    select max(versions.loaded_at) as loaded_at
    from {{ ref('stg_earthquake_versions') }} as versions

),

raw_features as (

    select
        feature.value:id::string as event_id,
        feature.value:properties:updated::number as updated_ms,
        feature.value:properties:ids::string as all_ids,
        feature.value:properties:type::string as event_type,
        feature.value:properties:status::string as status
    from {{ source('usgs', 'feed_files') }} as raw_files,
        lateral flatten(input => raw_files.payload:features) as feature
    where
        raw_files.loaded_at <= (select watermark.loaded_at from watermark)
        -- Same validity rules as stg_earthquake_versions.
        and feature.value:id is not null
        and feature.value:properties:updated is not null
        and feature.value:properties:time is not null
        and array_size(feature.value:geometry:coordinates) = 3
        and feature.value:geometry:coordinates[0]::float between -180 and 180
        and feature.value:geometry:coordinates[1]::float between -90 and 90

),

latest as (

    select
        raw_features.event_id,
        raw_features.updated_ms,
        raw_features.all_ids,
        raw_features.event_type,
        raw_features.status
    from raw_features
    qualify row_number() over (
        partition by raw_features.event_id order by raw_features.updated_ms desc
    ) = 1

),

expected as (

    -- Same rule as stg_earthquakes: skip events listed under ids by a newer event.
    select count(*) as n
    from latest
    where
        latest.event_type = 'earthquake'
        and coalesce(latest.status, '') <> 'deleted'
        and not exists (
            select 1
            from latest as newer
            where
                newer.event_id <> latest.event_id
                and newer.updated_ms > latest.updated_ms
                and contains(newer.all_ids, ',' || latest.event_id || ',')
        )

),

actual as (

    select count(*) as n from {{ ref('fct_earthquakes') }}

)

select
    expected.n as expected,
    actual.n as actual
from expected, actual
where expected.n <> actual.n
