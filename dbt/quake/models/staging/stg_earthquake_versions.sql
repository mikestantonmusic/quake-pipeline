{{ config(materialized='incremental', incremental_strategy='append') }}

-- One row per event version: every feature of every raw file, kept once per
-- (event_id, updated_ms). Hourly feeds overlap, so most features in a file are versions
-- already stored; only versions not seen before are added. This does the job of the
-- Postgres loader's ON CONFLICT DO NOTHING.

with features as (

    select
        feature.value:id::string as event_id,
        feature.value:properties:updated::number as updated_ms,
        feature.value as feature,
        raw_files.source,
        raw_files.source_file,
        raw_files.loaded_at
    from {{ source('usgs', 'feed_files') }} as raw_files,
        lateral flatten(input => raw_files.payload:features) as feature

    {% if is_incremental() %}
        -- Files loaded since the last run, plus a lookback: a slow COPY's rows can become
        -- visible after a faster run already moved past them. Re-reading is harmless, since
        -- versions already stored are filtered out below.
        where raw_files.loaded_at > (
            select
                coalesce(max(current_versions.loaded_at), '1900-01-01'::timestamp_ltz)
                - interval '3 hours'
            from {{ this }} as current_versions
        )
    {% endif %}

),

valid as (

    -- The checks ingestion/schemas.py applies: an id, epoch-ms time and updated, and
    -- [lon, lat, depth] with lon and lat in range. Invalid features stay in raw.
    select *
    from features
    where
        event_id is not null
        and updated_ms is not null
        and feature:properties:time::number is not null
        and array_size(feature:geometry:coordinates) = 3
        and feature:geometry:coordinates[0]::float between -180 and 180
        and feature:geometry:coordinates[1]::float between -90 and 90

),

first_seen as (

    -- The same version usually appears in many files; keep the first one loaded.
    select *
    from valid
    qualify row_number() over (
        partition by event_id, updated_ms order by loaded_at asc, source_file asc
    ) = 1

)

select first_seen.*
from first_seen

{% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} as stored
        where
            stored.event_id = first_seen.event_id
            and stored.updated_ms = first_seen.updated_ms
    )
{% endif %}
