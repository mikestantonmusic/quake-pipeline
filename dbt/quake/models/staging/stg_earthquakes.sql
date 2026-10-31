{{ config(materialized='incremental', unique_key='event_id') }}

-- One row per event: its latest version, flattened from JSON into typed columns.

with versions as (

    select *
    from {{ source('usgs', 'earthquake_events') }}

    {% if is_incremental() %}
    -- Re-rank every stored version of any event with new rows, not just the new rows,
    -- so an older version loaded late (e.g. by a backfill) can't replace a newer one.
    where event_id in (
        select event_id
        from {{ source('usgs', 'earthquake_events') }}
        where loaded_at > (select coalesce(max(loaded_at), '-infinity') from {{ this }})
    )
    {% endif %}

),

ranked as (

    select
        *,
        row_number() over (partition by event_id order by updated_ms desc) as version_rank,
        count(*) over (partition by event_id) as n_versions
    from versions

),

flattened as (

    select
        event_id,
        (payload -> 'properties' ->> 'mag')::numeric as mag,
        payload -> 'properties' ->> 'magType' as mag_type,
        payload -> 'properties' ->> 'place' as place,
        payload -> 'properties' ->> 'type' as event_type,
        payload -> 'properties' ->> 'status' as status,
        payload -> 'properties' ->> 'alert' as alert,
        (payload -> 'properties' ->> 'tsunami')::int = 1 as has_tsunami_flag,
        (payload -> 'properties' ->> 'sig')::int as sig,
        (payload -> 'properties' ->> 'felt')::int as felt_reports,
        (payload -> 'properties' ->> 'cdi')::numeric as cdi,
        (payload -> 'properties' ->> 'mmi')::numeric as mmi,
        payload -> 'properties' ->> 'net' as network,
        payload -> 'properties' ->> 'url' as url,
        to_timestamp((payload -> 'properties' ->> 'time')::bigint / 1000.0) as occurred_at,
        to_timestamp(updated_ms / 1000.0) as updated_at,
        (payload -> 'geometry' -> 'coordinates' ->> 0)::numeric as longitude,
        (payload -> 'geometry' -> 'coordinates' ->> 1)::numeric as latitude,
        (payload -> 'geometry' -> 'coordinates' ->> 2)::numeric as depth_km,
        coalesce(payload -> 'properties' ->> 'status', '') = 'deleted' as is_deleted,
        n_versions,
        source,
        loaded_at
    from ranked
    where version_rank = 1

)

select * from flattened
