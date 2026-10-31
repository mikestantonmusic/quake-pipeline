-- One row per real earthquake: non-earthquakes (blasts, ice quakes) and deleted events are excluded.

with events as (

    select *
    from {{ ref('stg_earthquakes') }}
    where event_type = 'earthquake'
      and not is_deleted

),

with_place_region as (

    select
        *,
        -- "57 km NNW of Kota Belud, Malaysia" -> Malaysia; "Banda Sea" has no comma, keep it whole.
        case
            when place is null then 'Unknown'
            when place like '%,%' then trim(regexp_replace(place, '^.*,', ''))
            else place
        end as place_region
    from events

)

select
    e.event_id,
    e.occurred_at,
    (e.occurred_at at time zone 'UTC')::date as occurred_date,
    e.updated_at,
    e.mag,
    e.mag_type,
    case
        when e.mag is null then 'unknown'
        when e.mag < 3 then 'micro'
        when e.mag < 4 then 'minor'
        when e.mag < 5 then 'light'
        when e.mag < 6 then 'moderate'
        when e.mag < 7 then 'strong'
        when e.mag < 8 then 'major'
        else 'great'
    end as mag_class,
    e.depth_km,
    case
        when e.depth_km is null then 'unknown'
        when e.depth_km < 70 then 'shallow'
        when e.depth_km < 300 then 'intermediate'
        else 'deep'
    end as depth_class,
    e.latitude,
    e.longitude,
    e.place,
    coalesce(a.region, e.place_region) as region,
    e.status,
    e.alert,
    e.has_tsunami_flag,
    e.sig,
    e.sig >= 600 as is_significant,
    e.felt_reports,
    e.cdi,
    e.mmi,
    e.network,
    e.url,
    e.n_versions
from with_place_region as e
left join {{ ref('region_aliases') }} as a
    on e.place_region = a.alias
