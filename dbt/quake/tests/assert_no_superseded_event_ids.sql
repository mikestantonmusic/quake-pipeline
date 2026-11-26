-- Fails if a quake appears twice in fct_earthquakes under an old and a new USGS ID: a row
-- whose event_id is listed in the all_ids of a row updated after it (see stg_earthquakes).
--
-- A listing by an *older* row doesn't count: USGS sometimes associates two events and later
-- separates them again. The separated event is then updated after the listing and USGS
-- still returns it under its own ID (e.g. aka2026vzfkmq, listed by aka2026vzfkqp).

select
    old_row.event_id as old_event_id,
    new_row.event_id as listed_by
from {{ ref('fct_earthquakes') }} as old_row
inner join {{ ref('fct_earthquakes') }} as new_row
    on
        old_row.event_id <> new_row.event_id
        and old_row.updated_at < new_row.updated_at
        and contains(new_row.all_ids, ',' || old_row.event_id || ',')
