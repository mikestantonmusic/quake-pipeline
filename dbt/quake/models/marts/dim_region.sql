-- One row per region, with lifetime stats.

select
    region,
    count(*) as n_earthquakes,
    max(mag) as max_mag,
    min(occurred_at) as first_earthquake_at,
    max(occurred_at) as last_earthquake_at
from {{ ref('fct_earthquakes') }}
group by region
