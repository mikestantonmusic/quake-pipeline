-- One row per region per UTC day that had at least one earthquake.

select
    occurred_date,
    region,
    count(*) as n_earthquakes,
    max(mag) as max_mag,
    round(avg(mag), 2) as avg_mag,
    count_if(has_tsunami_flag) as n_tsunami_flags,
    count_if(is_significant) as n_significant
from {{ ref('fct_earthquakes') }}
group by occurred_date, region
