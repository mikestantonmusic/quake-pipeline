-- Fails if the daily aggregate doesn't add up to the fact table.

select a.n as agg_total, f.n as fct_total
from (select sum(n_earthquakes) as n from {{ ref('agg_daily_by_region') }}) as a,
     (select count(*) as n from {{ ref('fct_earthquakes') }}) as f
where a.n <> f.n
