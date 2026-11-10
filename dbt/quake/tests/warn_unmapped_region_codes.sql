{{ config(severity='warn') }}

-- Warns about short region names (likely codes such as WA or BC) that region_aliases
-- doesn't expand yet. Fix by adding a row to seeds/region_aliases.csv.

select region, count(*) as n_earthquakes
from {{ ref('fct_earthquakes') }}
where length(region) <= 3
group by region
