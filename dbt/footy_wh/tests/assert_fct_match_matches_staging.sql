-- fails if fct_match has a different number of rows than staging
with f as (select count(*) as n from {{ ref('fct_match') }}),
     s as (select count(*) as n from {{ ref('stg_matches') }})
select f.n as fct_rows, s.n as stg_rows
from f, s
where f.n != s.n
