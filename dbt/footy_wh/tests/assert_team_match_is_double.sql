-- the unpivot must produce exactly two rows per match
select 'mismatch' as issue
from (select count(*) n from {{ ref('fct_team_match') }}) t,
     (select count(*) n from {{ ref('fct_match') }}) m
where t.n != 2 * m.n
