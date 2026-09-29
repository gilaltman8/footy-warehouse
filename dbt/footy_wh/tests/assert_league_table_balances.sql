-- in any league-season, wins must equal losses and goals for must equal goals against
select season, div_code, sum(won) w, sum(lost) l, sum(goals_for) gf, sum(goals_against) ga
from {{ ref('league_table') }}
group by 1, 2
having sum(won) != sum(lost) or sum(goals_for) != sum(goals_against)
