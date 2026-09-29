select
    season,
    div_code,
    team_key,
    count(*)                                   as played,
    count_if(result = 'W')                     as won,
    count_if(result = 'D')                     as drawn,
    count_if(result = 'L')                     as lost,
    sum(goals_for)                             as goals_for,
    sum(goals_against)                         as goals_against,
    sum(goals_for) - sum(goals_against)        as goal_difference,
    sum(points)                                as points,
    rank() over (
        partition by season, div_code
        order by sum(points) desc, sum(goals_for) - sum(goals_against) desc, sum(goals_for) desc
    )                                          as position
from {{ ref('fct_team_match') }}
group by 1, 2, 3
