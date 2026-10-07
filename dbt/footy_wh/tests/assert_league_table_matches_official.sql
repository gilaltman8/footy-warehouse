-- the computed 24/25 Premier League table must equal the published one, row for row (position incl. tie-breaks)
with mine as (
    select
        t.team_name,
        l.position,
        l.points,
        l.goal_difference
    from {{ ref('league_table') }} as l
    inner join {{ ref('dim_team') }} as t on l.team_key = t.team_key
    where l.season = '2425' and l.div_code = 'E0'
),

official as (
    select
        team_name,
        position,
        points,
        goal_difference
    from {{ ref('pl_2425_official') }}
)

select
    coalesce(m.team_name, o.team_name) as team_name,
    m.position as my_position,
    o.position as official_position,
    m.points as my_points,
    o.points as official_points
from mine as m
full outer join official as o on m.team_name = o.team_name
where
    m.team_name is null or o.team_name is null
    or m.position != o.position or m.points != o.points or m.goal_difference != o.goal_difference
