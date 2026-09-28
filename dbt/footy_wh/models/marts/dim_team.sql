with names as (
    select home_team as team_name from {{ ref('stg_matches') }}
    union distinct
    select away_team from {{ ref('stg_matches') }}
),
first_seen as (
    select
        team_name,
        min(match_date) as first_match_date,
        max(match_date) as last_match_date
    from (
        select home_team as team_name, match_date from {{ ref('stg_matches') }}
        union all
        select away_team, match_date from {{ ref('stg_matches') }}
    )
    group by 1
)
select
    {{ dbt_utils.generate_surrogate_key(['n.team_name']) }} as team_key,
    n.team_name,
    f.first_match_date,
    f.last_match_date
from names n
join first_seen f using (team_name)
