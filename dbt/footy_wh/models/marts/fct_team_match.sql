{{ config(materialized='table', file_format='delta', liquid_clustered_by=['match_date', 'team_key']) }}

with base as (
    select * from {{ ref('fct_match') }}
),

home as (
    select
        match_key,
        match_date,
        season,
        div_code,
        home_team_key as team_key,
        away_team_key as opponent_key,
        true as is_home,
        home_goals as goals_for,
        away_goals as goals_against,
        home_points as points,
        home_shots as shots,
        home_shots_on_target as shots_on_target,
        home_corners as corners,
        home_yellows as yellows,
        home_reds as reds
    from base
),

away as (
    select
        match_key,
        match_date,
        season,
        div_code,
        away_team_key,
        home_team_key,
        false,
        away_goals,
        home_goals,
        away_points,
        away_shots,
        away_shots_on_target,
        away_corners,
        away_yellows,
        away_reds
    from base
)

select
    {{ dbt_utils.generate_surrogate_key(['match_key', 'team_key']) }} as team_match_key,
    *,
    case when points = 3 then 'W' when points = 1 then 'D' else 'L' end as result
from (
    select * from home
    union all
    select * from away
) as unioned
