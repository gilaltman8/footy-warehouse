with tm as (
    select * from {{ ref('fct_team_match') }}
),

windowed as (
    select
        team_match_key,
        match_key,
        team_key,
        match_date,
        season,
        div_code,
        is_home,
        goals_for,
        goals_against,
        points,
        result,
        -- form BEFORE this match: previous 5, excluding the current row
        sum(points) over form5 as points_last_5,
        sum(goals_for) over form5 as goals_for_last_5,
        sum(goals_against) over form5 as goals_against_last_5,
        count(*) over form5 as matches_in_window,
        -- season to date INCLUDING this match
        sum(points) over season_to_date as season_points,
        sum(goals_for - goals_against) over season_to_date as season_gd,
        row_number() over season_order as matches_played_season
    from tm
    window
        form5 as (
            partition by team_key order by match_date, match_key
            rows between 5 preceding and 1 preceding
        ),
        season_to_date as (
            partition by team_key, season, div_code order by match_date, match_key
            rows between unbounded preceding and current row
        ),
        season_order as (partition by team_key, season, div_code order by match_date, match_key)
),

ranked as (
    select
        *,
        rank() over (
            partition by season, div_code, match_date
            order by season_points desc, season_gd desc
        ) as position_on_date
    from windowed
)

select * from ranked
