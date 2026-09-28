{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'match_key',
    file_format = 'delta',
    liquid_clustered_by = ['match_date', 'div_code'],
    on_schema_change = 'append_new_columns'
) }}

select
    m.match_key,
    m.match_date,
    m.kickoff_time,
    m.season,
    m.div_code,
    ht.team_key                                                     as home_team_key,
    at.team_key                                                     as away_team_key,
    m.home_goals,
    m.away_goals,
    m.full_time_result,
    m.ht_home_goals,
    m.ht_away_goals,
    m.home_shots, m.away_shots,
    m.home_shots_on_target, m.away_shots_on_target,
    m.home_corners, m.away_corners,
    m.home_fouls, m.away_fouls,
    m.home_yellows, m.away_yellows,
    m.home_reds, m.away_reds,
    m.odds_home, m.odds_draw, m.odds_away,
    case m.full_time_result when 'H' then 3 when 'D' then 1 else 0 end as home_points,
    case m.full_time_result when 'A' then 3 when 'D' then 1 else 0 end as away_points,
    m._loaded_at
from {{ ref('stg_matches') }} m
join {{ ref('dim_team') }} ht on ht.team_name = m.home_team
join {{ ref('dim_team') }} at on at.team_name = m.away_team

{% if is_incremental() %}
where m._loaded_at > (
    select coalesce(max(_loaded_at), to_timestamp('1900-01-01'))
           - make_interval(0, 0, 0, {{ var('lookback_days', 3) }})
    from {{ this }}
)
{% endif %}
