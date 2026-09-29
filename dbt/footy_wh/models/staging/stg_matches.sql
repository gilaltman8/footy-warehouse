with src as (
    select * from {{ source('raw', 'matches') }}
),
typed as (
    select
        season,
        div_code,
        match_date,
        nullif(time_raw, '')                        as kickoff_time,   -- Spark SQL has no TIME type
        trim(home_team)                             as home_team,
        trim(away_team)                             as away_team,
        cast(ft_home_goals as int)                  as home_goals,
        cast(ft_away_goals as int)                  as away_goals,
        ft_result                                   as full_time_result,
        try_cast(ht_home_goals as int)              as ht_home_goals,
        try_cast(ht_away_goals as int)              as ht_away_goals,
        try_cast(home_shots as int)                 as home_shots,
        try_cast(away_shots as int)                 as away_shots,
        try_cast(home_shots_on_target as int)       as home_shots_on_target,
        try_cast(away_shots_on_target as int)       as away_shots_on_target,
        try_cast(home_corners as int)               as home_corners,
        try_cast(away_corners as int)               as away_corners,
        try_cast(home_fouls as int)                 as home_fouls,
        try_cast(away_fouls as int)                 as away_fouls,
        try_cast(home_yellows as int)               as home_yellows,
        try_cast(away_yellows as int)               as away_yellows,
        try_cast(home_reds as int)                  as home_reds,
        try_cast(away_reds as int)                  as away_reds,
        try_cast(odds_home as double)               as odds_home,
        try_cast(odds_draw as double)               as odds_draw,
        try_cast(odds_away as double)               as odds_away,
        nullif(trim(referee), '')                   as referee,
        _loaded_at,
        _source_file
    from src
    where match_date is not null
),
deduped as (
    select
        *,
        row_number() over (
            partition by match_date, home_team, away_team
            order by _loaded_at desc
        ) as rn
    from typed
)
select
    {{ dbt_utils.generate_surrogate_key(['match_date', 'home_team', 'away_team']) }} as match_key,
    * except (rn)
from deduped
where rn = 1
