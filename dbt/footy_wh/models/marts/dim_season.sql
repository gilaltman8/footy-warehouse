with seasons as (
    select distinct season from {{ ref('stg_matches') }}
)

select
    season as season_key,
    cast(concat('20', substr(season, 1, 2)) as int) as season_start_year,
    concat('20', substr(season, 1, 2), '/', substr(season, 3, 2)) as season_label
from seasons
