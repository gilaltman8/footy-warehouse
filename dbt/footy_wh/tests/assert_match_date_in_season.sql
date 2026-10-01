-- every match must fall inside its season (1 Jul – 30 Jun); catches data loaded under the wrong season code
select season, div_code, match_date, home_team, away_team
from {{ ref('stg_matches') }}
where match_date not between make_date(2000 + cast(left(season, 2) as int), 7, 1)
                         and make_date(2001 + cast(left(season, 2) as int), 6, 30)
