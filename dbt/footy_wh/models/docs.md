{% docs match_key %}
Surrogate key for one match: a hash of match_date, home_team and away_team. The source has no match id, so this is the grain of every match-level model.
{% enddocs %}

{% docs team_key %}
Surrogate key for a team: a hash of the team name as the source spells it. Stable across seasons and divisions.
{% enddocs %}

{% docs season %}
Season code as the source writes it, e.g. '2425' for 2024/25. Joins to dim_season.season_key.
{% enddocs %}

{% docs div_code %}
football-data.co.uk league code: E0 Premier League, E1 Championship, SP1 La Liga, D1 Bundesliga, I1 Serie A, F1 Ligue 1.
{% enddocs %}

{% docs match_date %}
Date the match was played, parsed from dd/mm/yyyy or dd/mm/yy in ingest.
{% enddocs %}

{% docs loaded_at %}
UTC time this row was loaded into raw. The incremental watermark: a corrected or reloaded file gets a new value, so its rows are re-processed.
{% enddocs %}

{% docs points %}
League points from this match for the team: 3 for a win, 1 for a draw, 0 for a loss.
{% enddocs %}

{% docs result %}
Result from the team's point of view: W, D or L.
{% enddocs %}

{% docs points_last_5 %}
Points from the team's previous five matches, excluding the current one (frame: 5 preceding to 1 preceding). Null for a team's first loaded match. No leakage by construction; covered by a unit test.
{% enddocs %}
