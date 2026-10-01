"""Load football-data.co.uk CSVs into a Unity Catalog volume and a Delta raw table.

Usage: python ingest/load_raw.py --season 2627 [--divs E0 E1 ...]
"""
import argparse
import datetime as dt
import io
import logging
import os
import sys
import tempfile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from databricks import sql
from databricks.sdk import WorkspaceClient

CATALOG = "footy"
TABLE = f"{CATALOG}.raw.matches"
VOLUME = f"/Volumes/{CATALOG}/raw/landing"
ALL_DIVS = ["E0", "E1", "SP1", "D1", "I1", "F1"]

RENAME = {
    "Div": "div_raw", "Date": "date_raw", "Time": "time_raw",
    "HomeTeam": "home_team", "AwayTeam": "away_team",
    "FTHG": "ft_home_goals", "FTAG": "ft_away_goals", "FTR": "ft_result",
    "HTHG": "ht_home_goals", "HTAG": "ht_away_goals", "HTR": "ht_result",
    "HS": "home_shots", "AS": "away_shots",
    "HST": "home_shots_on_target", "AST": "away_shots_on_target",
    "HC": "home_corners", "AC": "away_corners",
    "HF": "home_fouls", "AF": "away_fouls",
    "HY": "home_yellows", "AY": "away_yellows",
    "HR": "home_reds", "AR": "away_reds",
    "B365H": "odds_home", "B365D": "odds_draw", "B365A": "odds_away",
    "Referee": "referee",
}
COLS = list(RENAME.values()) + ["match_date", "season", "div_code", "_loaded_at", "_source_file"]
# explicit file types: plain strings (pandas 3 would write "large_string", which Delta rejects)
ARROW_SCHEMA = pa.schema([
    (c, pa.date32() if c == "match_date" else pa.timestamp("us", tz="UTC") if c == "_loaded_at" else pa.string())
    for c in COLS
])

log = logging.getLogger("load_raw")


def parse_match_date(s: pd.Series) -> pd.Series:
    """football-data switches between dd/mm/yyyy and dd/mm/yy. Try both, never guess.
    Anything unparseable becomes NaT, and staging's not_null test catches it."""
    s = s.fillna("").astype(str).str.strip()
    long = pd.to_datetime(s, format="%d/%m/%Y", errors="coerce")
    short = pd.to_datetime(s, format="%d/%m/%y", errors="coerce")
    return long.fillna(short).dt.date


def connect():
    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", ""),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def ddl(cur) -> None:
    typ = lambda c: "DATE" if c == "match_date" else "TIMESTAMP" if c == "_loaded_at" else "STRING"
    cols = ",\n  ".join(f"{c} {typ(c)}" for c in COLS)
    cur.execute(f"CREATE TABLE IF NOT EXISTS {TABLE} (\n  {cols}\n) USING DELTA")
    cur.execute(f"DESCRIBE TABLE {TABLE}")
    existing = {row[0] for row in cur.fetchall()}
    for c in COLS:
        if c not in existing:            # additive only: never drop or retype here
            cur.execute(f"ALTER TABLE {TABLE} ADD COLUMN {c} {typ(c)}")
            log.warning("schema change: added raw column %s", c)


def season_window(season: str) -> tuple[dt.date, dt.date]:
    """'2627' -> (2026-07-01, 2027-06-30). Rejects anything that is not two consecutive years,
    e.g. '9999' — football-data.co.uk answered that with the 1998/99 files and HTTP 200."""
    if len(season) != 4 or not season.isdigit() or (int(season[:2]) + 1) % 100 != int(season[2:]):
        raise argparse.ArgumentTypeError(f"season must be two consecutive years like 2627, got {season!r}")
    start = 2000 + int(season[:2])
    return dt.date(start, 7, 1), dt.date(start + 1, 6, 30)


def dates_in_season(df: pd.DataFrame, season: str) -> bool:
    """Content check before anything destructive: every parsed match date must fall inside the season."""
    lo, hi = season_window(season)
    d = pd.to_datetime(df["match_date"], errors="coerce").dropna().dt.date
    return len(d) > 0 and bool(d.between(lo, hi).all())


def fetch(season: str, div: str):
    url = f"https://www.football-data.co.uk/mmz4281/{season}/{div}.csv"
    r = requests.get(url, timeout=30)
    if r.status_code != 200 or len(r.text) < 100:
        return None, url, None
    df = pd.read_csv(io.StringIO(r.text), dtype=str)
    df = df.reindex(columns=list(RENAME)).rename(columns=RENAME)   # missing cols -> NaN
    df = df.dropna(subset=["home_team", "away_team"])              # trailing blank rows
    df["match_date"] = parse_match_date(df["date_raw"])
    df["season"] = season
    df["div_code"] = div
    df["_loaded_at"] = dt.datetime.now(dt.timezone.utc)
    df["_source_file"] = url
    return df[COLS], url, r.text


def upload(w: WorkspaceClient, season: str, div: str, df: pd.DataFrame, csv_text: str) -> str:
    parquet_path = f"{VOLUME}/parquet/{season}/{div}.parquet"
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        table = pa.Table.from_pandas(df, schema=ARROW_SCHEMA, preserve_index=False)
        pq.write_table(table, tmp.name)
        with open(tmp.name, "rb") as fh:
            w.files.upload(parquet_path, fh, overwrite=True)
    with io.BytesIO(csv_text.encode()) as fh:                      # archive the original
        w.files.upload(f"{VOLUME}/csv/{season}/{div}.csv", fh, overwrite=True)
    return parquet_path


def load(cur, season: str, div: str, path: str) -> None:
    cur.execute(f"DELETE FROM {TABLE} WHERE season = '{season}' AND div_code = '{div}'")
    cur.execute(f"""
        COPY INTO {TABLE}
        FROM '{path}'
        FILEFORMAT = PARQUET
        COPY_OPTIONS ('force' = 'true', 'mergeSchema' = 'false')
    """)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", required=True, help="e.g. 2627 for 2026/27",
                    type=lambda x: (season_window(x), x)[1])   # reject 9999 before any network call
    ap.add_argument("--divs", nargs="+", default=ALL_DIVS)
    a = ap.parse_args(argv)

    w = WorkspaceClient()                      # reads DATABRICKS_HOST / DATABRICKS_TOKEN
    failures = 0
    rejected = 0
    with connect() as conn, conn.cursor() as cur:
        ddl(cur)
        for div in a.divs:
            df, url, csv_text = fetch(a.season, div)
            if df is None:
                log.warning("skip %s: not available at %s", div, url)
                failures += 1
                continue
            if not dates_in_season(df, a.season):          # never DELETE a slice for data that is not that season
                log.error("reject %s/%s: match dates outside the season window — not loading %s", a.season, div, url)
                rejected += 1
                continue
            path = upload(w, a.season, div, df, csv_text)
            load(cur, a.season, div, path)
            log.info("loaded %s rows for %s/%s from %s", len(df), a.season, div, path)
    if rejected:
        return 1                                   # wrong data is always a failure
    return 1 if failures == len(a.divs) else 0   # a missing file fails only if nothing loaded


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("databricks").setLevel(logging.WARNING)   # hide the connector's per-request chatter
    sys.exit(main())