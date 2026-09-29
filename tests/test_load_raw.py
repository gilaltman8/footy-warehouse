import datetime as dt

import pandas as pd

from ingest.load_raw import parse_match_date


def test_both_year_formats():
    out = parse_match_date(pd.Series(["17/08/2024", "17/08/24"]))
    assert list(out) == [dt.date(2024, 8, 17)] * 2


def test_day_first_not_month_first():
    assert parse_match_date(pd.Series(["03/04/2025"]))[0] == dt.date(2025, 4, 3)


def test_bad_values_become_null_not_errors():
    out = parse_match_date(pd.Series(["", None, "31/02/2025", "not a date"]))
    assert out.isna().all()
