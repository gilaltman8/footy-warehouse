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


def test_season_code_must_be_two_consecutive_years():
    import argparse
    import pytest
    from ingest.load_raw import season_window
    assert season_window("2627") == (dt.date(2026, 7, 1), dt.date(2027, 6, 30))
    for bad in ["9999", "2626", "262", "abcd"]:
        with pytest.raises(argparse.ArgumentTypeError):
            season_window(bad)


def test_rejects_data_from_another_season():
    from ingest.load_raw import dates_in_season
    good = pd.DataFrame({"match_date": [dt.date(2026, 8, 15), dt.date(2027, 5, 20)]})
    old = pd.DataFrame({"match_date": [dt.date(1998, 8, 15)]})      # what '9999' actually returned
    assert dates_in_season(good, "2627")
    assert not dates_in_season(old, "2627")
    assert not dates_in_season(pd.DataFrame({"match_date": []}), "2627")
