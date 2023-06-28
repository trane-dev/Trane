from woodwork.column_schema import ColumnSchema
from woodwork.logical_types import (
    Datetime,
)

from trane.datasets.load_functions import (
    load_bike,
    load_bike_metadata,
    load_covid,
    load_covid_metadata,
    load_youtube,
    load_youtube_metadata,
)


def test_load_covid():
    df = load_covid()
    metadata = load_covid_metadata()
    expected_columns = [
        "Province/State",
        "Country/Region",
        "Lat",
        "Long",
        "Date",
        "Confirmed",
        "Deaths",
        "Recovered",
    ]
    check_column_schema(expected_columns, df, metadata)
    assert len(df) >= 17136
    assert df["Date"].dtype == "datetime64[ns]"
    assert metadata["Date"] == ColumnSchema(logical_type=Datetime)


def test_load_bike():
    df = load_bike()
    metadata = load_bike_metadata()
    expected_columns = [
        "date",
        "hour",
        "usertype",
        "gender",
        "tripduration",
        "temperature",
        "from_station_id",
        "dpcapacity_start",
        "to_station_id",
        "dpcapacity_end",
    ]
    check_column_schema(expected_columns, df, metadata)
    assert df["date"].dtype == "datetime64[ns]"
    assert metadata["date"] == ColumnSchema(logical_type=Datetime)


def test_load_youtube():
    df = load_youtube()
    metadata = load_youtube_metadata()
    expected_columns = [
        "trending_date",
        "channel_title",
        "category_id",
        "views",
        "likes",
        "dislikes",
        "comment_count",
    ]
    check_column_schema(expected_columns, df, metadata)
    assert df["trending_date"].dtype == "datetime64[ns]"
    assert metadata["trending_date"] == ColumnSchema(logical_type=Datetime)


def check_column_schema(columns, df, metadata):
    for col in columns:
        assert col in df.columns
        assert col in metadata.keys()
        assert isinstance(metadata[col], ColumnSchema)
