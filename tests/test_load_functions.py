from trane.datasets.load_functions import (
    load_covid,
    load_covid_metadata,
    load_youtube,
    load_youtube_metadata,
)
from trane.typing.column_schema import ColumnSchema
from trane.typing.logical_types import (
    Datetime,
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
