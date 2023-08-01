from trane.datasets.load_functions import (
    load_covid,
    load_covid_metadata,
    load_youtube,
    load_youtube_metadata,
    load_airbnb_reviews
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
    assert df["Lat"].dtype == "float64[pyarrow]"
    assert df["Long"].dtype == "float64[pyarrow]"
    assert df["Confirmed"].dtype == "int64[pyarrow]"
    assert df["Deaths"].dtype == "int64[pyarrow]"
    assert df["Recovered"].dtype == "int64[pyarrow]"
    assert df["Country/Region"].dtype == "category"
    assert df["Province/State"].dtype == "category"


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
    assert df["views"].dtype == "int64[pyarrow]"
    assert df["likes"].dtype == "int64[pyarrow]"
    assert df["dislikes"].dtype == "int64[pyarrow]"
    assert df["channel_title"].dtype == "category"
    assert df["category_id"].dtype == "category"

def test_load_airbnb_reviews():
    df = load_airbnb_reviews()
    expected_columns = ['listing_id', 'id', 'date', 'rating']
    
    assert df["date"].dtype == "datetime64[ns]"
    assert df["listing_id"].dtype == "int64[pyarrow]"
    assert df["id"].dtype == "int64[pyarrow]"
    assert df["rating"].dtype == "int64[pyarrow]"

def check_column_schema(columns, df, metadata):
    for col in columns:
        assert col in df.columns
        assert col in metadata.keys()
        assert isinstance(metadata[col], ColumnSchema)
