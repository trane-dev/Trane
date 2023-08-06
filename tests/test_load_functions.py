from trane.datasets.load_functions import (
    load_airbnb_reviews,
)
from trane.typing.column_schema import ColumnSchema


def test_load_airbnb_reviews():
    df = load_airbnb_reviews()

    assert df["date"].dtype == "datetime64[ns]"
    assert df["listing_id"].dtype == "int64[pyarrow]"
    assert df["id"].dtype == "int64[pyarrow]"
    assert df["rating"].dtype == "int64[pyarrow]"


def check_column_schema(columns, df, metadata):
    for col in columns:
        assert col in df.columns
        assert col in metadata.keys()
        assert isinstance(metadata[col], ColumnSchema)
