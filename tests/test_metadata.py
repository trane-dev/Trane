from trane.metadata.metadata import Metadata
from trane.typing.ml_types import Datetime, Integer, MLType


def test_init():
    metadata = Metadata()
    assert metadata.relationships == []
    assert metadata.ml_types == {}
    assert metadata.indexes == {}
    assert metadata.time_indexes == {}


def test_set_time_index():
    metadata = Metadata()
    metadata.set_time_index("table", "time_column")
    assert metadata.time_indexes == {"table": "time_column"}
    assert metadata.ml_types == {
        "table1": {"time_column": Datetime(tags={"time_index"})},
    }


def test_set_index():
    metadata = Metadata()
    metadata.set_type("table1", "index_column", Integer)
    metadata.set_index("table1", "index_column")
    assert metadata.indexes == {"table1": "index_column"}
    assert metadata.ml_types == {
        "table1": {"index_column": Integer(tags={"primary_key"})},
    }


def test_set_type():
    metadata = Metadata()
    metadata.set_type("table1", "column1", "categorical")
    assert metadata.ml_types == {"table1": {"column1": MLType("categorical")}}

    metadata.set_type("table1", "column2", "numeric", tags={"feature"})
    assert metadata.ml_types == {
        "table1": {
            "column1": MLType("categorical"),
            "column2": MLType("numeric", tags={"feature"}),
        },
    }


def test_add_relationships():
    metadata = Metadata()
    metadata.add_relationships(
        [("parent_table", "parent_key", "child_table", "child_key")],
    )
    assert metadata.relationships == [
        [("parent_table", "parent_key", "child_table", "child_key")],
    ]


def test_get_ml_type():
    metadata = Metadata()
    metadata.set_type("table1", "column1", "categorical")
    assert metadata.get_ml_type("table1", "column1") == MLType("categorical")
