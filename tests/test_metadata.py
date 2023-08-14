from datetime import datetime

import pandas as pd
import pytest

from trane.metadata.metadata import MultiTableMetadata, SingleTableMetadata
from trane.typing.ml_types import Categorical, Datetime, Double, Integer, Unknown


@pytest.fixture(scope="function")
def single_metadata():
    single_metadata = SingleTableMetadata(
        ml_types={
            "column_1": "Categorical",
            "column_2": "Integer",
            "column_3": "Datetime",
        },
        index="column_1",
        time_index="column_3",
    )
    return single_metadata


def test_init_single(single_metadata):
    assert single_metadata.ml_types == {
        "column_1": Categorical,
        "column_2": Integer,
        "column_3": Datetime,
    }
    assert single_metadata.index == "column_1"
    assert single_metadata.time_index == "column_3"
    with pytest.raises(ValueError):
        single_metadata.set_primary_key("column_4")


def test_set_primary_key(single_metadata):
    single_metadata.set_primary_key("column_2")
    assert single_metadata.index == "column_2"
    assert single_metadata.ml_types == {
        "column_1": Categorical,
        "column_2": Integer,
        "column_3": Datetime,
    }
    with pytest.raises(ValueError):
        single_metadata.set_primary_key("column_4")


def test_set_time_index(single_metadata):
    single_metadata.set_type("column_2", "Datetime")
    single_metadata.set_time_index("column_2")
    match = "Time index must be of type Datetime"
    with pytest.raises(ValueError, match=match):
        single_metadata.set_time_index("column_1")


def test_from_dataframe():
    data = pd.DataFrame(
        {
            "column_1": [1, 2, 3, 4, 5, 6],
            "column_2": ["a", "b", "c", "a", "b", "c"],
            "column_3": [
                datetime(2018, 1, 1),
                datetime(2018, 1, 2),
                datetime(2018, 1, 3),
                datetime(2018, 1, 4),
                datetime(2018, 1, 5),
                datetime(2018, 1, 6),
            ],
        },
    )
    metadata = SingleTableMetadata.from_data(data)
    assert isinstance(metadata, SingleTableMetadata)
    assert metadata.ml_types == {
        "column_1": Integer,
        "column_2": Unknown,
        "column_3": Datetime,
    }


@pytest.fixture(scope="module")
def multitable_metadata():
    return MultiTableMetadata(
        ml_types={
            "orders": {
                "column_1": "Categorical",
                "column_2": "Integer",
                "column_3": "Datetime",
            },
            "customers": {
                "column_5": "Categorical",
                "column_6": "Integer",
                "column_7": "Datetime",
            },
        },
        primary_keys={
            "orders": "column_1",
            "customers": "column_5",
        },
        time_primary_keys={
            "orders": "column_3",
            "customers": "column_7",
        },
        relationships=[
            ("orders", "column_1", "customers", "column_5"),
        ],
    )


def test_init_multi(multitable_metadata):
    assert multitable_metadata.ml_types == {
        "orders": {
            "column_1": Categorical,
            "column_2": Integer,
            "column_3": Datetime,
        },
        "customers": {
            "column_5": Categorical,
            "column_6": Integer,
            "column_7": Datetime,
        },
    }
    assert multitable_metadata.primary_keys == {
        "orders": "column_1",
        "customers": "column_5",
    }
    assert multitable_metadata.time_primary_keys == {
        "orders": "column_3",
        "customers": "column_7",
    }
    assert "column_4" not in multitable_metadata.ml_types["orders"]
    with pytest.raises(ValueError):
        multitable_metadata.set_primary_key("orders", "column_4")


def test_set_primary_key_multi(multitable_metadata):
    multitable_metadata.set_primary_key("orders", "column_2")
    assert multitable_metadata.primary_keys == {
        "orders": "column_2",
        "customers": "column_5",
    }


def test_set_time_index_multi(multitable_metadata):
    multitable_metadata.remove_table("products")
    multitable_metadata.add_table(
        table="products",
        ml_types={
            "column_8": "Categorical",
            "column_9": "Integer",
            "column_10": "Datetime",
        },
    )
    multitable_metadata.set_primary_key("products", "column_9")
    multitable_metadata.set_time_index("products", "column_10")
    with pytest.raises(ValueError):
        multitable_metadata.set_time_index("products", "column_9")


def test_set_type_multi(multitable_metadata):
    multitable_metadata.set_type("orders", "column_1", "double")
    assert multitable_metadata.ml_types["orders"] == {
        "column_1": Double,
        "column_2": Integer,
        "column_3": Datetime,
    }


def test_add_relationships(multitable_metadata):
    multitable_metadata.clear_relationships()
    multitable_metadata.add_relationships(
        ("orders", "column_1", "customers", "column_6"),
    )
    assert multitable_metadata.relationships == [
        ("orders", "column_1", "customers", "column_6"),
    ]


def test_add_relationships_new_table(multitable_metadata):
    multitable_metadata.remove_table("products")
    multitable_metadata.add_table(
        table="products",
        ml_types={
            "column_8": "Categorical",
            "column_9": "Integer",
            "column_10": "Datetime",
        },
    )
    relationships = [("orders", "column_1", "products", "column_9")]
    multitable_metadata.add_relationships(relationships)
    multitable_metadata.remove_relationship(relationships)
    relationships = [("orders", "column_1", "products", "column_1")]
    with pytest.raises(ValueError):
        multitable_metadata.add_relationships(relationships)
