from datetime import datetime

import pandas as pd
import pytest

from trane.metadata.metadata import MultiTableMetadata, SingleTableMetadata
from trane.typing.ml_types import (
    Boolean,
    Categorical,
    Datetime,
    Double,
    Integer,
    NaturalLanguage,
    Unknown,
)
from trane.utils.testing_utils import generate_mock_data


@pytest.fixture(scope="function")
def single_metadata():
    _, ml_types, _, primary_key, time_index = generate_mock_data(
        tables=["products"],
    )
    single_metadata = SingleTableMetadata(
        ml_types=ml_types,
        primary_key=primary_key,
        time_index=time_index,
    )
    return single_metadata


@pytest.fixture(scope="function")
def multitable_metadata():
    _, ml_types, relationships, primary_keys, time_indices = generate_mock_data(
        tables=["products", "logs"],
    )
    multitable_metadata = MultiTableMetadata(
        ml_types=ml_types,
        primary_keys=primary_keys,
        time_indices=time_indices,
        relationships=relationships,
    )
    return multitable_metadata


def test_init_single(single_metadata):
    verify_ml_types(
        single_metadata,
        {
            "id": Integer(tags="primary_key"),
            "price": Double(),
            "purchase_date": Datetime(tags="time_index"),
            "first_purchase": Boolean(),
            "card_type": Categorical(),
        },
    )
    assert single_metadata.primary_key == "id"
    assert single_metadata.time_index == "purchase_date"
    with pytest.raises(ValueError):
        single_metadata.set_primary_key("column_4")


def test_set_primary_key(single_metadata):
    single_metadata.reset_primary_key()
    assert single_metadata.primary_key is None
    single_metadata.set_primary_key("purchase_date")
    assert single_metadata.primary_key == "purchase_date"
    with pytest.raises(ValueError):
        single_metadata.set_primary_key("column_4")


def test_set_time_index(single_metadata):
    single_metadata.reset_time_index()
    assert single_metadata.time_index is None
    single_metadata.set_time_index("purchase_date")
    match = "Time index must be of type Datetime"
    with pytest.raises(ValueError, match=match):
        single_metadata.set_time_index("card_type")


def test_from_dataframe_single():
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
    assert list(metadata.ml_types.keys()) == ["column_1", "column_2", "column_3"]
    assert metadata.ml_types["column_1"] == Integer()
    assert metadata.ml_types["column_2"] == Unknown()
    assert metadata.ml_types["column_3"] == Datetime()


def test_from_dataframes_multi():
    (
        dataframes,
        ml_types,
        relationships,
        primary_keys,
        time_indices,
    ) = generate_mock_data(
        tables=["products", "logs"],
    )
    metadata = MultiTableMetadata.from_data(dataframes)
    assert isinstance(metadata, MultiTableMetadata)
    assert metadata.ml_types.keys() == ml_types.keys()
    for table in metadata.ml_types:
        assert metadata.ml_types[table].keys() == ml_types[table].keys()
        for column in metadata.ml_types[table]:
            if table == "products" and column == "card_type":
                assert isinstance(metadata.ml_types[table][column], Unknown)
                continue
            assert str(metadata.ml_types[table][column]) == ml_types[table][column]


def test_init_multi(multitable_metadata):
    expected_ml_types = {
        "products": {
            "id": Integer(tags="primary_key"),
            "price": Double(),
            "purchase_date": Datetime(tags="time_index"),
            "first_purchase": Boolean(),
            "card_type": Categorical(),
        },
        "logs": {
            "id": Integer(tags="primary_key"),
            "product_id": Integer(),
            "session_id": Integer(),
            "log_date": Datetime(),
        },
    }
    verify_ml_types(multitable_metadata, expected_ml_types)
    assert multitable_metadata.primary_keys == {
        "products": "id",
        "logs": "id",
    }
    assert multitable_metadata.time_indices == {
        "products": "purchase_date",
        "logs": "log_date",
    }
    assert "column_4" not in multitable_metadata.ml_types["orders"]
    with pytest.raises(ValueError):
        multitable_metadata.set_primary_key("orders", "column_4")


def test_set_primary_key_multi(multitable_metadata):
    multitable_metadata.reset_primary_key("products")
    assert multitable_metadata.primary_keys == {"logs": "id"}
    multitable_metadata.set_primary_key("products", "id")
    assert multitable_metadata.primary_keys == {
        "products": "id",
        "logs": "id",
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
    multitable_metadata.set_type("products", "card_type", "NaturalLanguage")
    expected_ml_types = {
        "products": {
            "id": Integer(tags="primary_key"),
            "price": Double(),
            "purchase_date": Datetime(tags="time_index"),
            "first_purchase": Boolean(),
            "card_type": NaturalLanguage(),
        },
        "logs": {
            "id": Integer(tags="primary_key"),
            "product_id": Integer(),
            "session_id": Integer(),
            "log_date": Datetime(),
        },
    }
    verify_ml_types(multitable_metadata, expected_ml_types)


def test_add_relationships(multitable_metadata):
    relationships = multitable_metadata.relationships
    multitable_metadata.clear_relationships()
    assert multitable_metadata.relationships == []
    multitable_metadata.add_relationships(relationships)
    assert multitable_metadata.relationships == [
        ("products", "id", "logs", "product_id"),
    ]


def test_add_relationships_new_table(multitable_metadata):
    multitable_metadata.remove_table("products")
    multitable_metadata.clear_relationships()
    assert "products" not in multitable_metadata.ml_types
    assert multitable_metadata.relationships == []
    multitable_metadata.add_table(
        table="products",
        ml_types={
            "id": "Integer",
            "price": "Double",
            "purchase_date": "Datetime",
            "first_purchase": "Boolean",
            "card_type": "Categorical",
        },
    )
    assert "id" in multitable_metadata.ml_types["products"]
    multitable_metadata.add_relationships(
        [("products", "id", "logs", "product_id")],
    )
    multitable_metadata.clear_relationships()
    with pytest.raises(ValueError):
        multitable_metadata.add_relationships(
            [("orders", "column_1", "products", "column_1")],
        )


def verify_ml_types(metadata, expected_ml_types):
    if metadata.get_metadata_type() == "single":
        for key in expected_ml_types:
            assert key in metadata.ml_types
            assert metadata.ml_types[key] == expected_ml_types[key]
    else:
        for table in expected_ml_types:
            assert table in metadata.ml_types
            for column in expected_ml_types[table]:
                assert column in metadata.ml_types[table]
                if isinstance(expected_ml_types[table][column], Datetime):
                    assert isinstance(metadata.ml_types[table][column], Datetime)
                else:
                    assert (
                        metadata.ml_types[table][column]
                        == expected_ml_types[table][column]
                    )
