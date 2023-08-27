import pytest

from trane.metadata import MultiTableMetadata, SingleTableMetadata
from trane.parsing.denormalize import denormalize
from trane.typing.ml_types import Boolean, Categorical, Datetime, Double, Integer
from trane.utils.testing_utils import generate_mock_data


@pytest.fixture
def four_table_metadata():
    _, ml_types, relationships, primary_keys, time_indices = generate_mock_data(
        tables=["products", "logs", "sessions", "customers"],
    )
    metadata = MultiTableMetadata(
        ml_types=ml_types,
        primary_keys=primary_keys,
        relationships=relationships,
        time_indices=time_indices,
    )
    return metadata


def test_denormalize_two_tables():
    _, ml_types, relationships, primary_keys, time_indices = generate_mock_data(
        tables=["products", "logs"],
    )
    multi_metadata = MultiTableMetadata(
        ml_types=ml_types,
        primary_keys=primary_keys,
        relationships=relationships,
        time_indices=time_indices,
    )
    dataframes, normalized_metadata = denormalize(
        metadata=multi_metadata,
        target_table="logs",
    )
    assert isinstance(normalized_metadata, SingleTableMetadata)
    assert normalized_metadata.ml_types == {
        "id": Integer(tags="primary_key"),
        "product_id": Integer(),
        "session_id": Integer(),
        "products.price": Double(),
        "log_date": Datetime(tags="time_index"),
        "products.card_type": Categorical(),
        "products.first_purchase": Boolean(),
        "products.purchase_date": Datetime(),
    }
    assert normalized_metadata.primary_key == multi_metadata.primary_keys["logs"]


def test_denormalize_two_tables_change_target():
    _, ml_types, relationships, primary_keys, time_indices = generate_mock_data(
        tables=["products", "logs"],
    )
    multi_metadata = MultiTableMetadata(
        ml_types=ml_types,
        primary_keys=primary_keys,
        relationships=relationships,
        time_indices=time_indices,
    )
    dataframes, normalized_metadata = denormalize(
        metadata=multi_metadata,
        target_table="products",
    )
    # should not bring in logs
    for col in normalized_metadata.ml_types.keys():
        assert col in ml_types["products"]


def test_denormalize_three_tables():
    """
    S   P   Sessions, Products
     \\ /   .
      L     Log
    """
    _, ml_types, relationships, primary_keys, time_indices = generate_mock_data(
        tables=["products", "logs", "sessions"],
    )
    multi_metadata = MultiTableMetadata(
        ml_types=ml_types,
        primary_keys=primary_keys,
        time_indices=time_indices,
        relationships=relationships,
    )
    dataframes, normalized_metadata = denormalize(
        metadata=multi_metadata,
        target_table="logs",
    )
    assert normalized_metadata.ml_types == {
        "id": Integer(tags="primary_key"),
        "product_id": Integer(),
        "session_id": Integer(),
        "products.price": Double(),
        "sessions.customer_id": Categorical(),
        "log_date": Datetime(tags="time_index"),
        "products.card_type": Categorical(),
        "products.first_purchase": Boolean(),
        "products.purchase_date": Datetime(),
        "sessions.session_date": Datetime(),
    }


def test_denormalize_four_tables(four_table_metadata):
    """
     C       Customers
     |
    |||
     S   P   Sessions, Products
     \\ //
       L     Log
    """
    dataframes, normalized_metadata = denormalize(
        metadata=four_table_metadata,
        target_table="logs",
    )
    assert normalized_metadata.ml_types == {
        "id": Integer(tags="primary_key"),
        "product_id": Integer(),
        "session_id": Integer(),
        "products.price": Double(),
        "sessions.customer_id": Categorical(),
        "sessions.customers.age": Integer(),
        "sessions.customers.région_id": Categorical(),
        "log_date": Datetime(tags="time_index"),
        "products.card_type": Categorical(),
        "products.first_purchase": Boolean(),
        "products.purchase_date": Datetime(),
        "sessions.session_date": Datetime(),
    }


def test_denormalize_change_target(four_table_metadata):
    """
     C       Customers
     |
    |||
     S   P   Sessions, Products
     \\ //
       L     Log
    """

    dataframes, normalized_metadata = denormalize(
        metadata=four_table_metadata,
        target_table="sessions",
    )
    assert normalized_metadata.ml_types == {
        "id": Integer(tags="primary_key"),
        "customer_id": Categorical(),
        "customers.age": Integer(),
        "customers.région_id": Categorical(),
        "session_date": Datetime(tags="time_index"),
    }
