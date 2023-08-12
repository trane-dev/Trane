import pytest

from trane.core.problem_generator import denormalize_metadata
from trane.metadata import MultiTableMetadata, SingleTableMetadata
from trane.typing.ml_types import Categorical, Double, Integer


@pytest.fixture(scope="function")
def four_table_metadata():
    """
     C       Customers
     |
    |||
     S   P   Sessions, Products
     \\ //
       L     Log
    """
    relationships = [
        ("sessions", "id", "log", "session_id"),
        ("customers", "id", "sessions", "customer_id"),
        ("products", "id", "log", "product_id"),
    ]
    multi_metadata = MultiTableMetadata(
        ml_types={
            "products": {"id": "Integer", "price": "Double"},
            "log": {
                "id": "Integer",
                "product_id": "Integer",
                "session_id": "Integer",
            },
            "sessions": {"id": "Integer", "customer_id": "Categorical"},
            "customers": {
                "id": "Integer",
                "age": "Integer",
                "région_id": "Categorical",
            },
        },
        indices={
            "products": "id",
            "log": "id",
            "sessions": "id",
            "customers": "id",
        },
        time_indices={},
        relationships=relationships,
    )
    return multi_metadata


def test_denormalize_two_tables():
    relationships = [
        # one to many relationship
        ("products", "id", "log", "product_id"),
    ]
    multi_metadata = MultiTableMetadata(
        ml_types={
            "products": {"id": "Integer", "price": "Double"},
            "log": {"id": "Integer", "product_id": "Integer", "session_id": "Integer"},
        },
        indices={"products": "id", "log": "id"},
        time_indices={},
        relationships=relationships,
    )
    normalized_metadata = denormalize_metadata(
        metadata=multi_metadata,
        target_table="log",
    )
    assert isinstance(normalized_metadata, SingleTableMetadata)
    assert normalized_metadata.ml_types == {
        "id": Integer,
        "product_id": Integer,
        "session_id": Integer,
        "products.price": Double,
    }
    assert normalized_metadata.index == multi_metadata.indices["log"]


def test_denormalize_three_tables():
    """
    S   P   Sessions, Products
     \\ /   .
      L     Log
    """
    relationships = [
        # one to many relationships
        ("products", "id", "log", "product_id"),
        ("sessions", "id", "log", "session_id"),
    ]
    multi_metadata = MultiTableMetadata(
        ml_types={
            "products": {"id": "Integer", "price": "Double"},
            "log": {
                "id": "Integer",
                "product_id": "Integer",
                "session_id": "Integer",
            },
            "sessions": {"id": "Integer", "customer_id": "Categorical"},
        },
        indices={"products": "id", "log": "id", "sessions": "id"},
        time_indices={},
        relationships=relationships,
    )
    normalized_metadata = denormalize_metadata(
        metadata=multi_metadata,
        target_table="log",
    )
    assert normalized_metadata.ml_types == {
        "id": Integer,
        "product_id": Integer,
        "session_id": Integer,
        "products.price": Double,
        "sessions.customer_id": Categorical,
    }


def test_denormalize_four_tables(four_table_metadata):
    normalized_metadata = denormalize_metadata(
        metadata=four_table_metadata,
        target_table="log",
    )
    assert normalized_metadata.ml_types == {
        "id": Integer,
        "product_id": Integer,
        "session_id": Integer,
        "products.price": Double,
        "sessions.customer_id": Categorical,
        "sessions.customers.age": Integer,
        "sessions.customers.région_id": Categorical,
    }


def test_denormalize_change_target(four_table_metadata):
    normalized_metadata = denormalize_metadata(
        metadata=four_table_metadata,
        target_table="sessions",
    )
    assert normalized_metadata.ml_types == {
        "id": Integer,
        "customer_id": Categorical,
        "customers.age": Integer,
        "customers.région_id": Categorical,
    }
