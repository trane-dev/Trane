from trane.core.problem_generator import denormalize_metadata
from trane.metadata import MultiTableMetadata, SingleTableMetadata
from trane.typing.ml_types import Categorical, Double, Integer
from trane.utils.testing_utils import generate_mock_data


def test_denormalize_metadata_two_tables():
    _, ml_types, relationships, primary_keys = generate_mock_data(
        tables=["products", "logs"],
    )
    multi_metadata = MultiTableMetadata(
        ml_types=ml_types,
        primary_keys=primary_keys,
        time_primary_keys={},
        relationships=relationships,
    )
    normalized_metadata = denormalize_metadata(
        metadata=multi_metadata,
        target_table="logs",
    )
    assert isinstance(normalized_metadata, SingleTableMetadata)
    assert normalized_metadata.ml_types == {
        "id": Integer,
        "product_id": Integer,
        "session_id": Integer,
        "products.price": Double,
    }
    assert normalized_metadata.index == multi_metadata.primary_keys["logs"]


def test_denormalize_metadata_three_tables():
    """
    S   P   Sessions, Products
     \\ /   .
      L     Log
    """
    _, ml_types, relationships, primary_keys = generate_mock_data(
        tables=["products", "logs", "sessions"],
    )
    multi_metadata = MultiTableMetadata(
        ml_types=ml_types,
        primary_keys=primary_keys,
        time_primary_keys={},
        relationships=relationships,
    )
    normalized_metadata = denormalize_metadata(
        metadata=multi_metadata,
        target_table="logs",
    )
    assert normalized_metadata.ml_types == {
        "id": Integer,
        "product_id": Integer,
        "session_id": Integer,
        "products.price": Double,
        "sessions.customer_id": Categorical,
    }


def test_denormalize_metadata_four_tables():
    """
     C       Customers
     |
    |||
     S   P   Sessions, Products
     \\ //
       L     Log
    """
    _, ml_types, relationships, primary_keys = generate_mock_data(
        tables=["products", "logs", "sessions", "customers"],
    )
    four_table_metadata = MultiTableMetadata(
        ml_types=ml_types,
        primary_keys=primary_keys,
        time_primary_keys={},
        relationships=relationships,
    )
    normalized_metadata = denormalize_metadata(
        metadata=four_table_metadata,
        target_table="logs",
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


def test_denormalize_metadata_change_target():
    """
     C       Customers
     |
    |||
     S   P   Sessions, Products
     \\ //
       L     Log
    """
    _, ml_types, relationships, primary_keys = generate_mock_data(
        tables=["products", "logs", "sessions", "customers"],
    )
    four_table_metadata = MultiTableMetadata(
        ml_types=ml_types,
        primary_keys=primary_keys,
        time_primary_keys={},
        relationships=relationships,
    )
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
