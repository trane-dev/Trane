from trane.metadata import MultiTableMetadata
from trane.parsing.denormalize import (
    child_relationships,
    denormalize,
)
from trane.utils.testing_utils import generate_mock_data


def test_denormalize_two_tables():
    """
      products
     /
    logs
    """
    (
        dataframes,
        ml_types,
        relationships,
        primary_keys,
        time_indices,
    ) = generate_mock_data(
        tables=["products", "logs"],
    )
    metadata = MultiTableMetadata(
        ml_types=ml_types,
        relationships=relationships,
        primary_keys=primary_keys,
        time_indices=time_indices,
    )
    products_df = dataframes["products"]
    logs_df = dataframes["logs"]

    assert products_df["id"].is_unique
    assert logs_df["id"].is_unique
    flat, _ = denormalize(
        dataframes=dataframes,
        metadata=metadata,
        target_table="logs",
    )
    assert flat.shape == (5, 8)
    assert flat["id"].is_unique
    assert sorted(flat.columns.tolist()) == [
        "id",
        "log_date",
        "product_id",
        "products.card_type",
        "products.first_purchase",
        "products.price",
        "products.purchase_date",
        "session_id",
    ]
    assert sorted(flat["id"].tolist()) == [1, 2, 3, 4, 5]
    flat = flat.set_index("id").sort_values("id")
    assert flat["product_id"].tolist() == [1, 2, 3, 1, 2]
    assert flat["session_id"].tolist() == [1, 1, 2, 2, 2]
    assert flat["products.price"].tolist() == [10.5, 20.25, 30.01, 10.5, 20.25]


def test_denormalize_two_tables_change():
    """
      products
     /
    logs
    """
    (
        dataframes,
        ml_types,
        relationships,
        primary_keys,
        time_indices,
    ) = generate_mock_data(
        tables=["products", "logs"],
    )
    metadata = MultiTableMetadata(
        ml_types=ml_types,
        relationships=relationships,
        primary_keys=primary_keys,
        time_indices=time_indices,
    )
    flat, _ = denormalize(
        dataframes=dataframes,
        metadata=metadata,
        target_table="products",
    )
    # should not bring in logs
    assert flat.shape == dataframes["products"].shape


def test_denormalize_three_tables():
    """
    S   P   sessions, products
     \\ /   .
      L     logs
    """
    (
        dataframes,
        ml_types,
        relationships,
        primary_keys,
        time_indices,
    ) = generate_mock_data(
        tables=["products", "logs", "sessions"],
    )
    metadata = MultiTableMetadata(
        ml_types=ml_types,
        relationships=relationships,
        primary_keys=primary_keys,
        time_indices=time_indices,
    )
    assert dataframes["sessions"]["id"].is_unique

    for session_id in dataframes["logs"]["session_id"].tolist():
        assert session_id in dataframes["sessions"]["id"].tolist()

    for product_id in dataframes["logs"]["product_id"].tolist():
        assert product_id in dataframes["products"]["id"].tolist()
    flat, _ = denormalize(
        dataframes=dataframes,
        metadata=metadata,
        target_table="logs",
    )
    assert flat.shape == (5, 10)
    assert flat["id"].is_unique
    assert sorted(flat["id"].tolist()) == [1, 2, 3, 4, 5]
    flat = flat.set_index("id").sort_values("id")
    assert flat["product_id"].tolist() == [1, 2, 3, 1, 2]
    assert flat["session_id"].tolist() == [1, 1, 2, 2, 2]
    assert flat["products.price"].tolist() == [10.5, 20.25, 30.01, 10.5, 20.25]
    assert flat["sessions.customer_id"].tolist() == [0, 0, 0, 0, 0]


def test_denormalize_four_tables():
    """
     C       customers
     |
    |||
     S   P   sessions, products
     \\ //
       L     logs
    """
    (
        dataframes,
        ml_types,
        relationships,
        primary_keys,
        time_indices,
    ) = generate_mock_data(
        tables=["products", "logs", "sessions", "customers"],
    )
    metadata = MultiTableMetadata(
        ml_types=ml_types,
        relationships=relationships,
        primary_keys=primary_keys,
        time_indices=time_indices,
    )
    flat, _ = denormalize(
        dataframes=dataframes,
        metadata=metadata,
        target_table="logs",
    )
    assert flat.shape == (5, 12)
    assert flat["id"].is_unique
    assert sorted(flat["id"].tolist()) == [1, 2, 3, 4, 5]
    flat = flat.set_index("id").sort_values("id")
    assert flat["products.price"].tolist() == [10.5, 20.25, 30.01, 10.5, 20.25]
    assert flat["sessions.customer_id"].tolist() == [0, 0, 0, 0, 0]
    assert flat["sessions.customers.age"].tolist() == [33, 33, 33, 33, 33]
    assert flat["sessions.customers.région_id"].tolist() == ["United States"] * 5
    assert flat["session_id"].tolist() == [1, 1, 2, 2, 2]
    assert flat["product_id"].tolist() == [1, 2, 3, 1, 2]


def test_denormalize_change_target():
    """
     C       customers
     |
    |||
      S   P   sessions, products
     ||| |||
       ||
        L     logs
    """
    (
        dataframes,
        ml_types,
        relationships,
        primary_keys,
        time_indices,
    ) = generate_mock_data(
        tables=["products", "logs", "sessions", "customers"],
    )
    metadata = MultiTableMetadata(
        ml_types=ml_types,
        relationships=relationships,
        primary_keys=primary_keys,
        time_indices=time_indices,
    )
    flat, _ = denormalize(
        dataframes=dataframes,
        metadata=metadata,
        target_table="sessions",
    )
    assert flat.shape == (6, 5)
    assert flat["id"].is_unique
    assert sorted(flat["id"].tolist()) == [0, 1, 2, 3, 4, 5]
    flat = flat.set_index("id").sort_values("id")
    assert flat["customer_id"].tolist() == [0, 0, 0, 1, 1, 2]
    assert flat["customers.age"].tolist() == [33, 33, 33, 25, 25, 56]
    assert flat["customers.région_id"].tolist() == ["United States"] * 6


def test_child_relationships():
    relationships = [
        ("sessions", "id", "logs", "session_id"),
        ("customers", "id", "sessions", "customer_id"),
        ("products", "id", "logs", "product_id"),
    ]
    valid = child_relationships("logs", relationships=relationships)
    assert len(valid) == 3
    for relationship in relationships:
        assert relationship in valid

    valid = child_relationships("customers", relationships)
    assert len(valid) == 0

    valid = child_relationships("sessions", relationships)
    assert valid == [
        ("customers", "id", "sessions", "customer_id"),
    ]

    valid = child_relationships("products", relationships)
    assert len(valid) == 0
