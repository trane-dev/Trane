from trane.parsing.denormalize import (
    child_relationships,
    denormalize,
)
from trane.utils.testing_utils import generate_mock_data


def test_denormalize_two_tables():
    """
      Products
     /
    Logs
    """
    dataframes, _, relationships = generate_mock_data(tables=["products", "logs"])
    products_df = dataframes["products"]
    logs_df = dataframes["logs"]

    assert products_df["id"].is_unique
    assert logs_df["id"].is_unique
    relationships = [
        # one to many relationship
        ("products", "id", "logs", "product_id"),
    ]
    flat = denormalize(
        dataframes={
            "products": products_df,
            "logs": logs_df,
        },
        relationships=relationships,
        target_entity="logs",
    )
    assert flat.shape == (5, 4)
    assert flat["id"].is_unique
    assert sorted(flat.columns.tolist()) == [
        "id",
        "product_id",
        "products.price",
        "session_id",
    ]
    assert sorted(flat["id"].tolist()) == [1, 2, 3, 4, 5]
    flat = flat.set_index("id").sort_values("id")
    assert flat["product_id"].tolist() == [1, 2, 3, 1, 2]
    assert flat["session_id"].tolist() == [1, 1, 2, 2, 2]
    assert flat["products.price"].tolist() == [10, 20, 30, 10, 20]


def test_denormalize_three_tables():
    """
    S   P   Sessions, Products
     \\ /   .
      L     Logs
    """
    dataframes, _, relationships = generate_mock_data(
        tables=["products", "logs", "sessions"],
    )
    products_df = dataframes["products"]
    logs_df = dataframes["logs"]
    sessions_df = dataframes["sessions"]
    assert sessions_df["id"].is_unique

    for session_id in logs_df["session_id"].tolist():
        assert session_id in sessions_df["id"].tolist()

    for product_id in logs_df["product_id"].tolist():
        assert product_id in products_df["id"].tolist()
    relationships = [
        # one to many relationships
        ("products", "id", "logs", "product_id"),
        ("sessions", "id", "logs", "session_id"),
    ]
    flat = denormalize(
        dataframes={
            "products": products_df,
            "logs": logs_df,
            "sessions": sessions_df,
        },
        relationships=relationships,
        target_entity="logs",
    )
    assert flat.shape == (5, 5)
    assert flat["id"].is_unique
    assert sorted(flat["id"].tolist()) == [1, 2, 3, 4, 5]
    flat = flat.set_index("id").sort_values("id")
    assert flat["product_id"].tolist() == [1, 2, 3, 1, 2]
    assert flat["session_id"].tolist() == [1, 1, 2, 2, 2]
    assert flat["products.price"].tolist() == [10, 20, 30, 10, 20]
    assert flat["sessions.customer_id"].tolist() == [0, 0, 0, 0, 0]


def test_denormalize_four_tables():
    """
     C       Customers
     |
    |||
     S   P   Sessions, Products
     \\ //
       L     Logs
    """
    dataframes, _, relationships = generate_mock_data(
        tables=["products", "logs", "sessions", "customers"],
    )
    products_df = dataframes["products"]
    logs_df = dataframes["logs"]
    sessions_df = dataframes["sessions"]
    customers_df = dataframes["customers"]
    flat = denormalize(
        dataframes={
            "products": products_df,
            "logs": logs_df,
            "sessions": sessions_df,
            "customers": customers_df,
        },
        relationships=relationships,
        target_entity="logs",
    )
    assert flat.shape == (5, 7)
    assert flat["id"].is_unique
    assert sorted(flat["id"].tolist()) == [1, 2, 3, 4, 5]
    flat = flat.set_index("id").sort_values("id")
    assert flat["products.price"].tolist() == [10, 20, 30, 10, 20]
    assert flat["sessions.customer_id"].tolist() == [0, 0, 0, 0, 0]
    assert flat["sessions.customers.age"].tolist() == [33, 33, 33, 33, 33]
    assert flat["sessions.customers.région_id"].tolist() == ["United States"] * 5
    assert flat["session_id"].tolist() == [1, 1, 2, 2, 2]
    assert flat["product_id"].tolist() == [1, 2, 3, 1, 2]


def test_denormalize_change_target():
    """
     C       Customers
     |
    |||
      S   P   Sessions, Products
     ||| |||
       ||
        L     Logs
    """
    dataframes, _, relationships = generate_mock_data(
        tables=["products", "logs", "sessions", "customers"],
    )
    products_df = dataframes["products"]
    logs_df = dataframes["logs"]
    sessions_df = dataframes["sessions"]
    customers_df = dataframes["customers"]
    flat = denormalize(
        dataframes={
            "products": products_df,
            "logs": logs_df,
            "sessions": sessions_df,
            "customers": customers_df,
        },
        relationships=relationships,
        target_entity="sessions",
    )
    assert flat.shape == (6, 4)
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
