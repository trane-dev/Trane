import numpy as np
import pandas as pd
import pytest

from trane.utils.data_parser import (
    child_relationships,
    denormalize,
)


@pytest.fixture()
def products_df():
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "price": [10, 20, 30],
        },
    )


@pytest.fixture()
def logs_df():
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "product_id": [1, 2, 3, 1, 2],
            "session_id": [1, 1, 2, 2, 2],
        },
    )


@pytest.fixture()
def sessions_df():
    return pd.DataFrame(
        {
            "id": [0, 1, 2, 3, 4, 5],
            "customer_id": pd.Categorical([0, 0, 0, 1, 1, 2]),
        },
    )


@pytest.fixture()
def régions_df():
    return pd.DataFrame(
        {
            "id": ["United States", "Mexico"],
        },
    )


@pytest.fixture()
def stores_df():
    return pd.DataFrame(
        {
            "id": range(6),
            "région_id": ["United States"] * 3 + ["Mexico"] * 2 + [np.nan],
        },
    )


@pytest.fixture()
def customers_df():
    return pd.DataFrame(
        {
            "id": pd.Categorical([0, 1, 2]),
            "age": [33, 25, 56],
            "région_id": ["United States"] * 3,
        },
    )


def test_denormalize_two_tables(products_df, logs_df):
    """
      Products
     /
    Log
    """
    assert products_df["id"].is_unique
    assert logs_df["id"].is_unique
    relationships = [
        # one to many relationship
        ("products", "id", "log", "product_id"),
    ]
    flat = denormalize(
        dataframes={
            "products": (products_df, "id"),
            "log": (logs_df, "id"),
        },
        relationships=relationships,
        target_entity="log",
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


def test_denormalize_three_tables(products_df, logs_df, sessions_df):
    """
    S   P   Sessions, Products
     \\ /   .
      L     Log
    """
    assert sessions_df["id"].is_unique

    for session_id in logs_df["session_id"].tolist():
        assert session_id in sessions_df["id"].tolist()

    for product_id in logs_df["product_id"].tolist():
        assert product_id in products_df["id"].tolist()
    relationships = [
        # one to many relationships
        ("products", "id", "log", "product_id"),
        ("sessions", "id", "log", "session_id"),
    ]
    flat = denormalize(
        dataframes={
            "products": (products_df, "id"),
            "log": (logs_df, "id"),
            "sessions": (sessions_df, "id"),
        },
        relationships=relationships,
        target_entity="log",
    )
    assert flat.shape == (5, 5)
    assert flat["id"].is_unique
    assert sorted(flat["id"].tolist()) == [1, 2, 3, 4, 5]
    flat = flat.set_index("id").sort_values("id")
    assert flat["product_id"].tolist() == [1, 2, 3, 1, 2]
    assert flat["session_id"].tolist() == [1, 1, 2, 2, 2]
    assert flat["products.price"].tolist() == [10, 20, 30, 10, 20]
    assert flat["sessions.customer_id"].tolist() == [0, 0, 0, 0, 0]


def test_denormalize_four_tables(products_df, logs_df, sessions_df, customers_df):
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
    flat = denormalize(
        dataframes={
            "products": (products_df, "id"),
            "log": (logs_df, "id"),
            "sessions": (sessions_df, "id"),
            "customers": (customers_df, "id"),
        },
        relationships=relationships,
        target_entity="log",
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


def test_denormalize_change_target(products_df, logs_df, sessions_df, customers_df):
    """
     C       Customers
     |
    |||
      S   P   Sessions, Products
     ||| |||
       ||
        L     Log
    """
    relationships = [
        ("sessions", "id", "log", "session_id"),
        ("customers", "id", "sessions", "customer_id"),
        ("products", "id", "log", "product_id"),
    ]
    flat = denormalize(
        dataframes={
            "products": (products_df, "id"),
            "log": (logs_df, "id"),
            "sessions": (sessions_df, "id"),
            "customers": (customers_df, "id"),
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
        ("sessions", "id", "log", "session_id"),
        ("customers", "id", "sessions", "customer_id"),
        ("products", "id", "log", "product_id"),
    ]
    valid = child_relationships("log", relationships=relationships)
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
