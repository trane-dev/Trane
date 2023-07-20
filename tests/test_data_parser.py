import numpy as np
import pandas as pd
import pytest

from trane.utils.data_parser import denormalize, reorder_relationships


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
    flattened = denormalize(
        dataframes={
            "products": products_df,
            "log": logs_df,
        },
        relationships=relationships,
        target_entity="log",
    )
    assert flattened.shape == (5, 4)
    assert flattened["id"].is_unique
    print(flattened)
    assert sorted(flattened.columns.tolist()) == [
        "id",
        "product_id",
        "products.price",
        "session_id",
    ]
    for price in flattened["products.price"].tolist():
        assert price in [10, 20, 30]
    assert sorted(flattened["id"].tolist()) == [1, 2, 3, 4, 5]
    assert sorted(flattened["product_id"].tolist()) == [1, 1, 2, 2, 3]


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
    flattend = denormalize(
        dataframes={
            "products": products_df,
            "log": logs_df,
            "sessions": sessions_df,
        },
        relationships=relationships,
        target_entity="log",
    )
    assert flattend.shape == (5, 5)
    assert flattend["id"].is_unique
    assert sorted(flattend.columns.tolist()) == [
        "id",
        "product_id",
        "products.price",
        "session_id",
        "sessions.customer_id",
    ]
    for price in flattend["products.price"].tolist():
        assert price in [10, 20, 30]
    for session_id in flattend["session_id"]:
        assert session_id in [1, 2]
    for product_id in flattend["product_id"]:
        assert product_id in [1, 2, 3]


def test_denormalize_five_tables(products_df, logs_df, sessions_df, customers_df):
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
    target_entity_index = "id"
    flatened = denormalize(
        dataframes={
            "products": products_df,
            "log": logs_df,
            "sessions": sessions_df,
            "customers": customers_df,
        },
        relationships=relationships,
        target_entity="log",
    )
    assert flatened.shape == (5, 7)
    assert flatened[target_entity_index].is_unique
    assert sorted(flatened["products.price"].tolist()) == sorted([10, 20, 30, 10, 20])
    for session_id in flatened["session_id"]:
        assert session_id in [1, 2]
    for product_id in flatened["product_id"]:
        assert product_id in [1, 2, 3]
    for customer_id in flatened["sessions.customer_id"]:
        assert customer_id == 0
    for age in flatened["sessions.customers.age"]:
        assert age == 33
    for région_id in flatened["sessions.customers.région_id"]:
        assert région_id == "United States"


def test_denormalize_change_target(products_df, logs_df, sessions_df, customers_df):
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
    flatened = denormalize(
        dataframes={
            "products": products_df,
            "log": logs_df,
            "sessions": sessions_df,
            "customers": customers_df,
        },
        relationships=relationships,
        target_entity="sessions",
    )
    assert flatened["id"].is_unique
    for price in flatened["price"].tolist():
        assert price in [10, 20, 30]
    for session_id in flatened["session_id"]:
        assert session_id in [1, 2]
    for product_id in flatened["product_id"]:
        assert product_id in [1, 2, 3]
    for customer_id in flatened["customer_id"]:
        assert customer_id in [0, 1, 2]
    for région_id in flatened["région_id"]:
        assert région_id in [np.nan, "United States"]


def test_reorder_relationships():
    relationships = [
        ("sessions", "id", "log", "session_id"),
        ("customers", "id", "sessions", "customer_id"),
        ("products", "id", "log", "product_id"),
    ]
    reordered = reorder_relationships(relationships, "log")
    assert reordered[0] == ("customers", "id", "sessions", "customer_id")


# def test_denormalize_many_to_many():
#     group_df = pd.DataFrame({
#         "employee": ["Bob", "Jake", "Lisa", "Sue"],
#         "group": ["Accounting", "Engineering", "Engineering", "HR"],
#     })
#     hire_date_df = pd.DataFrame({
#         "employee": ["Bob", "Jake", "Lisa", "Sue"],
#         "hire_date": [2004, 2008, 2012, 2014],
#     })
#     skills_df = pd.DataFrame({
#         "group": ["Accounting", "Accounting", "Engineering", "Engineering", "HR", "HR"],
#         "skills": ["math", "spreadsheet", "coding", "linux", "spreadsheet", "organization"],
#     })
#     supervisor_df = pd.DataFrame({
#         "group": ["Accounting", "Engineering", "HR"],
#         "supervisor": ["Carly", "Guido", "Steve"],
#     })
#     denormalized = denormalize(
#         dataframes={
#             "group": group_df,
#             "hire_date": hire_date_df,
#             "skills": skills_df,
#             "supervisor": supervisor_df,
#         },
#         relationships=[
#             # one to one relationship
#             ("group", "employee", "hire_date", "employee"),
#             # one to many relationship
#             ("supervisor", "group", "group", "group"),
#             # many to many relationship
#             ("group", "group", "skills", "group"),
#         ],
#         target_entity="group",

#     )
#     print(denormalized)

#     expected = pd.DataFrame(
#         [
#             ("Accounting", "Carly", "Bob", 2004, "math"),
#             ("Accounting", "Carly", "Bob", 2004, "spreadsheets"),
#             ("Engineering", "Guido", "Jake", 2008, "coding"),
#             ("Engineering", "Guido", "Jake", 2008, "linux"),
#             ("Engineering", "Guido", "Lisa", 2012, "coding"),
#             ("Engineering", "Guido", "Lisa", 2012, "linux"),
#             ("HR", "Steve", "Sue", 2014, "spreadsheets"),
#             ("HR", "Steve", "Sue", 2014, "organization"),
#         ],
#         columns=["group", "supervisor", "employee", "hire_date", "skills"],
#     )
#     assert denormalized.equals(expected)


# def test_denormalize():
#     relationships = [
#         ("group.csv", "employee", "hire_date.csv", "employee"),
#         ("supervisor.csv", "group", "group.csv", "group"),
#         ("group.csv", "group", "skills.csv", "group"),
#     ]
#     res = denormalize(relationships)
#     expected = pd.DataFrame(
#         [
#             ("Accounting", "Carly", "Bob", 2004, "math"),
#             ("Accounting", "Carly", "Bob", 2004, "spreadsheets"),
#             ("Engineering", "Guido", "Jake", 2008, "coding"),
#             ("Engineering", "Guido", "Jake", 2008, "linux"),
#             ("Engineering", "Guido", "Lisa", 2012, "coding"),
#             ("Engineering", "Guido", "Lisa", 2012, "linux"),
#             ("HR", "Steve", "Sue", 2014, "spreadsheets"),
#             ("HR", "Steve", "Sue", 2014, "organization"),
#         ],
#         columns=["group", "supervisor", "employee", "hire_date", "skills"],
#     )
#     assert res.equals(expected)
