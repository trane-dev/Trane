import pandas as pd
import pytest

from trane.utils.data_parser import denormalize


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
            "id": [1, 2],
        },
    )


def test_denormalize_simple(products_df, logs_df):
    logs_df = logs_df.drop(columns=["session_id"])
    assert products_df["id"].is_unique
    assert logs_df["id"].is_unique
    relationships = [
        # one to many relationship
        ("products", "id", "log", "product_id"),
    ]
    flattend = denormalize(
        dataframes={
            "products": products_df,
            "log": logs_df,
        },
        relationships=relationships,
    )
    assert flattend.shape == (5, 3)
    assert flattend["id"].is_unique
    assert flattend.columns.tolist().sort() == ["id", "price", "product_id"].sort()
    for price in flattend["price"].tolist():
        assert price in [10, 20, 30]


def test_denormalize_three_tables(products_df, logs_df, sessions_df):
    assert sessions_df["id"].is_unique
    relationships = [
        # one to many relationship
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
    )
    assert flattend.shape == (5, 4)
    assert flattend["id"].is_unique
    assert (
        flattend.columns.tolist().sort()
        == ["id", "price", "product_id", "session_id"].sort()
    )
    for price in flattend["price"].tolist():
        assert price in [10, 20, 30]


# def test_denormalize_complex():
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
#         ]
#     )
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
