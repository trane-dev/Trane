import numpy as np
import pandas as pd


def generate_mock_data(tables):
    """
     C       Customers
     |
    |||
     S   P   Sessions, Products
     \\ //
       L     Logs
    """
    ml_types = {}
    dataframes = {}
    if "products" in tables:
        dataframes["products"] = products_df()
        ml_types["products"] = {"id": "Integer", "price": "Double"}
    if "logs" in tables:
        dataframes["logs"] = logs_df()
        ml_types["logs"] = {
            "id": "Integer",
            "product_id": "Integer",
            "session_id": "Integer",
        }
    if "sessions" in tables:
        dataframes["sessions"] = sessions_df()
        ml_types["sessions"] = {"id": "Integer", "customer_id": "Categorical"}
    if "customers" in tables:
        dataframes["customers"] = customers_df()
        ml_types["customers"] = {
            "id": "Integer",
            "age": "Integer",
            "région_id": "Categorical",
        }

    relationships = []
    if "sessions" in tables and "logs" in tables:
        relationships.append(("sessions", "id", "logs", "session_id"))
    if "products" in tables and "logs" in tables:
        relationships.append(("products", "id", "logs", "product_id"))
    if "customers" in tables and "sessions" in tables:
        relationships.append(("customers", "id", "sessions", "customer_id"))
    return dataframes, ml_types, relationships


def products_df():
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "price": [10, 20, 30],
        },
    )


def logs_df():
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "product_id": [1, 2, 3, 1, 2],
            "session_id": [1, 1, 2, 2, 2],
        },
    )


def sessions_df():
    return pd.DataFrame(
        {
            "id": [0, 1, 2, 3, 4, 5],
            "customer_id": pd.Categorical([0, 0, 0, 1, 1, 2]),
        },
    )


def régions_df():
    return pd.DataFrame(
        {
            "id": ["United States", "Mexico"],
        },
    )


def stores_df():
    return pd.DataFrame(
        {
            "id": range(6),
            "région_id": ["United States"] * 3 + ["Mexico"] * 2 + [np.nan],
        },
    )


def customers_df():
    return pd.DataFrame(
        {
            "id": pd.Categorical([0, 1, 2]),
            "age": [33, 25, 56],
            "région_id": ["United States"] * 3,
        },
    )
