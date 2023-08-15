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
    primary_keys = {}
    if "products" in tables:
        dataframes["products"] = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "price": [10, 20, 30],
            },
        )
        ml_types["products"] = {"id": "Integer", "price": "Double"}
        primary_keys["products"] = "id"
    if "logs" in tables:
        dataframes["logs"] = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "product_id": [1, 2, 3, 1, 2],
                "session_id": [1, 1, 2, 2, 2],
            },
        )
        ml_types["logs"] = {
            "id": "Integer",
            "product_id": "Integer",
            "session_id": "Integer",
        }
        primary_keys["logs"] = "id"
    if "sessions" in tables:
        dataframes["sessions"] = pd.DataFrame(
            {
                "id": [0, 1, 2, 3, 4, 5],
                "customer_id": pd.Categorical([0, 0, 0, 1, 1, 2]),
            },
        )
        ml_types["sessions"] = {"id": "Integer", "customer_id": "Categorical"}
        primary_keys["sessions"] = "id"
    if "customers" in tables:
        dataframes["customers"] = pd.DataFrame(
            {
                "id": pd.Categorical([0, 1, 2]),
                "age": [33, 25, 56],
                "région_id": ["United States"] * 3,
            },
        )
        ml_types["customers"] = {
            "id": "Integer",
            "age": "Integer",
            "région_id": "Categorical",
        }
        primary_keys["customers"] = "id"

    relationships = []
    if "sessions" in tables and "logs" in tables:
        relationships.append(("sessions", "id", "logs", "session_id"))
    if "products" in tables and "logs" in tables:
        relationships.append(("products", "id", "logs", "product_id"))
    if "customers" in tables and "sessions" in tables:
        relationships.append(("customers", "id", "sessions", "customer_id"))
    return dataframes, ml_types, relationships, primary_keys
