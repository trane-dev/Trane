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
    time_indices = {}
    for table in tables:
        if table not in ["products", "logs", "sessions", "customers"]:
            raise ValueError("Invalid table name: {}".format(table))
    if "products" in tables:
        dataframes["products"] = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "purchase_date": pd.to_datetime(
                    ["2018-01-01", "2018-01-02", "2018-01-03"],
                ),
                "price": [10, 20, 30],
                "first_purchase": [True, False, False],
                "card_type": ["visa", "mastercard", "visa"],
            },
        )
        ml_types["products"] = {
            "id": "Integer",
            "price": "Double",
            "purchase_date": "Datetime",
            "first_purchase": "Boolean",
            "card_type": "Categorical",
        }
        primary_keys["products"] = "id"
        time_indices["products"] = "purchase_date"
    if "logs" in tables:
        dataframes["logs"] = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "product_id": [1, 2, 3, 1, 2],
                "session_id": [1, 1, 2, 2, 2],
                "log_date": pd.to_datetime(
                    [
                        "2018-01-01",
                        "2018-01-02",
                        "2018-01-03",
                        "2018-01-04",
                        "2018-01-05",
                    ],
                ),
            },
        )
        ml_types["logs"] = {
            "id": "Integer",
            "product_id": "Integer",
            "session_id": "Integer",
            "log_date": "Datetime",
        }
        primary_keys["logs"] = "id"
        time_indices["logs"] = "log_date"
    if "sessions" in tables:
        dataframes["sessions"] = pd.DataFrame(
            {
                "id": [0, 1, 2, 3, 4, 5],
                "customer_id": pd.Categorical([0, 0, 0, 1, 1, 2]),
                "session_date": pd.to_datetime(
                    [
                        "2020-05-01",
                        "2020-05-02",
                        "2020-05-03",
                        "2020-05-04",
                        "2020-05-05",
                        "2020-05-06",
                    ],
                ),
            },
        )
        ml_types["sessions"] = {
            "id": "Integer",
            "customer_id": "Categorical",
            "session_date": "Datetime",
        }
        primary_keys["sessions"] = "id"
        time_indices["sessions"] = "session_date"
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
    if len(tables) == 1:
        ml_types = ml_types[tables[0]]
        primary_keys = primary_keys[tables[0]]
        time_indices = time_indices[tables[0]]
    return dataframes, ml_types, relationships, primary_keys, time_indices
