import numpy as np
import pandas as pd
from numpy import random
from numpy.random import choice

from trane.metadata import MultiTableMetadata, SingleTableMetadata


# v1 version
def generate_mock_data(tables):
    """
     C       customers
     |
    |||
     S   P   sessions, products
     \\ //
       L     logs
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
                "price": [10.5, 20.25, 30.01],
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


# v2 version
def create_mock_data(
    num_customers=5,
    num_products=5,
    num_sessions=5,
    num_transactions=500,
    return_single_dataframe=False,
    random_seed=0,
):
    random.seed(random_seed)
    last_date = pd.Timestamp("2022-12-01")
    first_date = pd.Timestamp("2018-01-01")
    first_birthday = pd.Timestamp("1950-01-01")

    join_dates = random_dates(first_date, last_date, num_customers)
    birth_dates = random_dates(first_birthday, first_date, num_customers)
    customers_df = pd.DataFrame(
        {
            "customer_id": range(1, num_customers + 1),
            "zip_code": choice(["11101", "27109"], num_customers),
            "join_date": pd.Series(join_dates).dt.round("1s"),
            "birthday": pd.Series(birth_dates).dt.round("1d"),
        },
    )
    customers_df = customers_df.astype(
        {
            "customer_id": "int64[pyarrow]",
            "zip_code": "category",
            "join_date": "datetime64[ns]",
            "birthday": "datetime64[ns]",
        },
    )

    products_df = pd.DataFrame(
        {
            "product_id": range(1, num_products + 1),
            "brand": choice(["A", "B", "C"], num_products),
        },
    )
    products_df = products_df.astype(
        {
            "product_id": "int64[pyarrow]",
            "brand": "category",
        },
    )

    valid_customer_ids = customers_df["customer_id"].unique()
    sessions_df = pd.DataFrame(
        {
            "session_id": range(1, num_sessions + 1),
            "customer_id": choice(valid_customer_ids, num_sessions),
            "device": choice(["desktop", "mobile", "tablet"], num_sessions),
        },
    )

    valid_sesions_ids = sessions_df["session_id"].unique()
    valid_product_ids = products_df["product_id"].unique()
    transactions_df = pd.DataFrame(
        {
            "transaction_id": range(1, num_transactions + 1),
            "session_id": choice(valid_sesions_ids, num_transactions),
            "product_id": choice(valid_product_ids, num_transactions),
            "amount": random.randint(500, 15000, num_transactions) / 100,
        },
    )
    transactions_df = transactions_df.sort_values("session_id").reset_index(drop=True)
    base_time = pd.Timestamp("1/1/2022")
    random_seconds = np.random.randint(1, 5000, num_transactions)
    transactions_df["transaction_time"] = pd.to_datetime(
        base_time + pd.to_timedelta(np.cumsum(random_seconds), unit="s"),
    )
    transactions_df = transactions_df.astype(
        {
            "transaction_id": "int64[pyarrow]",
            "session_id": "int64[pyarrow]",
            "product_id": "int64[pyarrow]",
            "amount": "float64[pyarrow]",
            "transaction_time": "datetime64[ns]",
        },
    )
    # session_start is the first transaction time for each session
    session_starts = transactions_df.drop_duplicates("session_id")[
        ["session_id", "transaction_time"]
    ].rename(columns={"transaction_time": "session_start"})
    sessions_df = sessions_df.merge(session_starts)
    sessions_df = sessions_df.astype(
        {
            "session_id": "int64[pyarrow]",
            "customer_id": "int64[pyarrow]",
            "device": "category",
            "session_start": "datetime64[ns]",
        },
    )
    if return_single_dataframe:
        # assume target table is transactions
        return (
            transactions_df.merge(sessions_df)
            .merge(customers_df)
            .merge(products_df)
            .reset_index(drop=True)
        )
    dataframes = {
        "customers": customers_df,
        "products": products_df,
        "sessions": sessions_df,
        "transactions": transactions_df,
    }
    return dataframes


def create_mock_data_metadata(single_table=False):
    if single_table:
        return SingleTableMetadata(
            {
                "customer_id": "Integer",
                "zip_code": "Categorical",
                "join_date": "Datetime",
                "birthday": "Datetime",
                "product_id": "Integer",
                "brand": "Categorical",
                "session_id": "Integer",
                "device": "Categorical",
                "transaction_id": "Integer",
                "amount": "Double",
                "transaction_time": "Datetime",
                "session_start": "Datetime",
            },
            primary_key="transaction_id",
            time_index="transaction_time",
        )
    else:
        return MultiTableMetadata(
            {
                "customers": {
                    "customer_id": "Integer",
                    "zip_code": "Categorical",
                    "join_date": "Datetime",
                    "birthday": "Datetime",
                },
                "products": {
                    "product_id": "Integer",
                    "brand": "Categorical",
                },
                "sessions": {
                    "session_id": "Integer",
                    "customer_id": "Integer",
                    "device": "Categorical",
                    "session_start": "Datetime",
                },
                "transactions": {
                    "transaction_id": "Integer",
                    "session_id": "Integer",
                    "product_id": "Integer",
                    "amount": "Double",
                    "transaction_time": "Datetime",
                },
            },
            primary_keys={
                "customers": "customer_id",
                "products": "product_id",
                "sessions": "session_id",
                "transactions": "transaction_id",
            },
            time_indices={
                "customers": "birthday",
                "products": None,
                "sessions": "session_start",
                "transactions": "transaction_time",
            },
        )


def random_dates(start, end, n):
    """
    Generate n random dates between start and end (inclusive).

    Parameters:
        start (str or datetime): Start date in string or datetime format.
        end (str or datetime): End date in string or datetime format.
        n (int): Number of dates to generate.

    Returns:
        pd.Series: datetimes containing n random dates between start and end.
    """
    start_u = pd.Timestamp(start).value // 10**9
    end_u = pd.Timestamp(end).value // 10**9
    return pd.to_datetime(np.random.randint(start_u, end_u, n), unit="s")
