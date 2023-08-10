import os

import pandas as pd


def load_airbnb_reviews():
    time_col = "date"
    filepath = generate_local_filepath("data/airbnb_reviews/airbnb_reviews.csv")
    df = pd.read_csv(filepath, dtype_backend="pyarrow")
    df = df.dropna()
    df[time_col] = pd.to_datetime(df[time_col], format="%Y-%m-%d")
    df = df.sort_values(by=["date"])

    return df


def load_store():
    filepaths = {
        "categories": generate_local_filepath("data/store/categories.csv"),
        "cust_hist": generate_local_filepath("data/store/cust_hist.csv"),
        "customers": generate_local_filepath("data/store/customers.csv"),
        "inventory": generate_local_filepath("data/store/inventory.csv"),
        "orderlines": generate_local_filepath("data/store/orderlines.csv"),
        "orders": generate_local_filepath("data/store/orders.csv"),
        "products": generate_local_filepath("data/store/products.csv"),
        "reorder": generate_local_filepath("data/store/reorder.csv"),
    }

    dataframes = {}
    for filepath in filepaths.items():
        dataframes[filepath[0]] = (
            pd.read_csv(filepath[1], dtype_backend="pyarrow"),
            "id",
        )

    relationships = [
        ("customers", "customerid", "cust_hist", "customerid"),
        ("products", "prod_id", "cust_hist", "prod_id"),
        ("products", "prod_id", "inventory", "prod_id"),
        ("products", "prod_id", "orderlines", "prod_id"),
        ("orders", "orderid", "orderlines", "orderid"),
        ("customers", "customerid", "orders", "customerid"),
        ("categories", "category", "products", "category"),
    ]

    return dataframes, relationships


def generate_local_filepath(key):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(dir_path, key)
