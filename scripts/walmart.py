import pandas as pd
import pyarrow as pa


def create_walmart_parquets():
    sales = pd.read_csv("train.csv", dtype_backend="pyarrow")
    stores = pd.read_csv("stores.csv", dtype_backend="pyarrow")
    features = pd.read_csv("features.csv", dtype_backend="pyarrow")

    pa_type = pd.ArrowDtype(pa.timestamp("s"))

    sales["Date"] = sales["Date"].astype(pa_type)
    features["Date"] = features["Date"].astype(pa_type)

    sales.to_parquet("sales.parquet")
    stores.to_parquet("stores.parquet")
    features.to_parquet("features.parquet")


create_walmart_parquets()
