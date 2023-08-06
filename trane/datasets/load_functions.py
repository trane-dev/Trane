import os

import pandas as pd


def load_airbnb_reviews():
    time_col = "date"
    filepath = generate_local_filepath("data/airbnb_reviews/airbnb_reviews.csv.bz2")
    df = pd.read_csv(filepath, dtype_backend="pyarrow")
    df = df.dropna()
    df[time_col] = pd.to_datetime(df[time_col], format="%Y-%m-%d")
    df = df.sort_values(by=["date"])

    return df


def generate_local_filepath(key):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(dir_path, key)
