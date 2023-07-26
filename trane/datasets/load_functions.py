import os

import pandas as pd

from trane.typing.column_schema import ColumnSchema
from trane.typing.logical_types import (
    Categorical,
    Datetime,
    Double,
    Integer,
)


def load_covid():
    filepath = generate_local_filepath("covid19.csv")
    df = pd.read_csv(filepath)
    df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%y")
    df = df[
        [
            "Country/Region",
            "Date",
            "Province/State",
            "Lat",
            "Long",
            "Confirmed",
            "Deaths",
            "Recovered",
        ]
    ]
    df = df.fillna(0)
    df = df.sort_values(by=["Date"])
    df = df.reset_index(drop=True)
    return df


def load_youtube():
    time_col = "trending_date"
    filepath = generate_local_filepath("USvideos.csv")
    df = pd.read_csv(filepath)
    df[time_col] = pd.to_datetime(df[time_col], format="%y.%d.%m")
    df = df.sort_values(by=[time_col])
    df = df.fillna(0)
    return df


def load_covid_metadata():
    table_meta = {
        "Province/State": ColumnSchema(
            logical_type=Categorical,
            semantic_tags={"category"},
        ),
        "Country/Region": ColumnSchema(
            logical_type=Categorical,
            semantic_tags={"category", "primary_key"},
        ),
        "Lat": ColumnSchema(logical_type=Double, semantic_tags={"numeric"}),
        "Long": ColumnSchema(logical_type=Double, semantic_tags={"numeric"}),
        "Date": ColumnSchema(logical_type=Datetime),
        "Confirmed": ColumnSchema(logical_type=Integer, semantic_tags={"numeric"}),
        "Deaths": ColumnSchema(logical_type=Integer, semantic_tags={"numeric"}),
        "Recovered": ColumnSchema(logical_type=Integer, semantic_tags={"numeric"}),
    }
    return table_meta


def load_youtube_metadata():
    table_meta = {
        "trending_date": ColumnSchema(logical_type=Datetime),
        "channel_title": ColumnSchema(
            logical_type=Categorical,
            semantic_tags={"primary_key"},
        ),
        "category_id": ColumnSchema(
            logical_type=Categorical,
            semantic_tags={"category", "primary_key"},
        ),
        "views": ColumnSchema(logical_type=Integer, semantic_tags={"numeric"}),
        "likes": ColumnSchema(logical_type=Integer, semantic_tags={"numeric"}),
        "dislikes": ColumnSchema(logical_type=Integer, semantic_tags={"numeric"}),
        "comment_count": ColumnSchema(logical_type=Integer, semantic_tags={"numeric"}),
    }
    return table_meta


def generate_local_filepath(key):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(dir_path, key)
