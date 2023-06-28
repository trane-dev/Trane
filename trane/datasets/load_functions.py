import os

import pandas as pd

from trane.utils import TableMeta


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


def load_bike():
    filepath = generate_local_filepath("bike-sampled.csv")
    df = pd.read_csv(filepath)
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df = df.sort_values(by=["date"])
    df = df.fillna(0)
    return df


def load_youtube():
    time_col = "trending_date"
    filepath = generate_local_filepath("USvideos.csv")
    df = pd.read_csv(filepath)
    df[time_col] = pd.to_datetime(df[time_col], format="%y.%d.%m")
    df = df.sort_values(by=[time_col])
    df = df.fillna(0)
    return df


def load_yelp():
    # Sampled Yelp Reviews.zip or Yelp Reviews.zip?
    filepath = generate_local_filepath("Yelp Reviews.zip")
    df = pd.read_csv(filepath)
    return df


def load_covid_tablemeta():
    metadata = {
        "tables": [
            {
                "fields": [
                    {"name": "Province/State", "type": "text"},
                    {"name": "Country/Region", "type": "text"},
                    {"name": "Lat", "type": "number", "subtype": "float"},
                    {"name": "Long", "type": "number", "subtype": "float"},
                    {"name": "Date", "type": "datetime"},
                    {"name": "Confirmed", "type": "number", "subtype": "integer"},
                    {"name": "Deaths", "type": "number", "subtype": "integer"},
                    {"name": "Recovered", "type": "number", "subtype": "integer"},
                ],
            },
        ],
    }
    return TableMeta(metadata)


def load_youtube_metadata():
    metadata = {
        "tables": [
            {
                "fields": [
                    {"name": "trending_date", "type": "time"},
                    {"name": "channel_title", "type": "id"},
                    {
                        "name": "category_id",
                        "type": "categorical",
                        "subtype": "categorical",
                    },
                    {"name": "views", "type": "categorical", "subtype": "number"},
                    {"name": "likes", "type": "categorical", "subtype": "integer"},
                    {"name": "dislikes", "type": "integer", "subtype": "number"},
                    {"name": "comment_count", "type": "integer", "subtype": "number"},
                ],
            },
        ],
    }
    return TableMeta(metadata)


def load_bike_metadata():
    metadata = {
        "tables": [
            {
                "fields": [
                    {"name": "date", "type": "time"},
                    {"name": "hour", "subtype": "categorical", "type": "categorical"},
                    {
                        "name": "usertype",
                        "subtype": "categorical",
                        "type": "categorical",
                    },
                    {"name": "gender", "subtype": "categorical", "type": "categorical"},
                    {"name": "tripduration", "subtype": "float", "type": "number"},
                    {"name": "temperature", "subtype": "float", "type": "number"},
                    {"name": "from_station_id", "type": "id"},
                    {
                        "name": "dpcapacity_start",
                        "subtype": "integer",
                        "type": "number",
                    },
                    {"name": "to_station_id", "type": "id"},
                    {"name": "dpcapacity_end", "subtype": "integer", "type": "number"},
                ],
            },
        ],
    }
    return TableMeta(metadata)


def generate_local_filepath(key):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(dir_path, key)


def generate_s3_url(key, bucket="trane-datasets"):
    return f"https://{bucket}.s3.amazonaws.com/{key}"
