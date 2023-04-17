import pandas as pd

from trane.utils import TableMeta


def load_covid():
    filepath = generate_s3_url("covid19.csv")
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


def load_flight():
    filepath = generate_s3_url("airlines.csv")
    airlines_df = pd.read_csv(filepath)

    filepath = generate_s3_url("airports.csv")
    airport_df = pd.read_csv(filepath)

    filepath = generate_s3_url("flight-sampled.csv")
    flights_df = pd.read_csv(filepath)
    flights_df["DATE"] = pd.to_datetime(flights_df["DATE"])

    return airlines_df, airport_df, flights_df


def load_bike():
    filepath = generate_s3_url("bike-sampled.csv")
    df = pd.read_csv(filepath)
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df = df.sort_values(by=["date"])
    df = df.fillna(0)
    return df


def load_youtube():
    time_col = "trending_date"
    filepath = generate_s3_url("USvideos.csv")
    df = pd.read_csv(filepath)
    df[time_col] = pd.to_datetime(df[time_col], format="%y.%d.%m")
    df = df.sort_values(by=[time_col])
    df = df.fillna(0)
    return df


def load_yelp():
    # Sampled Yelp Reviews.zip or Yelp Reviews.zip?
    filepath = generate_s3_url("Yelp Reviews.zip")
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


def generate_s3_url(key, bucket="trane-datasets"):
    return f"https://{bucket}.s3.amazonaws.com/{key}"
