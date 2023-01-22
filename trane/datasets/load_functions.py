import os

import pandas as pd

from trane.utils import TableMeta


def covid_metadata():
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
    return metadata


def load_covid_data():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    filepath = os.path.join(dir_path, "covid19.csv")
    df = pd.read_csv(filepath)
    df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%y")
    df = df.sort_values(by=["Date"])
    df = df.fillna(0)
    return df


def load_covid_tablemeta():
    return TableMeta(covid_metadata())
