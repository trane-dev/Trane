from datetime import datetime

import pandas as pd


def clean_date(date):
    if isinstance(date, str):
        return pd.Timestamp(datetime.strptime(date, "%Y-%m-%d"))
    return date
