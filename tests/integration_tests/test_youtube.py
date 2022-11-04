import pandas as pd 
import numpy as np
import os 
from datetime import datetime, timedelta
import json 
import pytest

import trane


@pytest.fixture
def current_dir():
    return os.path.dirname(__file__)

@pytest.fixture
def df(current_dir):
    datetime_col = 'trending_date'
    filename = 'USvideos.csv'
    df = pd.read_csv(os.path.join(current_dir, filename))
    df[datetime_col] = pd.to_datetime(df[datetime_col], format="%y.%d.%m")
    df = df.sort_values(by=[datetime_col])
    df = df.fillna(0)
    return df

@pytest.fixture
def meta(current_dir):
    filename = "meta_youtube.json"
    meta_fp = os.path.join(current_dir, filename)
    meta = trane.TableMeta(json.loads(open(meta_fp).read()))
    return meta

def test_youtube(df, meta):
    entity = "category_id"
    time = "trending_date"
    cutoff = '4d'
    cutoff_base = pd.Timestamp(datetime.strptime("2017-11-14", "%Y-%m-%d"))
    cutoff_end = pd.Timestamp(datetime.strptime("2018-06-14", "%Y-%m-%d"))
    cutoff_strategy = trane.FixWindowCutoffStrategy(entity, cutoff, cutoff_base, cutoff_end)

    problem_generator = trane.PredictionProblemGenerator(table_meta=meta, 
                                                         entity_col=entity,
                                                         cutoff_strategy=cutoff_strategy,
                                                         time_col=time)
    problems = problem_generator.generate(df, generate_thresholds=True)
    for p in problems:
        try:
            x = p.execute(df,-1)
            problem_label_dict[str(p)]=x
        except:
            pass