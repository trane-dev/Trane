# coding: utf-8

# In[1]:


import json

import pandas as pd
from datetime import datetime, timedelta

import trane
import featuretools as ft
import numpy as np


# In[2]:


def solve(entity_col, window, fwindow):
    df = pd.read_csv("USvideos.csv")
    df["trending_date"] = df["trending_date"].apply(
        lambda x: datetime.strptime(x, "%y.%d.%m"),
    )
    df = df.sort_values(by=["trending_date"])

    channel_to_id = {}
    id_to_channel = []
    n_channel = 0
    for cc in set(df["channel_title"]):
        channel_to_id[cc] = n_channel
        id_to_channel.append(cc)
        n_channel += 1
    df["channel_title"] = df["channel_title"].apply(lambda x: channel_to_id[x])

    meta = trane.TableMeta(json.loads(open("meta.json").read()))
    if entity_col == "__fake_root_entity__":
        df, meta = trane.overall_prediction_helper(df, meta)

    cutoff_base = datetime.strptime("2017-12-01", "%Y-%m-%d")
    cutoff_end = datetime.strptime("2018-06-01", "%Y-%m-%d")
    cutoff_strategy = trane.CutoffStrategy(
        entity_col,
        cutoff_base,
        cutoff_end,
        window,
    )

    problems = problem_generator.generate()

    evaluator = trane.PredictionProblemEvaluator(
        df,
        entity_col=entity_col,
        cutoff_strategy=cutoff_strategy,
    )

    result = trane.multi_process_evaluation(evaluator, problems, features)
    return result


if __name__ == "__main__":
    results = []
    for entity in ["category_id", "channel_title", "__fake_root_entity__"]:
        if entity == "channel_title":
            results += solve(entity, 28, 28)
        else:
            results += solve(entity, 1, 3)

    with open("youtube-evaluation.json") as f:
        json.dump(results, f)
