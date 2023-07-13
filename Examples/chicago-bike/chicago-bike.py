import json

import pandas as pd
from datetime import datetime, timedelta

import trane
import featuretools as ft
import numpy as np


def solve(entity_col):
    df = pd.read_csv("data/bike-sampled.csv")
    df["date"] = df["date"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d"))
    df = df.sort_values(by=["date"])

    meta = trane.TableMeta(json.loads(open("data/meta.json").read()))
    if entity_col == "__fake_root_entity__":
        df, meta = trane.overall_prediction_helper(df, meta)

    # MAP str to int
    df_ft = df.copy()
    str_col_list = ["usertype", "gender"]
    str_mappers = {}
    for str_col in str_col_list:
        str_to_id = {}
        id_to_str = []
        n_entity = 0

        for item in set(df_ft[str_col]):
            str_to_id[item] = n_entity
            id_to_str.append(item)
            n_entity += 1

        if str_col == entity_col:
            df[str_col] = df[str_col].apply(lambda x: str_to_id[x])
        df_ft[str_col] = df_ft[str_col].apply(lambda x: str_to_id[x])
        str_mappers[str_col] = (str_to_id, id_to_str)

    cutoff_base = datetime.strptime("2017-01-06", "%Y-%m-%d")
    cutoff_end = datetime.strptime("2017-01-31", "%Y-%m-%d")
    cutoff_strategy = trane.CutoffStrategy(
        entity_col,
        cutoff_base,
        cutoff_end,
        1,
    )

    problem_generator = trane.PredictionProblemGenerator(
        table_meta=meta,
        entity_col=entity_col,
        time_col="date",
    )

    problems = problem_generator.generate()

    evaluator = trane.PredictionProblemEvaluator(
        df,
        entity_col=entity_col,
        cutoff_strategy=cutoff_strategy,
        min_train_set=5,
        min_test_set=5,
        previous_k_as_feature=2,
        latest_k_as_test=8,
    )

    result = trane.multi_process_evaluation(evaluator, problems, features)
    return result


if __name__ == "__main__":
    results = []
    for entity in ["from_station_id", "__fake_root_entity__"]:
        results += solve(entity)

    with open("youtube-evaluation.json") as f:
        json.dump(results, f)
