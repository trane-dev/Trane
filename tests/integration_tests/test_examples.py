import json
import os
from datetime import datetime

import pandas as pd
import pytest

import trane
from trane import (
    load_bike,
    load_covid,
    load_covid_tablemeta,
    load_youtube,
    load_youtube_metadata,
)

from .utils import generate_and_verify_prediction_problem


@pytest.fixture
def current_dir():
    return os.path.dirname(__file__)


@pytest.fixture
def df_youtube():
    df = load_youtube()
    return df


@pytest.fixture
def meta_youtube():
    return load_youtube_metadata()


@pytest.fixture
def df_covid():
    datetime_col = "Date"
    df = load_covid()
    # to speed up things as covid dataset takes awhile
    df = df.sample(frac=0.25, random_state=1)
    df = df.sort_values(by=[datetime_col])
    df = df.fillna(0)
    return df


@pytest.fixture
def meta_covid():
    return load_covid_tablemeta()


@pytest.fixture
def covid_cutoff_strategy(df_covid, meta_covid, sample):
    entity_col = "Country/Region"
    cutoff = "2d"
    cutoff_base = str(datetime.strptime("2020-01-22", "%Y-%m-%d"))
    cutoff_end = str(datetime.strptime("2020-03-29", "%Y-%m-%d"))
    cutoff_strategy = trane.CutoffStrategy(
        entity_col=entity_col,
        window_size=cutoff,
        minimum_data=cutoff_base,
        maximum_data=cutoff_end,
    )
    return cutoff_strategy


@pytest.fixture
def df_chicago():
    df = load_bike()
    return df


@pytest.fixture
def meta_chicago(current_dir):
    filename = "meta_chicago.json"
    meta_fp = os.path.join(current_dir, filename)
    meta_covid = trane.TableMeta(json.loads(open(meta_fp).read()))
    return meta_covid


def test_youtube(df_youtube, meta_youtube, sample):
    entity_col = "category_id"
    time_col = "trending_date"
    cutoff = "4d"
    cutoff_base = pd.Timestamp(datetime.strptime("2017-11-14", "%Y-%m-%d"))
    cutoff_end = pd.Timestamp(datetime.strptime("2018-06-14", "%Y-%m-%d"))
    cutoff_strategy = trane.CutoffStrategy(
        entity_col=entity_col,
        window_size=cutoff,
        minimum_data=cutoff_base,
        maximum_data=cutoff_end,
    )
    prediction_problem_to_label_times = generate_and_verify_prediction_problem(
        df=df_youtube,
        meta=meta_youtube,
        entity_col=entity_col,
        time_col=time_col,
        cutoff_strategy=cutoff_strategy,
        sample=sample,
    )

    ft_wrapper = trane.FeaturetoolsWrapper(
        df=df_youtube,
        entity_col=entity_col,
        time_col=time_col,
        name="youtube",
    )
    for problem_str in prediction_problem_to_label_times:
        print(problem_str)
        label_times = prediction_problem_to_label_times[problem_str]
        features = ft_wrapper.compute_features(label_times, cutoff)
        print(features)


def test_covid(df_covid, covid_cutoff_strategy, meta_covid, sample):
    time_col = "Date"
    generate_and_verify_prediction_problem(
        df=df_covid,
        meta=meta_covid,
        entity_col=covid_cutoff_strategy.entity_col,
        time_col=time_col,
        cutoff_strategy=covid_cutoff_strategy,
        sample=sample,
    )


def test_covid_multi(df_covid, covid_cutoff_strategy, meta_covid, sample):
    time_col = "Date"
    generate_and_verify_prediction_problem(
        df=df_covid,
        meta=meta_covid,
        entity_col=covid_cutoff_strategy.entity_col,
        time_col=time_col,
        cutoff_strategy=covid_cutoff_strategy,
        sample=sample,
        use_multiprocess=True,
    )


def test_chicago(df_chicago, meta_chicago, sample):
    entity_col = "usertype"
    time_col = "date"
    cutoff = "1h"
    cutoff_base = pd.Timestamp(datetime.strptime("2017-01-02", "%Y-%m-%d"))
    cutoff_end = pd.Timestamp(datetime.strptime("2017-01-31", "%Y-%m-%d"))
    cutoff_strategy = trane.CutoffStrategy(
        entity_col=entity_col,
        window_size=cutoff,
        minimum_data=cutoff_base,
        maximum_data=cutoff_end,
    )
    generate_and_verify_prediction_problem(
        df=df_chicago,
        meta=meta_chicago,
        entity_col=entity_col,
        time_col=time_col,
        cutoff_strategy=cutoff_strategy,
        sample=sample,
    )
