import os

import pytest

import trane
from trane.datasets.load_functions import (
    load_covid,
    load_covid_metadata,
    load_youtube,
    load_youtube_metadata,
)

from .utils import generate_and_verify_prediction_problem


@pytest.fixture
def current_dir():
    return os.path.dirname(__file__)


def test_youtube(sample):
    df_youtube = load_youtube()
    meta_youtube = load_youtube_metadata()

    entity_col = "category_id"
    time_col = "trending_date"
    cutoff = "4d"
    cutoff_base = "2017-11-14"
    cutoff_end = "2018-06-14"
    cutoff_strategy = trane.CutoffStrategy(
        entity_col=entity_col,
        window_size=cutoff,
        minimum_data=cutoff_base,
        maximum_data=cutoff_end,
    )
    generate_and_verify_prediction_problem(
        df=df_youtube,
        meta=meta_youtube,
        entity_col=entity_col,
        time_col=time_col,
        cutoff_strategy=cutoff_strategy,
        sample=sample,
        use_multiprocess=False,
    )


def test_covid(sample):
    df_covid = load_covid()
    meta_covid = load_covid_metadata()

    entity_col = "Country/Region"
    time_col = "Date"
    cutoff = "2d"
    cutoff_base = "2020-01-22"
    cutoff_end = "2020-03-29"
    cutoff_strategy = trane.CutoffStrategy(
        entity_col=entity_col,
        window_size=cutoff,
        minimum_data=cutoff_base,
        maximum_data=cutoff_end,
    )
    generate_and_verify_prediction_problem(
        df=df_covid,
        meta=meta_covid,
        entity_col=entity_col,
        time_col=time_col,
        cutoff_strategy=cutoff_strategy,
        sample=sample,
        use_multiprocess=False,
    )


@pytest.fixture
def covid_cutoff_strategy(df_covid, meta_covid, sample):
    entity_col = "Country/Region"
    cutoff = "2d"
    cutoff_base = "2020-01-22"
    cutoff_end = "2020-03-29"
    cutoff_strategy = trane.CutoffStrategy(
        entity_col=entity_col,
        window_size=cutoff,
        minimum_data=cutoff_base,
        maximum_data=cutoff_end,
    )
    return cutoff_strategy
