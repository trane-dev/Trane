import pandas as pd 
import numpy as np
import os 
from datetime import datetime
import json 
import pytest

import trane

@pytest.fixture
def current_dir():
	return os.path.dirname(__file__)

@pytest.fixture
def df(current_dir):
    datetime_col = 'trending_date'
    filename = 'covid19.csv'
	df = pd.read_csv(os.path.join(current_dir, filename))
	df[datetime_col] = pd.to_datetime(df[datetime_col], format="%m/%d/%y")
	df = df.sort_values(by=[datetime_col])
	df = df.fillna(0)
	return df

@pytest.fixture
def meta_covid(current_dir):
	filename = 'meta_covid.json'
	meta_fp = os.path.join(current_dir, filename)
	meta_covid = trane.TableMeta(json.loads(open(meta_fp).read()))
	return meta_covid

def test_covid_dataset(df, meta_covid):
	entity = 'Country/Region'
	time = 'Date'
	cutoff = '2d'
	cutoff_base = str(datetime.strptime("2020-01-22", "%Y-%m-%d"))
	cutoff_end = str(datetime.strptime("2020-03-29", "%Y-%m-%d"))

	cutoff_strategy = trane.FixWindowCutoffStrategy(entity, cutoff, cutoff_base, cutoff_end, cutoff)
	problem_generator = trane.PredictionProblemGenerator(table_meta=meta_covid,
														 entity_col=entity,
														 time_col=time,
														 cutoff_strategy=cutoff_strategy)
	problems = problem_generator.generate(df, generate_thresholds=True)
	for p in problems:
		try:
			x = p.execute(df,-1)
			problem_label_dict[str(p)]=x
		except:
			pass