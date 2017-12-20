import sys
sys.dont_write_bytecode = True
import pandas as pd
from trane.core.label_generator import LabelGenerator
import logging
import json
import numpy as np

if __name__ == '__main__':
	logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', 
						datefmt='%Y/%m/%d %H:%M:%S', level=logging.DEBUG)
	
	jsonstr = open("tasks_pretty.json").read()
	label_gen = LabelGenerator.from_json(jsonstr)
	# dataframe = pd.read_csv('../test_datasets/synthetic_taxi_data.csv')
	dataframe = pd.read_csv('../test_datasets/donations_sample.csv')
	results = label_gen.execute(dataframe)
	for prob, label in results:
		print(str(prob))
		print(prob.generate_nl_description())
		print(label)
		print()
