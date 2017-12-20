import sys
sys.dont_write_bytecode = True
import pandas as pd
from trane.core.prediction_problem_generator import PredictionProblemGenerator
from trane.core.label_generator import LabelGenerator
import logging
import json
import numpy as np

if __name__ == '__main__':
	logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', 
						datefmt='%Y/%m/%d %H:%M:%S', level=logging.DEBUG)

	table_meta = open('../test_datasets/taxi_meta.json').read()
	table_meta = json.loads(table_meta)
	gen = PredictionProblemGenerator(table_meta, entity_id_column='taxi_id')
	cnt = 0
	lst = []
	for problem in gen.generate():
		lst.append(problem)
		cnt += 1

	logging.info("Generate %d problems." % cnt)

	label_gen = LabelGenerator(np.random.choice(lst, 5))
	jsonstr = label_gen.to_json()
	with open("tasks.json", "w") as f:
		print(jsonstr, file=f)
	# dataframe = pd.read_csv('../test_datasets/synthetic_taxi_data.csv')
	# results = label_gen.execute(dataframe)
	# print(results[0][1])
	
# 
# #inputs to the generator
# LABEL_GENERATING_COLUMN_NAME = "fare"
# ENTITY_ID_COLUMN_NAME = "taxi_id"
# TIME_COLUMN_NAME = "time"
# 
# prediction_problem_generator = PredictionProblemGenerator(LABEL_GENERATING_COLUMN_NAME,
# 														ENTITY_ID_COLUMN_NAME,
# 														TIME_COLUMN_NAME)
# 
# prediction_problems = prediction_problem_generator.generate()
# 
# print(len(prediction_problems))
# # print(str(prediction_problems[0]))
