import sys
sys.dont_write_bytecode = True
import pandas as pd
from Trane.core.PredictionProblemGenerator import PredictionProblemGenerator
import logging
import json

if __name__ == '__main__':
	logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', 
						datefmt='%Y/%m/%d %H:%M:%S', level=logging.DEBUG)

	table_meta = open('../test_datasets/taxi_meta.json').read()
	table_meta = json.loads(table_meta)
	gen = PredictionProblemGenerator(table_meta, entity_id_column='taxi_id')
	cnt = 0
	for problem in gen.generate():
		print(str(problem))
		print(problem.generate_nl_description())
		cnt += 1
	logging.info("Generate %d problems." % cnt)


# DATAFRAME = pd.read_csv('../../test_datasets/synthetic_taxi_data.csv')
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
