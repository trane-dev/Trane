import sys
sys.dont_write_bytecode = True
import pandas as pd
from PredictionProblemGenerator import PredictionProblemGenerator


DATAFRAME = pd.read_csv('../../test_datasets/synthetic_taxi_data.csv')

#inputs to the generator
LABEL_GENERATING_COLUMN_NAME = "fare"
ENTITY_ID_COLUMN_NAME = "taxi_id"
TIME_COLUMN_NAME = "time"

prediction_problem_generator = PredictionProblemGenerator(LABEL_GENERATING_COLUMN_NAME,
														ENTITY_ID_COLUMN_NAME,
														TIME_COLUMN_NAME)

prediction_problems = prediction_problem_generator.generate()

print len(prediction_problems)
# print str(prediction_problems[0])
