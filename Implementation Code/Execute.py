import pandas as pd
from PredictionProblemGenerator import PredictionProblemGenerator

#inputs to the generator
DATAFRAME = pd.read_csv('../../test_datasets/synthetic_taxi_data.csv')
LABEL_GENERATING_COLUMN_NAME = "fare"
ENTITY_ID_COLUMN_NAME = "taxi_id"
TIME_COLUMN_NAME = "time"

prediction_problem_generator = PredictionProblemGenerator(DATAFRAME,
														LABEL_GENERATING_COLUMN_NAME,
														ENTITY_ID_COLUMN_NAME,
														TIME_COLUMN_NAME)

prediction_problems = prediction_problem_generator.generate()

# print str(prediction_problems[0])
