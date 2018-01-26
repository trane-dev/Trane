# import sys
# sys.path.insert(0, '/Users/Alexander/Documents/Trane__HDI_REPO/')
from trane.core.labeler import *
import trane
from trane.core.prediction_problem import PredictionProblem
from trane.ops.row_ops import *
from trane.ops.filter_ops import *
from trane.ops.transformation_ops import *
from trane.ops.aggregation_ops import *
from trane.utils.table_meta import TableMeta
from trane.core.prediction_problem_saver import *
import json
import pandas as pd
import os

"""TESTING STRATEGY:
Function: execute()
1. Ensure that a prediction problem is applied correctly.
2. Ensure output dimensions are the correct shapes.
3. Ensure all other values are un-changed (entity_id and cutoff_time)
"""

meta_json_str = '[{"name": "vendor_id", "type": "identifier"}, {"name": "taxi_id", "type": "identifier"}, {"name": "trip_id", "type": "time"}, {"name": "distance", "type": "value"}, {"name": "duration", "type": "value"}, {"name": "fare", "type": "value"}, {"name": "num_passengers", "type": "value"}]'
dataframe = pd.DataFrame([(0, 0, 0, 5.32, 19.7, 53.89, 1), 
							(0, 0, 1, 1.08, 6.78, 18.89, 2),
							(0, 0, 2, 4.69, 14.11, 41.35, 4)], 
							columns = ["vendor_id", "taxi_id", "trip_id", "distance", "duration", "fare", "num_passengers"])
def test_labeler_apply():
	entity_id_column = "taxi_id"
	time_column = "trip_id"
	label_generating_column = "fare"
	table_meta = TableMeta.from_json(meta_json_str)
	labeler = Labeler()
	df = dataframe
	entity_to_data_dict = trane.df_group_by_entity_id(df, entity_id_column)
	entity_id_to_data_and_cutoff_dict = trane.FixedCutoffTimes().generate_cutoffs(entity_to_data_dict)


	prediction_problem = PredictionProblem([AllFilterOp(label_generating_column),
											IdentityRowOp(label_generating_column),
											IdentityTransformationOp(label_generating_column),
											LastAggregationOp(label_generating_column)])
	
	filename = "prediction_problem.json"

	prediction_problems_to_json_file([prediction_problem], table_meta, entity_id_column, label_generating_column, time_column, filename)

	input_ = entity_id_to_data_and_cutoff_dict
	expected = {0: ([41.35], 0)}
	found = labeler.execute(entity_id_to_data_and_cutoff_dict, filename)

	os.remove(filename)

	assert(expected == found)

	assert(len(expected) == len(found))
	assert(len(input_) == len(found))
	for i in range(len(found)):
		assert(len(found[i]) == 2)
	
	for entity_id in input_:
		input_cutoff_time = input_[entity_id][1]
		output_cutoff_time = found[entity_id][1]
		assert(input_cutoff_time == output_cutoff_time)
















