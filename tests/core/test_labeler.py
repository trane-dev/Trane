import sys
sys.path.insert(0, '/Users/Alexander/Documents/School/MEng Fall - 2017/MEng Thesis Work/Trane__HDI_REPO/')
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

#INPUTS
json_prediction_problems_filename = "test_data/prediction_problems.json"
multiple_csv = ["test_data/synthetic_taxi_data.csv"]
global_entity_id_column = "taxi_id"
global_time_column = "trip_id"
table_meta_filename = "test_data/taxi_meta.json"

json_str = open(table_meta_filename).read()
global_table_meta = TableMeta.from_json(json_str)
denormalized_dataframe = trane.csv_to_df(multiple_csv)
entity_to_data_dict = trane.df_group_by_entity_id(denormalized_dataframe, global_entity_id_column)
entity_id_to_data_and_cutoff_dict = trane.FixedCutoffTimes().generate_cutoffs(entity_to_data_dict)

labeler = Labeler()

entity_id_to_labels_and_cutoffs = labeler.execute(entity_id_to_data_and_cutoff_dict, json_prediction_problems_filename)

#TESTING STRATEGY:
#Ensure that a prediction problem is applied correctly.
#Ensure output dimensions are the correct shapes.


def test_labeler_dims():
	entity_id_to_labels_and_cutoffs = labeler.execute(entity_id_to_data_and_cutoff_dict, json_prediction_problems_filename)
	assert(len(entity_id_to_labels_and_cutoffs) == len(entity_id_to_data_and_cutoff_dict))
	for i in range(5):
		assert(len(entity_id_to_labels_and_cutoffs[i]) == 2)
	
def test_labeler_apply():
	column_name = "fare"
	prediction_problem = PredictionProblem([AllFilterOp(column_name),
											EqRowOp(column_name),
											IdentityTransformationOp(column_name),
											LastAggregationOp(column_name)])
	json_data = prediction_problems_to_json([prediction_problem], global_table_meta, global_entity_id_column, column_name, global_time_column)

	with open('prediction_problems.json', 'w') as outfile:
		json.dump(json.loads(json_data), outfile)

	labeler.execute(entity_id_to_data_and_cutoff_dict, 'prediction_problems.json')

	
	

test_labeler_apply()