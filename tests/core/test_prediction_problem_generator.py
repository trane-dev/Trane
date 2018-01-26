# import sys
# sys.path.insert(0, '/Users/Alexander/Documents/Trane__HDI_REPO/')

"""TESTING STRATEGY
Function: generate(self)
1. Ensure the correct # of problems are generated.
2. Generated problems are of proper type
3. Ensure prediction problems are generated in order of Filter->Row->Transformation->Aggregation
"""

from trane.core.prediction_problem import *
from trane.core.prediction_problem_generator import *
from trane.utils.table_meta import TableMeta
from trane.ops.row_ops import *
from trane.ops.filter_ops import *
from trane.ops.transformation_ops import *
from trane.ops.aggregation_ops import *

meta_json_str = '[{"name": "vendor_id", "type": "identifier"}, {"name": "taxi_id", "type": "identifier"}, {"name": "trip_id", "type": "time"}, {"name": "distance", "type": "value"}, {"name": "duration", "type": "value"}, {"name": "fare", "type": "value"}, {"name": "num_passengers", "type": "value"}]'

def test_number_of_problems_generated():
	table_meta = TableMeta.from_json(meta_json_str)
	entity_id_column = "taxi_id"
	label_generating_column = "fare"
	time_column = "trip_id"
	ppg = PredictionProblemGenerator(table_meta, entity_id_column, label_generating_column, time_column)
	generator = ppg.generate()

	
	expected = 640 #THIS NUMBER WILL CHANGE IF MORE OPERATIONS ARE ADDED
	found = len(list(generator))

	assert(expected == found)


def test_generated_types():
	table_meta = TableMeta.from_json(meta_json_str)
	entity_id_column = "taxi_id"
	label_generating_column = "fare"
	time_column = "trip_id"
	ppg = PredictionProblemGenerator(table_meta, entity_id_column, label_generating_column, time_column)
	generator = ppg.generate()	

	expected = PredictionProblem
	problems = [prob for prob in generator]
	
	for problem in problems:
		found = problem
		assert(type(found) is expected)
		
def test_order_of_operations():
	table_meta = TableMeta.from_json(meta_json_str)
	entity_id_column = "taxi_id"
	label_generating_column = "fare"
	time_column = "trip_id"
	ppg = PredictionProblemGenerator(table_meta, entity_id_column, label_generating_column, time_column)
	generator = ppg.generate()	

	problems = [prob for prob in generator]
	
	for problem in problems:
		ops = problem.operations
		assert(len(ops) == 4)
		first_op = ops[0]
		second_op = ops[1]
		third_op = ops[2]
		fourth_op = ops[3]

		assert(issubclass(first_op.__class__, FilterOpBase))
		assert(issubclass(second_op.__class__, RowOpBase))
		assert(issubclass(third_op.__class__, TransformationOpBase))
		assert(issubclass(fourth_op.__class__, AggregationOpBase))
		
		













