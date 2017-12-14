from .ops import Operation
import pandas as pd
from . import aggregation_ops_module as ag
from ..utils.table_meta import TableMeta

class AggregationOperation(Operation):
	"""
	Aggregation Operations are operations that take many rows and aggregate them
	into a single value.
	Transformation operations are similar to Aggregation operations, except that transformation operations must
	return a dataframe with more than 2 rows and an aggregation operaton must return a single row.
	"""

	"""
	Args:
		Takes as input a single aggregation operation to perform from the dictionary
		of possible aggregation operations defined above.
	Returns:
		None
	"""
	def __init__(self, column_name, aggregation_operation_name):
		self.aggregation_operation_name = aggregation_operation_name
		self.aggregation_operation = ag.possible_operations[aggregation_operation_name]
		self.column_name = column_name

	def preprocess(self, table_meta):
		self.input_meta = table_meta.copy()
		dtype = self.input_meta.get_type(self.column_name)
		for itype, otype in ag.operation_io_types[self.aggregation_operation_name]:
			if dtype == itype:
				self.itype = itype
				self.otype = otype
				table_meta.set_type(self.column_name, otype)
				return table_meta
		return None
	
	def execute(self, dataset):
		return self.aggregation_operation(dataset)

	def __str__(self):
		return "Aggregation operation (" + self.column_name + " " + self.aggregation_operation_name + ")"
	
	def generate_nl_description(self):
		if self.aggregation_operation_name == 'count':
			return "the number of"
		if self.aggregation_operation_name == 'sum' and self.itype == TableMeta.TYPE_BOOL:
			return "the number of"
		return "the %s of" % self.aggregation_operation_name

#SMALL TEST
# df = pd.DataFrame([[74, 200, 22, "Alex"],[71, 140, 19, "Shea"], [75, 170, 20, "Abby"]], columns = ['height', 'weight', 'age', 'name'])
# df.loc[3] = {"height":78, "name":"Future Alex", "weight":105, "age":25}
# agg_op = AggregationOperation("last minus first")
# print(agg_op.execute(df))
