from Operation import Operation
import pandas as pd
from SubOperation import SubOperation
class MultiRowOperation(Operation):
	
	#MULTIROW OPERATION TO BE APPLIED TO ALL COLUMNS FOR ALL ENTITIES

	"""
	Args:
	    (Dict) operation_columns_to_sub_operation: Keys are the subset of all columns that 
	    	will be operated on by an operator. Values are the operation to be applied for
	    	the column. Operations are of type SubOperation that take as input the value they need.
	Returns:
		None
	Raises:
	    None
	"""
	def __init__(self, column_names_to_sub_operation):
		