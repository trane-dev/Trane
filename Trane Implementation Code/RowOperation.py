import Operation
#Child classes of the parent class, Operation
class RowOperation(Operation):
	
	#ENUMERATE POSSIBLE OPERATION TYPES
	operation_types = {
		'Identity': 
	}
	"""
	Args:
	    (List) column_names: a list of strings representing the column names
	Returns:
		None
	Raises:
	    None
	"""
	def __init__(self, column_names):
		self.column_names = column_names
