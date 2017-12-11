from TableMeta import TableMeta as tm

"""
Module functions:
These are the Transformation operations possible under the Transformation Operation class.
Methods can be added here under 2 constraints.
1. Create a function with the dataframe as input and return a new dataframe.
2. Add the function to the dictionary of possible operations.
"""
def identity(dataframe):
	return dataframe

def diff(dataframe, column_name):
	df = dataframe.copy()
	df[column_name] = df[column_name].diff()
	df = df.dropna()
	return df

possible_operations = {
	"identity" : identity,
	"diff": diff
}

operation_io_types = {
	"identity" : [(tm.TYPE_VALUE, tm.TYPE_VALUE), (tm.TYPE_BOOL, tm.TYPE_BOOL)],
	"diff": [(tm.TYPE_VALUE, tm.TYPE_VALUE)]
}
