from .subops import SubOperation
from ..utils.table_meta import TableMeta as tm

"""
Module functions that act as possible FilterOperations.
There are two steps involved in adding a new method.
1. Create a new function.
2. Create a new mapping in the dictionary with the function inside the SubOperation class.
"""
def allpass(df, **kwargs):
	raise NotImplementedException

def equals(df, **kwargs):
	return val == param

def not_equals(df, **kwargs):
	return val != param

def less_than(df, **kwargs):
	return val < param

def greater_than(df, **kwargs):
	return val > param

possible_operations = {
"all": SubOperation("all", allpass, param_placeholder),
"equals" : SubOperation("equals", equals, param_placeholder),
"not equals" : SubOperation("not equals", not_equals, param_placeholder),
"less than" : SubOperation("less than", less_than, param_placeholder),
"greater than" : SubOperation("greater than", greater_than, param_placeholder)
}

operation_io_types = {
	"all": [(tm.TYPE_VALUE, tm.TYPE_VALUE)],
	"equals": [(tm.TYPE_VALUE, tm.TYPE_VALUE)],
	"not equals": [(tm.TYPE_VALUE, tm.TYPE_VALUE)],
	"less than": [(tm.TYPE_VALUE, tm.TYPE_VALUE)],
	"greater than": [(tm.TYPE_VALUE, tm.TYPE_VALUE)]
}

operation_params = {
	"all": [],
	"equals": ["threshold"],
	"not equals": ["threshold"],
	"less than": ["threshold"],
	"greater than": ["threshold"]
}
