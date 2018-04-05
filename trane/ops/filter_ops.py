from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase

FILTER_OPS = ["AllFilterOp", "GreaterFilterOp",
			  "EqFilterOp", "NeqFilterOp", "LessFilterOp"]
__all__ = ["FilterOpBase", "FILTER_OPS"] + FILTER_OPS
import logging

class FilterOpBase(OpBase):
	"""super class for all Filter Operations. (deprecated)"""
	pass


class AllFilterOp(FilterOpBase):
	REQUIRED_PARAMETERS = []
	IOTYPES = [(TM.TYPE_CATEGORY, TM.TYPE_CATEGORY), (TM.TYPE_BOOL, TM.TYPE_BOOL),
			   (TM.TYPE_ORDERED, TM.TYPE_ORDERED), (TM.TYPE_TEXT, TM.TYPE_TEXT),
			   (TM.TYPE_INTEGER, TM.TYPE_INTEGER), (TM.TYPE_FLOAT, TM.TYPE_FLOAT),
			   (TM.TYPE_TIME, TM.TYPE_TIME), (TM.TYPE_IDENTIFIER, TM.TYPE_IDENTIFIER)]

	def execute(self, dataframe):
		return dataframe


class EqFilterOp(FilterOpBase):
	REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_INTEGER}]
	IOTYPES = [(TM.TYPE_BOOL, TM.TYPE_BOOL), (TM.TYPE_INTEGER,
											  TM.TYPE_INTEGER), (TM.TYPE_FLOAT, TM.TYPE_FLOAT)]

	def execute(self, dataframe):
		dataframe = dataframe.copy()
		return dataframe[dataframe[self.column_name] == self.hyper_parameter_settings["threshold"]]


class NeqFilterOp(FilterOpBase):
	REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_INTEGER}]
	IOTYPES = [(TM.TYPE_BOOL, TM.TYPE_BOOL), (TM.TYPE_INTEGER,
											  TM.TYPE_INTEGER), (TM.TYPE_FLOAT, TM.TYPE_FLOAT)]

	def execute(self, dataframe):
		dataframe = dataframe.copy()
		return dataframe[dataframe[self.column_name] != self.hyper_parameter_settings["threshold"]]


class GreaterFilterOp(FilterOpBase):
	REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_INTEGER}]
	IOTYPES = [(TM.TYPE_BOOL, TM.TYPE_BOOL), (TM.TYPE_INTEGER,
											  TM.TYPE_INTEGER), (TM.TYPE_FLOAT, TM.TYPE_FLOAT)]

	def execute(self, dataframe):
		dataframe = dataframe.copy()
		return dataframe[dataframe[self.column_name] > self.hyper_parameter_settings["threshold"]]


class LessFilterOp(FilterOpBase):
	REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_INTEGER}]
	IOTYPES = [(TM.TYPE_BOOL, TM.TYPE_BOOL), (TM.TYPE_INTEGER,
											  TM.TYPE_INTEGER), (TM.TYPE_FLOAT, TM.TYPE_FLOAT)]

	def execute(self, dataframe):
		dataframe = dataframe.copy()
		return dataframe[dataframe[self.column_name] < self.hyper_parameter_settings["threshold"]]
