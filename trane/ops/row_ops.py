from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase
import numpy as np
import logging

ROW_OPS = ["IdentityRowOp", "GreaterRowOp",
		   "EqRowOp", "NeqRowOp", "LessRowOp", "ExpRowOp"]
__all__ = ["RowOpBase", "ROW_OPS"] + ROW_OPS


class RowOpBase(OpBase):
	"""super class for all Row Operations. (deprecated)"""
	pass


class IdentityRowOp(RowOpBase):
	REQUIRED_PARAMETERS = []
	IOTYPES = [(TM.TYPE_CATEGORY, TM.TYPE_CATEGORY), (TM.TYPE_BOOL, TM.TYPE_BOOL),
			   (TM.TYPE_ORDERED, TM.TYPE_ORDERED), (TM.TYPE_TEXT, TM.TYPE_TEXT),
			   (TM.TYPE_INTEGER, TM.TYPE_INTEGER), (TM.TYPE_FLOAT, TM.TYPE_FLOAT),
			   (TM.TYPE_TIME, TM.TYPE_TIME), (TM.TYPE_IDENTIFIER, TM.TYPE_IDENTIFIER)]

	def execute(self, dataframe):
		return dataframe


class EqRowOp(RowOpBase):
	REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_INTEGER}]
	IOTYPES = [(TM.TYPE_BOOL, TM.TYPE_BOOL), (TM.TYPE_INTEGER,
											  TM.TYPE_BOOL), (TM.TYPE_FLOAT, TM.TYPE_BOOL)]

	def execute(self, dataframe):
		dataframe = dataframe.copy()
		dataframe[self.column_name] = dataframe[self.column_name].apply(
			lambda x: x == self.hyper_parameter_settings["threshold"])
		return dataframe


class NeqRowOp(RowOpBase):
	REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_INTEGER}]
	IOTYPES = [(TM.TYPE_BOOL, TM.TYPE_BOOL), (TM.TYPE_INTEGER,
											  TM.TYPE_BOOL), (TM.TYPE_FLOAT, TM.TYPE_BOOL)]

	def execute(self, dataframe):
		dataframe = dataframe.copy()
		dataframe[self.column_name] = dataframe[self.column_name].apply(
			lambda x: x != self.hyper_parameter_settings["threshold"])
		return dataframe


class GreaterRowOp(RowOpBase):
	REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_INTEGER}]
	IOTYPES = [(TM.TYPE_BOOL, TM.TYPE_BOOL), (TM.TYPE_INTEGER,
											  TM.TYPE_BOOL), (TM.TYPE_FLOAT, TM.TYPE_BOOL)]

	def execute(self, dataframe):
		dataframe = dataframe.copy()
		dataframe[self.column_name] = dataframe[self.column_name].apply(
			lambda x: x > self.hyper_parameter_settings["threshold"])
		return dataframe


class LessRowOp(RowOpBase):
	REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_INTEGER}]
	IOTYPES = [(TM.TYPE_BOOL, TM.TYPE_BOOL), (TM.TYPE_INTEGER,
											  TM.TYPE_BOOL), (TM.TYPE_FLOAT, TM.TYPE_BOOL)]

	def execute(self, dataframe):
		dataframe = dataframe.copy()
		dataframe[self.column_name] = dataframe[self.column_name].apply(
			lambda x: x < self.hyper_parameter_settings["threshold"])
		return dataframe


class ExpRowOp(RowOpBase):
	REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_INTEGER}]
	IOTYPES = [(TM.TYPE_INTEGER, TM.TYPE_FLOAT),
			   (TM.TYPE_FLOAT, TM.TYPE_FLOAT)]

	def execute(self, dataframe):
		dataframe = dataframe.copy()
		dataframe[self.column_name] = dataframe[
			self.column_name].apply(lambda x: np.exp(x))
		return dataframe
