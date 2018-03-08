from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase

FILTER_OPS = ["AllFilterOp", "GreaterFilterOp", "EqFilterOp", "NeqFilterOp", "LessFilterOp"]
__all__ = ["FilterOpBase", "FILTER_OPS"] + FILTER_OPS


class FilterOpBase(OpBase):
    """super class for all Filter Operations. (deprecated)"""
    pass

class AllFilterOp(FilterOpBase):
    PARAMS = [{}, {}]
    IOTYPES = [(TM.TYPE_CATEGORY, TM.TYPE_CATEGORY), (TM.TYPE_BOOL, TM.TYPE_BOOL),
                (TM.TYPE_ORDERED, TM.TYPE_ORDERED), (TM.TYPE_TEXT, TM.TYPE_TEXT),
                (TM.TYPE_INTEGER, TM.TYPE_INTEGER), (TM.TYPE_FLOAT, TM.TYPE_FLOAT),
                (TM.TYPE_TIME, TM.TYPE_TIME), (TM.TYPE_IDENTIFIER, TM.TYPE_IDENTIFIER)]
    def execute(self, dataframe):
        return dataframe

class EqFilterOp(FilterOpBase):
    PARAMS = [{"threshold": TM.TYPE_INTEGER}]
    IOTYPES = [(TM.TYPE_BOOL, TM.TYPE_BOOL), (TM.TYPE_INTEGER, TM.TYPE_INTEGER), (TM.TYPE_FLOAT, TM.TYPE_FLOAT)]
    
    def execute(self, dataframe):
        return dataframe[dataframe[self.column_name] == self.param_values["threshold"]]

class NeqFilterOp(FilterOpBase):
    PARAMS = [{"threshold": TM.TYPE_INTEGER}]
    IOTYPES = [(TM.TYPE_BOOL, TM.TYPE_BOOL), (TM.TYPE_INTEGER, TM.TYPE_INTEGER), (TM.TYPE_FLOAT, TM.TYPE_FLOAT)]
    def execute(self, dataframe):
        return dataframe[dataframe[self.column_name] != self.param_values["threshold"]]

class GreaterFilterOp(FilterOpBase):
    PARAMS = [{"threshold": TM.TYPE_INTEGER}]
    IOTYPES = [(TM.TYPE_BOOL, TM.TYPE_BOOL), (TM.TYPE_INTEGER, TM.TYPE_INTEGER), (TM.TYPE_FLOAT, TM.TYPE_FLOAT)]
    def execute(self, dataframe):
        return dataframe[dataframe[self.column_name] > self.param_values["threshold"]]

class LessFilterOp(FilterOpBase):
    PARAMS = [{"threshold": TM.TYPE_INTEGER}]
    IOTYPES = [(TM.TYPE_BOOL, TM.TYPE_BOOL), (TM.TYPE_INTEGER, TM.TYPE_INTEGER), (TM.TYPE_FLOAT, TM.TYPE_FLOAT)]
    def execute(self, dataframe):
        return dataframe[dataframe[self.column_name] < self.param_values["threshold"]]
