from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase

FILTER_OPS = ["AllFilterOp", "GreaterFilterOp", "EqFilterOp", "NeqFilterOp", "LessFilterOp"]
__all__ = ["FilterOpBase", "FILTER_OPS"] + FILTER_OPS

class FilterOpBase(OpBase):
    """super class for all Filter Operations. (deprecated)"""
    pass

class AllFilterOp(FilterOpBase):
    PARAMS = [{}, {}]
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_FLOAT), (TM.TYPE_BOOL, TM.TYPE_BOOL)]
    def execute(self, dataframe):
        return dataframe

class EqFilterOp(FilterOpBase):
    PARAMS = [{"threshold": TM.TYPE_FLOAT}, {"threshold": TM.TYPE_BOOL}]
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_FLOAT), (TM.TYPE_BOOL, TM.TYPE_BOOL)]    
    def execute(self, dataframe):
        return dataframe[dataframe[self.column_name] == self.param_values["threshold"]]

class NeqFilterOp(FilterOpBase):
    PARAMS = [{"threshold": TM.TYPE_FLOAT}, {"threshold": TM.TYPE_BOOL}]
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_FLOAT), (TM.TYPE_BOOL, TM.TYPE_BOOL)]
    def execute(self, dataframe):
        return dataframe[dataframe[self.column_name] != self.param_values["threshold"]]

class GreaterFilterOp(FilterOpBase):
    PARAMS = [{"threshold": TM.TYPE_FLOAT}]
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_FLOAT)]    
    def execute(self, dataframe):
        return dataframe[dataframe[self.column_name] > self.param_values["threshold"]]

class LessFilterOp(FilterOpBase):
    PARAMS = [{"threshold": TM.TYPE_FLOAT}]
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_FLOAT)]
    def execute(self, dataframe):
        return dataframe[dataframe[self.column_name] < self.param_values["threshold"]]
