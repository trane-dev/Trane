from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase
import numpy as np

ROW_OPS = ["IdentityRowOp", "GreaterRowOp", "EqRowOp", "NeqRowOp", "LessRowOp", "ExpRowOp"]
__all__ = ["RowOpBase", "ROW_OPS"] + ROW_OPS

class RowOpBase(OpBase):
    """
    super class for all Row Operations. (deprecated)
    """
    pass

class IdentityRowOp(RowOpBase):
    PARAMS = [{}]
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_FLOAT)]
    def execute(self, dataframe):
        return dataframe

class EqRowOp(RowOpBase):
    PARAMS = [{"threshold": TM.TYPE_FLOAT}]
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_BOOL)]
    def execute(self, dataframe):
        dataframe[self.column_name] = dataframe[self.column_name].apply(lambda x: x == self.param_values["threshold"])
        return dataframe

class NeqRowOp(RowOpBase):
    PARAMS = [{"threshold": TM.TYPE_FLOAT}]
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_BOOL)]
    def execute(self, dataframe):
        dataframe[self.column_name] = dataframe[self.column_name].apply(lambda x: x != self.param_values["threshold"])
        return dataframe

class GreaterRowOp(RowOpBase):
    PARAMS = [{"threshold": TM.TYPE_FLOAT}]
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_BOOL)]
    def execute(self, dataframe):
        dataframe[self.column_name] = dataframe[self.column_name].apply(lambda x: x > self.param_values["threshold"])
        return dataframe

class LessRowOp(RowOpBase):
    PARAMS = [{"threshold": TM.TYPE_FLOAT}]
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_BOOL)]
    def execute(self, dataframe):
        dataframe[self.column_name] = dataframe[self.column_name].apply(lambda x: x < self.param_values["threshold"])
        return dataframe

class ExpRowOp(RowOpBase):
    PARAMS = [{"threshold": TM.TYPE_FLOAT}]
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_FLOAT)]
    def execute(self, dataframe):
        dataframe[self.column_name] = dataframe[self.column_name].apply(lambda x: np.exp(x))
        return dataframe
