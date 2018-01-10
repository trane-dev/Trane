from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase

FILTER_OPS = ["AllFilterOp", "GreaterFilterOp"]#, "EqFilterOp", "NeqFilterOp", "LessFilterOp"]
__all__ = ["FilterOpBase", "FILTER_OPS"] + FILTER_OPS

class FilterOpBase(OpBase):
    """
    super class for all Filter Operations. (deprecated)
    """
    pass

class AllFilterOp(FilterOpBase):
    PARAMS = [{}, {}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE), (TM.TYPE_BOOL, TM.TYPE_BOOL)]
    def execute(self, dataframe):
        return dataframe
    def generate_nl_description(self):
        return ""

class EqFilterOp(FilterOpBase):
    PARAMS = [{"threshold": TM.TYPE_VALUE}, {"threshold": TM.TYPE_BOOL}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE), (TM.TYPE_BOOL, TM.TYPE_BOOL)]    
    def execute(self, dataframe):
        raise NotImplementedError
    def generate_nl_description(self):
        return " among all the records with %s equals ____," % self.column_name

class NeqFilterOp(FilterOpBase):
    PARAMS = [{"threshold": TM.TYPE_VALUE}, {"threshold": TM.TYPE_BOOL}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE), (TM.TYPE_BOOL, TM.TYPE_BOOL)]
    def execute(self, dataframe):
        raise NotImplementedError
    def generate_nl_description(self):
        return "  among all the records with %s not equals ____," % self.column_name

class GreaterFilterOp(FilterOpBase):
    PARAMS = [{"threshold": TM.TYPE_VALUE}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE)]    
    def execute(self, dataframe):
        return dataframe[dataframe[self.column_name] > self.param_values["threshold"]]
    def generate_nl_description(self):
        return " among all the records with %s greater than ____," % self.column_name

class LessFilterOp(FilterOpBase):
    PARAMS = [{"threshold": TM.TYPE_VALUE}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE)]
    def execute(self, dataframe):
        raise NotImplementedError
    def generate_nl_description(self):
        return " among all the records with %s less than ____," % self.column_name
