from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase

FILTER_OPS = ["AllFilterOp", "EqFilterOp", "NeqFilterOp", 
    "GreaterFilterOp", "LessFilterOp"]
__all__ = ["FilterOpBase", "FILTER_OPS"] + FILTER_OPS

class FilterOpBase(OpBase):
    pass

class AllFilterOp(FilterOpBase):
    PARAMS = [{}, {}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE), (TM.TYPE_BOOL, TM.TYPE_BOOL)]
    def execute(self, data_frame):
        pass
    def generate_nl_description(self):
        return ""

class EqFilterOp(FilterOpBase):
    PARAMS = [{}, {}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE), (TM.TYPE_BOOL, TM.TYPE_BOOL)]    
    def execute(self, data_frame):
        pass
    def generate_nl_description(self):
        return " among all the records with %s equals ____," % self.column_name

class NeqFilterOp(FilterOpBase):
    PARAMS = [{}, {}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE), (TM.TYPE_BOOL, TM.TYPE_BOOL)]
    def execute(self, data_frame):
        pass
    def generate_nl_description(self):
        return "  among all the records with %s not equals ____," % self.column_name

class GreaterFilterOp(FilterOpBase):
    PARAMS = [{}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE)]    
    def execute(self, data_frame):
        pass
    def generate_nl_description(self):
        return " among all the records with %s greater than ____," % self.column_name

class LessFilterOp(FilterOpBase):
    PARAMS = [{}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE)]
    def execute(self, data_frame):
        pass
    def generate_nl_description(self):
        return " among all the records with %s less than ____," % self.column_name
