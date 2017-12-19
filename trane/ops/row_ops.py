from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase

ROW_OPS = ["IdentityRowOp", "EqRowOp", "NeqRowOp", "GreaterRowOp",
    "LessRowOp", "ExpRowOp"]
__all__ = ["RowOpBase", "ROW_OPS"] + ROW_OPS

class RowOpBase(OpBase):
    pass

class IdentityRowOp(RowOpBase):
    PARAMS = [{}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE)]
    def execute(self, data_frame):
        pass
    def generate_nl_description(self):
        if self.itype == TM.TYPE_BOOL:
            return ""
        return " %s" % self.column_name

class EqRowOp(RowOpBase):
    PARAMS = [{}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_BOOL)]
    def execute(self, data_frame):
        pass
    def generate_nl_description(self):
        return " %s equals to ____" % self.column_name

class NeqRowOp(RowOpBase):
    PARAMS = [{}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_BOOL)]
    def execute(self, data_frame):
        pass
    def generate_nl_description(self):
        return " %s does not equal to ____" % self.column_name

class GreaterRowOp(RowOpBase):
    PARAMS = [{}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_BOOL)]
    def execute(self, data_frame):
        pass
    def generate_nl_description(self):
        return " %s is greater than ____" % self.column_name

class LessRowOp(RowOpBase):
    PARAMS = [{}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_BOOL)]
    def execute(self, data_frame):
        pass
    def generate_nl_description(self):
        return " %s is less than ____" % self.column_name

class ExpRowOp(RowOpBase):
    PARAMS = [{}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE)]
    def execute(self, data_frame):
        pass
    def generate_nl_description(self):
        return " the exponentiate of %s" % self.column_name
