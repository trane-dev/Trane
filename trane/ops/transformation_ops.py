from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase

TRANSFORMATION_OPS = ["IdentityTransformationOp", "DiffTransformationOp"]
__all__ = ["TransformationOpBase", "TRANSFORMATION_OPS"] + TRANSFORMATION_OPS

class TransformationOpBase(OpBase):
    pass

class IdentityTransformationOp(TransformationOpBase):
    PARAMS = [{}, {}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE), (TM.TYPE_BOOL, TM.TYPE_BOOL)]
    def execute(self, dataframe):
        return dataframe
    def generate_nl_description(self):
        return ""

class DiffTransformationOp(TransformationOpBase):
    PARAMS = [{}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE)]
    def execute(self, dataframe):
        index = dataframe.index
        dataframe.at[index[0], self.column_name] = 0
        for i in range(1, len(index)):
            dataframe.at[index[i], self.column_name] -= dataframe.at[index[i-1], self.column_name]
        return dataframe
    def generate_nl_description(self):
        return " the fluctuation of"
