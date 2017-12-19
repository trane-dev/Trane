from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase

TRANSFORMATION_OPS = ["IdentityTransformationOp", "DiffTransformationOp"]

class TransformationOpBase(OpBase):
    pass

class IdentityTransformationOp(TransformationOpBase):
    PARAMS = [{}, {}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE), (TM.TYPE_BOOL, TM.TYPE_BOOL)]
    def execute(self, data_frame):
        pass
    def generate_nl_description(self):
        return ""

class DiffTransformationOp(TransformationOpBase):
    PARAMS = [{}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE)]
    def execute(self, data_frame):
        pass
    def generate_nl_description(self):
        return " the fluctuation of"
