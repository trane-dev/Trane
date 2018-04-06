from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase
import numpy
import logging

TRANSFORMATION_OPS = ["IdentityTransformationOp",
                      "DiffTransformationOp", "ObjectFrequencyTransformationOp"]
__all__ = ["TransformationOpBase", "TRANSFORMATION_OPS"] + TRANSFORMATION_OPS


class TransformationOpBase(OpBase):
    """super class for all Transformation Operations. (deprecated)"""
    pass


class IdentityTransformationOp(TransformationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [(TM.TYPE_CATEGORY, TM.TYPE_CATEGORY), (TM.TYPE_BOOL, TM.TYPE_BOOL),
               (TM.TYPE_ORDERED, TM.TYPE_ORDERED), (TM.TYPE_TEXT, TM.TYPE_TEXT),
               (TM.TYPE_INTEGER, TM.TYPE_INTEGER), (TM.TYPE_FLOAT, TM.TYPE_FLOAT),
               (TM.TYPE_TIME, TM.TYPE_TIME), (TM.TYPE_IDENTIFIER, TM.TYPE_IDENTIFIER)]

    def execute(self, dataframe):
        return dataframe


class DiffTransformationOp(TransformationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_FLOAT),
               (TM.TYPE_INTEGER, TM.TYPE_INTEGER)]

    def execute(self, dataframe):
        dataframe = dataframe.copy()

        #HACKY FIX
        # dataframe[self.column_name] = dataframe[self.column_name].astype(numpy.float32)

        index = dataframe.index
        for i in range(len(index) - 1, 0, -1):
            dataframe.at[index[i], self.column_name] -= float(dataframe.at[
                index[i - 1], self.column_name].astype(numpy.float32))
        dataframe.at[index[0], self.column_name] = 0
        return dataframe


class ObjectFrequencyTransformationOp(TransformationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [(TM.TYPE_CATEGORY, TM.TYPE_INTEGER), (TM.TYPE_BOOL, TM.TYPE_INTEGER),
               (TM.TYPE_ORDERED, TM.TYPE_INTEGER), (TM.TYPE_TEXT, TM.TYPE_INTEGER),
               (TM.TYPE_INTEGER, TM.TYPE_INTEGER), (TM.TYPE_FLOAT, TM.TYPE_INTEGER),
               (TM.TYPE_TIME, TM.TYPE_INTEGER), (TM.TYPE_IDENTIFIER, TM.TYPE_INTEGER)]

    def execute(self, dataframe):
        dataframe = dataframe.copy()
        objects = dataframe.copy()[self.column_name].unique()
        objects_to_frequency = {}
        for obj in objects:
          objects_to_frequency[obj] = 0

        for val in dataframe[self.column_name]:
            objects_to_frequency[val] = objects_to_frequency[val] + 1

        for idx, obj in enumerate(objects):
            dataframe.at[idx, self.column_name] = objects_to_frequency[obj]

        dataframe[self.column_name] = dataframe[self.column_name].astype(int)

        num_rows_to_drop = len(dataframe) - len(objects_to_frequency)
        assert(num_rows_to_drop >= 0)
        if num_rows_to_drop == 0:
            return dataframe

        return dataframe[:-num_rows_to_drop]
