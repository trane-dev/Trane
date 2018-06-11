import numpy

from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase

TRANSFORMATION_OPS = ["IdentityTransformationOp",
                      "DiffTransformationOp", "ObjectFrequencyTransformationOp"]
__all__ = ["TransformationOpBase", "TRANSFORMATION_OPS"] + TRANSFORMATION_OPS


class TransformationOpBase(OpBase):
    """
    Super class for all Transformation Operations. The class is empty and is currently a
    placeholder for any TransformationOpBase level methods we want to make.

    Transformation operations represent the 3rd operation
    in a prediction problem. They apply functions across rows by
    transforming a dataframe with n rows to a dataframe with at most n-1 rows.
    Transformation operations are defined as classes
    that inherit the RowOpBase class and instantiate the execute method.

    Make Your Own
    -------------
    Simply make a new class that follows the requirements below and issue a pull request.

    Requirements
    ------------
    REQUIRED_PARAMETERS: the hyper parameters needed for the operation
    IOTYPES: the input and output types of the operation using TableMeta types
    execute method: transform dataframe according to the operation and return
      the new dataframe

    """


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
        index = dataframe.index
        for i in range(len(index) - 1, 0, -1):
            dataframe.at[index[i], self.column_name] -= float(dataframe.at[
                index[i - 1], self.column_name].astype(numpy.float32))
        #Note: drop first row.
        dataframe = dataframe.iloc[1:]
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
        objects = numpy.sort(objects)
        objects_to_frequency = {}
        for obj in objects:
            objects_to_frequency[obj] = 0

        for val in dataframe[self.column_name]:
            objects_to_frequency[val] = objects_to_frequency[val] + 1

        dataframe[self.column_name] = dataframe[self.column_name].astype(int)
        for idx, obj in enumerate(objects):
            column_idx = dataframe.columns.get_loc(self.column_name)
            dataframe.iat[idx, column_idx] = objects_to_frequency[obj]

        num_rows_to_drop = len(dataframe) - len(objects_to_frequency)
        assert(num_rows_to_drop >= 0)

        if num_rows_to_drop != 0:
            dataframe = dataframe[:-num_rows_to_drop]

        dataframe[self.column_name] = dataframe[self.column_name].astype(int)
        return dataframe
