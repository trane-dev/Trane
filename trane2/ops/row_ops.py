import numpy as np

from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase

# ROW_OPS = ["IdentityRowOp", "GreaterRowOp",
#            "EqRowOp", "NeqRowOp", "LessRowOp", "ExpRowOp"]
# __all__ = ["RowOpBase", "ROW_OPS"] + ROW_OPS


class RowOpBase(OpBase):
    """
    Super class for all Row Operations. The class is empty and is currently a
    placeholder for any RowOpBase level methods we want to make.

    Row operations represent the 2nd operation
    in a prediction problem. They apply functions to rows.
    Row operations are defined as classes
    that inherit the RowOpBase class and instantiate the execute method.

    Make Your Own
    -------------
    Simply make a new class that follows the requirements below and issue a
    pull request.

    Requirements
    ------------
    REQUIRED_PARAMETERS: the hyper parameters needed for the operation
    IOTYPES: the input and output types of the operation using TableMeta types
    execute method: transform dataframe according to the operation and return
      the new dataframe

    """


class IdentityRowOp(RowOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [(TM.TYPE_CATEGORY, TM.TYPE_CATEGORY), (TM.TYPE_BOOL, TM.TYPE_BOOL), # noqa
               (TM.TYPE_ORDERED, TM.TYPE_ORDERED), (TM.TYPE_TEXT, TM.TYPE_TEXT),  # noqa
               (TM.TYPE_INTEGER, TM.TYPE_INTEGER), (TM.TYPE_FLOAT, TM.TYPE_FLOAT), # noqa
               (TM.TYPE_TIME, TM.TYPE_TIME), (TM.TYPE_IDENTIFIER, TM.TYPE_IDENTIFIER)] # noqa

    def execute(self, dataframe):
        return dataframe


class EqRowOp(RowOpBase):
    REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_INTEGER}]
    IOTYPES = [(TM.TYPE_BOOL, TM.TYPE_BOOL),
               (TM.TYPE_INTEGER, TM.TYPE_BOOL),
               (TM.TYPE_FLOAT, TM.TYPE_BOOL)]

    def execute(self, dataframe):
        dataframe = dataframe.copy()
        dataframe[self.column_name] = dataframe[self.column_name].apply(
            lambda x: x == self.hyper_parameter_settings["threshold"])
        return dataframe


class NeqRowOp(RowOpBase):
    REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_INTEGER}]
    IOTYPES = [(TM.TYPE_BOOL, TM.TYPE_BOOL),
               (TM.TYPE_INTEGER, TM.TYPE_BOOL),
               (TM.TYPE_FLOAT, TM.TYPE_BOOL)]

    def execute(self, dataframe):
        dataframe = dataframe.copy()
        dataframe[self.column_name] = dataframe[self.column_name].apply(
            lambda x: x != self.hyper_parameter_settings["threshold"])
        return dataframe


class GreaterRowOp(RowOpBase):
    REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_INTEGER}]
    IOTYPES = [(TM.TYPE_BOOL, TM.TYPE_BOOL),
               (TM.TYPE_INTEGER, TM.TYPE_BOOL),
               (TM.TYPE_FLOAT, TM.TYPE_BOOL)]

    def execute(self, dataframe):
        dataframe = dataframe.copy()
        dataframe[self.column_name] = dataframe[self.column_name].apply(
            lambda x: x > self.hyper_parameter_settings["threshold"])
        return dataframe


class LessRowOp(RowOpBase):
    REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_INTEGER}]
    IOTYPES = [(TM.TYPE_BOOL, TM.TYPE_BOOL),
               (TM.TYPE_INTEGER, TM.TYPE_BOOL),
               (TM.TYPE_FLOAT, TM.TYPE_BOOL)]

    def execute(self, dataframe):
        dataframe = dataframe.copy()
        dataframe[self.column_name] = dataframe[self.column_name].apply(
            lambda x: x < self.hyper_parameter_settings["threshold"])
        return dataframe


class ExpRowOp(RowOpBase):
    REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_INTEGER}]
    IOTYPES = [(TM.TYPE_INTEGER, TM.TYPE_FLOAT),
               (TM.TYPE_FLOAT, TM.TYPE_FLOAT)]

    def execute(self, dataframe):
        dataframe = dataframe.copy()
        dataframe[self.column_name] = dataframe[
            self.column_name].apply(lambda x: np.exp(x))
        return dataframe
