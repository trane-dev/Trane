from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase

FILTER_OPS = ["AllFilterOp", "GreaterFilterOp",
              "EqFilterOp", "NeqFilterOp", "LessFilterOp"]
__all__ = ["FilterOpBase", "FILTER_OPS"] + FILTER_OPS


class FilterOpBase(OpBase):
    """
    Super class for all Filter Operations. The class is empty and is currently a
    placeholder for any FilterOpBase level methods we want to make.

    Filter operations represent the 1st operation
    in a prediction problem. They filter out rows
    based on values in the filter_column. Filter operations are defined as classes
    that inherit the FilterOpBase class and instantiate the execute method.

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


class AllFilterOp(FilterOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [(TM.TYPE_CATEGORY, TM.TYPE_CATEGORY), (TM.TYPE_BOOL, TM.TYPE_BOOL),
               (TM.TYPE_ORDERED, TM.TYPE_ORDERED), (TM.TYPE_TEXT, TM.TYPE_TEXT),
               (TM.TYPE_INTEGER, TM.TYPE_INTEGER), (TM.TYPE_FLOAT, TM.TYPE_FLOAT),
               (TM.TYPE_TIME, TM.TYPE_TIME), (TM.TYPE_IDENTIFIER, TM.TYPE_IDENTIFIER)]

    def execute(self, dataframe):
        return dataframe


class EqFilterOp(FilterOpBase):
    REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_INTEGER}]
    IOTYPES = [(
        TM.TYPE_BOOL, TM.TYPE_BOOL), (TM.TYPE_INTEGER,
                                      TM.TYPE_INTEGER), (
                                          TM.TYPE_FLOAT, TM.TYPE_FLOAT)]

    def execute(self, dataframe):
        dataframe = dataframe.copy()
        return dataframe[dataframe[self.column_name] ==
                         self.hyper_parameter_settings["threshold"]]


class NeqFilterOp(FilterOpBase):
    REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_INTEGER}]
    IOTYPES = [(
        TM.TYPE_BOOL, TM.TYPE_BOOL), (TM.TYPE_INTEGER,
                                      TM.TYPE_INTEGER), (
                                          TM.TYPE_FLOAT, TM.TYPE_FLOAT)]

    def execute(self, dataframe):
        dataframe = dataframe.copy()
        return dataframe[dataframe[self.column_name] !=
                         self.hyper_parameter_settings["threshold"]]


class GreaterFilterOp(FilterOpBase):
    REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_INTEGER}]
    IOTYPES = [(
        TM.TYPE_BOOL, TM.TYPE_BOOL), (TM.TYPE_INTEGER,
                                      TM.TYPE_INTEGER), (
                                          TM.TYPE_FLOAT, TM.TYPE_FLOAT)]

    def execute(self, dataframe):
        dataframe = dataframe.copy()
        return dataframe[dataframe[self.column_name] >
                         self.hyper_parameter_settings["threshold"]]


class LessFilterOp(FilterOpBase):
    REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_INTEGER}]
    IOTYPES = [(
        TM.TYPE_BOOL, TM.TYPE_BOOL), (TM.TYPE_INTEGER,
                                      TM.TYPE_INTEGER), (
                                          TM.TYPE_FLOAT, TM.TYPE_FLOAT)]

    def execute(self, dataframe):
        dataframe = dataframe.copy()
        return dataframe[dataframe[self.column_name] <
                         self.hyper_parameter_settings["threshold"]]
