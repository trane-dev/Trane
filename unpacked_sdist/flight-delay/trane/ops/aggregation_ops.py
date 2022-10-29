from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase

AGGREGATION_OPS = [
    "CountAggregationOp", "SumAggregationOp",
    "AvgAggregationOp",
    "MaxAggregationOp", "MinAggregationOp",
    "MajorityAggregationOp"]
__all__ = ["AggregationOpBase", "AGGREGATION_OPS"] + AGGREGATION_OPS


class AggregationOpBase(OpBase):

    """
    Super class for all Aggregation Operations. The class is empty and is
    currently a placeholder for any AggregationOpBase level methods we want to
    make.

    Aggregation operations represent the 4th and final operation
    in a prediction problem. They aggregate data from many rows into
    a single row. The final output of the problem is the value in that row
    at the label generating column. Aggregation operations are defined as
    classes that inherit the AggregationOpBase class and instantiate the
    execute method.

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

    def op_type_check(self, table_meta):
        """
        Data type check for the operation.
        Operations may change the data type of a column, eg. int -> bool.
        One operation can only be applied on a few data types, eg. `greater`
        can be applied on int but can't be applied on bool.
        This function checks whether the current operation can be applied on
        the data.
        It returns the updated TableMeta for next operation or None if it's not
        valid.

        Parameters
        ----------
        table_meta: table meta before this operation.

        Returns
        -------
        table_meta: table meta after this operation. None if not compatable.

        """
        self.input_type = table_meta.get_type(self.column_name)
        for idx, (input_type, output_type) in enumerate(self.IOTYPES):
            if self.input_type == input_type:
                self.output_type = output_type
                return output_type
        return None


class CountAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = None

    def op_type_check(self, table_meta):
        self.output_type = TM.TYPE_INTEGER
        return TM.TYPE_INTEGER

    def execute(self, dataframe):
        return len(dataframe)


class SumAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_FLOAT),
               (TM.TYPE_INTEGER, TM.TYPE_FLOAT)]

    def execute(self, dataframe):
        if len(dataframe) == 0:
            return None
        return float(dataframe[self.column_name].sum())


class AvgAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_FLOAT),
               (TM.TYPE_INTEGER, TM.TYPE_FLOAT)]

    def execute(self, dataframe):
        if len(dataframe) == 0:
            return None
        return float(dataframe[self.column_name].mean())


class MaxAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_FLOAT),
               (TM.TYPE_INTEGER, TM.TYPE_FLOAT)]

    def execute(self, dataframe):
        if len(dataframe) == 0:
            return None
        return float(dataframe[self.column_name].max())


class MinAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_FLOAT),
               (TM.TYPE_INTEGER, TM.TYPE_FLOAT)]

    def execute(self, dataframe):
        if len(dataframe) == 0:
            return None
        return float(dataframe[self.column_name].min())


class MajorityAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [(TM.TYPE_CATEGORY, TM.TYPE_CATEGORY),
               (TM.TYPE_IDENTIFIER, TM.TYPE_IDENTIFIER)]

    def execute(self, dataframe):
        if len(dataframe) == 0:
            return None

        return str(dataframe[self.column_name].mode()[0])
