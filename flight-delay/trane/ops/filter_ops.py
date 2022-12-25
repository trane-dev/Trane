from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase

FILTER_OPS = ["AllFilterOp", "GreaterFilterOp",
              "EqFilterOp", "NeqFilterOp", "LessFilterOp"]
__all__ = ["FilterOpBase", "FILTER_OPS"] + FILTER_OPS


class FilterOpBase(OpBase):
    """
    Super class for all Filter Operations. The class is empty and is currently
    a placeholder for any FilterOpBase level methods we want to make.

    Filter operations represent the 1st operation in a prediction problem.
    They filter out rows based on values in the filter_column. Filter
    operations are defined as classes that inherit the FilterOpBase class and
    instantiate the execute method.

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

    def auto_set_hyperparams(
            self, df, filter_col, label_col=None, entity_col=None,
            num_random_samples=10, num_rows_to_execute_on=2000):
        """
        Overridden method of op_base. Sets operation hyperparams.
        Unnecessary variables are passed so that the method call is uniform
        across all operations

        Parameters
        ----------
        df: Dataframe to be tuned to
        filter_col: The column that will be filtered
        label_col: ignored
        entity_col: ignored
        num_random_samples: if there's more than this many unique values to
            test, randomly sample this many values from the dataset
        num_rows_to_execute_on: if the dataframe contains more than this number
            of rows, randomly select this many rows to use as the dataframe

        Returns
        -------
        hyperparameter: But this has already been set to the operation

        """
        # If the operator has no required parameters, return None
        if len(self.REQUIRED_PARAMETERS) == 0:
            return None

        filter_hyperparam = self.find_threshhold_by_remaining(
            fraction_of_data_target=0.8, df=df, col=filter_col,
            num_random_samples=num_random_samples,
            num_rows_to_execute_on=num_rows_to_execute_on)

        self.set_hyper_parameter(filter_hyperparam)
        return filter_hyperparam


class AllFilterOp(FilterOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = []

    def op_type_check(self, table_meta):
        return table_meta

    def execute(self, dataframe):
        return dataframe


class EqFilterOp(FilterOpBase):
    REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_CATEGORY}]
    IOTYPES = [(TM.TYPE_CATEGORY, TM.TYPE_CATEGORY),
               (TM.TYPE_IDENTIFIER, TM.TYPE_IDENTIFIER)]

    def execute(self, dataframe):
        return dataframe[dataframe[self.column_name] ==
                         self.hyper_parameter_settings["threshold"]]


class NeqFilterOp(FilterOpBase):
    REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_CATEGORY}]
    IOTYPES = [(TM.TYPE_CATEGORY, TM.TYPE_CATEGORY),
               (TM.TYPE_IDENTIFIER, TM.TYPE_IDENTIFIER)]

    def execute(self, dataframe):
        return dataframe[dataframe[self.column_name] !=
                         self.hyper_parameter_settings["threshold"]]


class GreaterFilterOp(FilterOpBase):
    REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_INTEGER}]
    IOTYPES = [(TM.TYPE_INTEGER, TM.TYPE_INTEGER),
               (TM.TYPE_FLOAT, TM.TYPE_FLOAT)]

    def execute(self, dataframe):
        return dataframe[dataframe[self.column_name] >
                         self.hyper_parameter_settings["threshold"]]


class LessFilterOp(FilterOpBase):
    REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_INTEGER}]
    IOTYPES = [(TM.TYPE_INTEGER, TM.TYPE_INTEGER),
               (TM.TYPE_FLOAT, TM.TYPE_FLOAT)]

    def execute(self, dataframe):
        return dataframe[dataframe[self.column_name] <
                         self.hyper_parameter_settings["threshold"]]
