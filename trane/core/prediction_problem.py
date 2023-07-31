import composeml as cp
import pandas as pd

from trane.core.utils import _check_operations_valid, _parse_table_meta


class PredictionProblem:

    """
    Prediction Problem is made up of a series of Operations. It also contains
    information about the types expected as the input and output of
    each operation.
    """

    def __init__(
        self,
        operations,
        time_col: str,
        entity_col: str = None,
        table_meta=None,
        cutoff_strategy=None,
    ):
        """
        Parameters
        ----------
        operations: list of Operations of type op_base
        cutoff_strategy: a CutoffStrategy object

        Returns
        -------
        None
        """
        self.operations = operations
        self.entity_col = entity_col
        self.time_col = time_col
        self.table_meta = _parse_table_meta(table_meta)
        self.cutoff_strategy = cutoff_strategy
        self.label_type = None

        self.window_size = None
        if cutoff_strategy:
            self.window_size = cutoff_strategy.window_size

    def __lt__(self, other):
        return self.__str__() < (other.__str__())

    def __le__(self, other):
        return self.__str__() <= (other.__str__())

    def __gt__(self, other):
        return self.__str__() > (other.__str__())

    def __ge__(self, other):
        return self.__str__() >= (other.__str__())

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(self, other.__class__):
            if (
                self.operations == other.operations
                and self.entity_col == other.entity_col
                and self.time_col == other.time_col
                and self.table_meta == other.table_meta
                and self.cutoff_strategy == other.cutoff_strategy
            ):
                return True
            return False
        return False

    def __hash__(self) -> int:
        # TODO: why is the opbase hash function not working
        attributes = ()
        for op in self.operations:
            attributes += (type(op), op.column_name, op.threshold)
        return hash(attributes)

    def is_valid(self, table_meta=None):
        """
        Typechecking for operations. Insures that their input and output types
        match. Allows a user to use the problem's existing table_meta, or pass
        in a new one

        Parameters
        ----------
        table_meta: TableMeta object. Contains meta information about the data
            example:
            {
                'id': ColumnSchema(logical_type=Categorial, semantic_tags={'index'}),
                'time': ColumnSchema(logical_type=Datetime, semantic_tags={'time_index'}),
                'price': ColumnSchema(logical_type=Double, semantic_tags={'numeric'}),
                'product': ColumnSchema(logical_type=Categorial, semantic_tags={'category'}),
            }

        Returns
        -------
        Bool
        """
        if table_meta:
            temp_meta = table_meta.copy()
        else:
            temp_meta = self.table_meta.copy()

        result, _ = _check_operations_valid(self.operations, temp_meta)
        if result:
            return True
        return False

    def execute(
        self,
        df,
        num_examples_per_instance=-1,
        minimum_data=None,
        maximum_data=None,
        gap=None,
        drop_empty=True,
        verbose=True,
        *args,
        **kwargs,
    ):
        """
        Executes the problem's operations on a dataframe. Generates the training examples (lable_times).
        The label_times contains the
        """

        assert df.isnull().sum().sum() == 0

        if not self.is_valid(self.table_meta):
            raise ValueError(
                (
                    "Your Problem's specified operations do not match with the "
                    "problem's table meta. Therefore, the problem is not "
                    "valid."
                ),
            )
        target_dataframe_index = self.entity_col
        if self.entity_col is None:
            # create a fake index with all rows to generate predictions problems "Predict X"
            df["__identity__"] = 0
            target_dataframe_index = "__identity__"

        self._label_maker = cp.LabelMaker(
            target_dataframe_index=target_dataframe_index,
            time_index=self.time_col,
            labeling_function=self._execute_operations_on_df,
            window_size=self.window_size,
        )
        minimum_data = minimum_data or self.cutoff_strategy.minimum_data
        maximum_data = maximum_data or self.cutoff_strategy.maximum_data
        lt = self._label_maker.search(
            df=df,
            num_examples_per_instance=num_examples_per_instance,
            minimum_data=minimum_data,
            maximum_data=maximum_data,
            gap=gap,
            drop_empty=drop_empty,
            verbose=verbose,
            *args,
            **kwargs,
        )
        if "__identity__" in df.columns:
            df.drop(columns=["__identity__"], inplace=True)
        lt = lt.rename(columns={"_execute_operations_on_df": "target"})
        return lt

    def _execute_operations_on_df(self, df: pd.DataFrame):
        """
        Execute operations on df. This method assumes that data leakage/cutoff
            times have already been taken into account, and just blindly
            executes the operations.
        Parameters
        ----------
        df: dataframe to be operated on

        Returns
        -------
        df: dataframe after operations
        """
        df = df.copy()
        for operation in self.operations:
            df = operation.label_function(df)
        return df

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self):
        """
        This function converts Prediction Problems to English.

        Parameters
        ----------
        None

        Returns
        -------
        description: str natural language description of the problem

        """
        description = "Predict"
        if self.entity_col:
            description = "For each <" + self.entity_col + "> predict"

        agg_op = self.operations[1]
        description += agg_op.generate_description()

        filter_op = self.operations[0]
        description += filter_op.generate_description()

        if self.cutoff_strategy and self.cutoff_strategy.window_size:
            description += " " + "in next {} days".format(
                self.cutoff_strategy.window_size,
            )
        return description
