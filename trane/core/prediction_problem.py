import logging

import composeml as cp
import numpy as np

from trane.core.utils import _check_operations_valid, _parse_table_meta
from trane.ops.filter_ops import (
    AllFilterOp,
    EqFilterOp,
    FilterOpBase,
    GreaterFilterOp,
    LessFilterOp,
    NeqFilterOp,
)


class PredictionProblem:

    """
    Prediction Problem is made up of a series of Operations. It also contains
    information about the types expected as the input and output of
    each operation.
    """

    def __init__(
        self,
        operations,
        entity_col: str,
        time_col: str,
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

        if cutoff_strategy:
            window_size = cutoff_strategy.window_size
        else:
            window_size = None

        self._label_maker = cp.LabelMaker(
            target_dataframe_index=entity_col,
            time_index=time_col,
            labeling_function=self._execute_operations_on_df,
            window_size=window_size,
        )

    def __lt__(self, other):
        return self.__str__() < (other.__str__())

    def __le__(self, other):
        return self.__str__() <= (other.__str__())

    def __gt__(self, other):
        return self.__str__() > (other.__str__())

    def __ge__(self, other):
        return self.__str__() >= (other.__str__())

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

        return lt

    def _execute_operations_on_df(self, df):
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
        if self.entity_col != "__fake_root_entity__":
            description = "For each <" + self.entity_col + "> predict"
        else:
            description = "Predict"
        # cycle through each operation to create dataops
        # dataops are a series of operations containing one and only one
        # aggregation op at its end

        description += self._describe_aggop(self.operations[-1])

        # cycle through ops, pick out filters and describe them
        filterop_desc_arr = []
        for op in self.operations:
            if issubclass(type(op), FilterOpBase):
                filterop_desc_arr.append(self._describe_filter(op))
        # join filter ops with ands and append to the description
        if len(filterop_desc_arr) > 0:
            description += " and ".join(filterop_desc_arr)

        # add the cutoff strategy description if it exists
        if self.cutoff_strategy:
            description += " " + self.cutoff_strategy.description

        return description

    def _describe_aggop(self, op):
        return op.generate_description()

    def _describe_filter(self, op):
        filter_op_str_dict = {
            GreaterFilterOp: "greater than",
            EqFilterOp: "equal to",
            NeqFilterOp: "not equal to",
            LessFilterOp: "less than",
        }

        filter_ops = [x for x in self.operations if issubclass(type(x), FilterOpBase)]

        # remove AllFilterOp
        filter_ops = [x for x in filter_ops if not isinstance(x, AllFilterOp)]
        if len(filter_ops) == 0:
            return ""

        desc = " with "
        last_op_idx = len(filter_ops) - 1
        for idx, op in enumerate(filter_ops):
            op_desc = "<{col}> {op} {threshold}".format(
                col=op.column_name,
                op=filter_op_str_dict[type(op)],
                threshold=op.__dict__.get("threshold", "__"),
            )
            desc += op_desc

            if idx != last_op_idx:
                desc += " and "

        return desc

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

    def _check_type(self, expected_type, actual_data):
        """
        Asserts that the expected type matches the actual data's type.
        Parameters
        ----------
        expected_type: the expected type of the data in TableMeta format
        actual_data: a piece of the actual data
        Returns
        ----------
        None
        """
        logging.debug(
            "Beginning check type. Expected type is: {},             Actual data is:"
            " {}, Actual type is: {}".format(
                expected_type,
                actual_data,
                type(actual_data),
            ),
        )

        allowed_types_bool = [bool, np.bool_]
        allowed_types_text = [str]
        allowed_types_int = [int, np.int64]
        allowed_types_float = [float, np.float64, np.float32]
        (
            allowed_types_bool
            + allowed_types_int
            + allowed_types_text
            + allowed_types_float
        )
        (
            allowed_types_bool
            + allowed_types_int
            + allowed_types_text
            + allowed_types_float
        )
        allowed_types_int + allowed_types_text + allowed_types_float

        # if expected_type == TableMeta.TYPE_CATEGORY:
        #     assert type(actual_data) in allowed_types_category

        # elif expected_type == TableMeta.TYPE_BOOL:
        #     assert type(actual_data) in allowed_types_bool

        # elif expected_type == TableMeta.TYPE_ORDERED:
        #     assert type(actual_data) in allowed_types_ordered

        # elif expected_type == TableMeta.TYPE_TEXT:
        #     assert type(actual_data) in allowed_types_text

        # elif expected_type == TableMeta.TYPE_INTEGER:
        #     assert type(actual_data) in allowed_types_int

        # elif expected_type == TableMeta.TYPE_FLOAT:
        #     assert type(actual_data) in allowed_types_float

        # elif expected_type == TableMeta.TYPE_TIME:
        #     assert type(actual_data) in allowed_types_time

        # elif expected_type == TableMeta.TYPE_IDENTIFIER:
        #     assert type(actual_data) in allowed_types_id

        # else:
        #     logging.critical("check_type function received an unexpected type.")
