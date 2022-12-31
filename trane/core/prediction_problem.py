import json
import logging
import os

import composeml as cp
import dill
import numpy as np

from trane.ops.aggregation_ops import (
    AvgAggregationOp,
    CountAggregationOp,
    MajorityAggregationOp,
    MaxAggregationOp,
    MinAggregationOp,
    SumAggregationOp,
)
from trane.ops.filter_ops import (
    AllFilterOp,
    EqFilterOp,
    FilterOpBase,
    GreaterFilterOp,
    LessFilterOp,
    NeqFilterOp,
)
from trane.ops.op_saver import op_from_json, op_to_json
from trane.utils.table_meta import TableMeta

__all__ = ["PredictionProblem"]


class PredictionProblem:

    """
    Prediction Problem is made up of a series of Operations. It also contains
    information about the types expected as the input and output of
    each operation.
    """

    def __init__(
        self,
        operations,
        entity_col,
        time_col,
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
        self.table_meta = table_meta
        self.cutoff_strategy = cutoff_strategy
        self.label_type = None

        if cutoff_strategy:
            window_size = cutoff_strategy.window_size
        else:
            window_size = None

        self._label_maker = cp.LabelMaker(
            target_dataframe_name=entity_col,
            time_index=time_col,
            labeling_function=self._execute_operations_on_df,
            window_size=window_size,
        )

    def is_valid(self, table_meta=None):
        """
        Typechecking for operations. Insures that their input and output types
        match. Allows a user to use the problem's existing table_meta, or pass
        in a new one

        Parameters
        ----------
        table_meta: TableMeta object. Contains meta information about the data

        Returns
        -------
        Bool
        """
        # don't contaminate original table_meta
        if table_meta:
            temp_meta = table_meta.copy()
        else:
            temp_meta = self.table_meta.copy()

        # sort each operation in its respective bucket
        for op in self.operations:
            # op.type_check returns a modified temp_meta,
            # which accounts for the operation having taken place
            temp_meta = op.op_type_check(temp_meta)
            if temp_meta is None:
                return False

        if temp_meta in TableMeta.TYPES:
            self.label_type = temp_meta
            return True
        else:
            return False

    def execute(
        self,
        df,
        num_examples_per_instance,
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
                "Your Problem's specified operations do not match with the "
                "problem's table meta. Therefore, the problem is not "
                "valid.",
            )

        default_kwarg = (
            self.cutoff_strategy.kwarg_dict() if self.cutoff_strategy else {}
        )
        search_kwargs = {
            "minimum_data": minimum_data or default_kwarg.get("minimum_data"),
            "maximum_data": maximum_data or default_kwarg.get("maximum_data"),
            "gap": gap or default_kwarg.get("gap"),
        }
        lt = self._label_maker.search(
            df=df,
            num_examples_per_instance=num_examples_per_instance,
            minimum_data=search_kwargs["minimum_data"],
            maximum_data=search_kwargs["maximum_data"],
            gap=search_kwargs["gap"],
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
            df = operation.execute(df)
        return df

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
        agg_op_str_dict = {
            SumAggregationOp: " the total <{}> in all related records",
            AvgAggregationOp: " the average <{}> in all related records",
            MaxAggregationOp: " the maximum <{}> in all related records",
            MinAggregationOp: " the minimum <{}> in all related records",
            MajorityAggregationOp: " the majority <{}> in all related records",
        }

        if isinstance(op, CountAggregationOp):
            return " the number of records"
        if type(op) in agg_op_str_dict:
            return agg_op_str_dict[type(op)].format(op.column_name)

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
                threshold=op.hyper_parameter_settings.get("threshold", "__"),
            )
            desc += op_desc

            if idx != last_op_idx:
                desc += " and "

        return desc

    def save(self, path, problem_name):
        """
        Saves the pediction problem in two files.

        One file is a dill of the cutoff strategy.
        The other file is the jsonified operations and the relative path to
        that cutoff strategy.

        Parameters
        ----------
        path: str - the directory in which save the problem
        problem_name: str - the filename to assign the problem

        Returns
        -------
        dict
        {'saved_correctly': bool,
         'directory_created': bool,
         'problem_name': str}
        The new problem_name may have changed due to a filename collision

        """
        json_saved = False
        dill_saved = False
        created_directory = False

        # create directory if it doesn't exist
        if not os.path.isdir(path):
            os.makedirs(path)
            created_directory = True

        # rename the problem_name if already exists
        json_file_exists = os.path.exists(os.path.join(path, problem_name + ".json"))
        dill_file_exists = os.path.exists(os.path.join(path, problem_name + ".dill"))

        i = 1
        while json_file_exists or dill_file_exists:
            problem_name += str(i)

            i += 1
            json_file_exists = os.path.exists(
                os.path.join(path, problem_name + ".json"),
            )
            dill_file_exists = os.path.exists(
                os.path.join(path, problem_name + ".dill"),
            )

        # get the cutoff_strategy bytes
        cutoff_dill_bytes = self._dill_cutoff_strategy()

        # add a key to the problem json
        json_dict = json.loads(self.to_json())
        json_dict["cutoff_dill"] = problem_name + ".dill"

        # write the files
        with open(os.path.join(path, problem_name + ".json"), "w") as f:
            json.dump(obj=json_dict, fp=f, indent=4, sort_keys=True)
            json_saved = True

        with open(os.path.join(path, problem_name + ".dill"), "wb") as f:
            f.write(cutoff_dill_bytes)
            dill_saved = True

        return {
            "saved_correctly": json_saved & dill_saved,
            "created_directory": created_directory,
            "problem_name": problem_name,
        }

    @classmethod
    def load(cls, json_file_path):
        """
        Load a prediction problem from json file.
        If the file links to a dill (binary) cutoff_srategy, also load that
        and assign it to the prediction problem.

        Parameters
        ----------
        json_file_path: str, path and filename for the json file

        Returns
        -------
        PredictionProblem

        """
        with open(json_file_path, "r") as f:
            problem_dict = json.load(f)
            problem = cls.from_json(problem_dict)

        cutoff_strategy_file_name = problem_dict.get("cutoff_dill", None)

        if cutoff_strategy_file_name:
            # reconstruct cutoff strategy filename
            pickle_path = os.path.join(
                os.path.dirname(json_file_path),
                cutoff_strategy_file_name,
            )

            # load cutoff strategy from file
            with open(pickle_path, "rb") as f:
                cutoff_strategy = dill.load(f)

            # assign cutoff strategy to problem
            problem.cutoff_strategy = cutoff_strategy

        return problem

    def to_json(self):
        """
        This function converts Prediction Problems to JSON. It captures the
        table_meta, but not the cutoff_strategy

        Parameters
        ----------
        None

        Returns
        -------
        json: JSON representation of the Prediction Problem.

        """
        table_meta_json = None
        if self.table_meta:
            table_meta_json = self.table_meta.to_json()

        return json.dumps(
            {
                "operations": [json.loads(op_to_json(op)) for op in self.operations],
                "entity_col": self.entity_col,
                "time_col": self.time_col,
                "table_meta": table_meta_json,
            },
        )

    @classmethod
    def from_json(cls, json_data):
        """
        This function converts a JSON snippet
        to a prediction problem

        Parameters
        ----------
        json_data: JSON code or dict containing the prediction problem.

        Returns
        -------
        problem: Prediction Problem
        """

        # only tries json.loads if json_data is not a dict
        if not isinstance(json_data, dict):
            json_data = json.loads(json_data)

        operations = [
            op_from_json(json.dumps(item)) for item in json_data["operations"]
        ]
        entity_col = json_data["entity_col"]
        time_col = json_data["time_col"]
        table_meta = TableMeta.from_json(json_data.get("table_meta"))

        problem = PredictionProblem(
            operations=operations,
            entity_col=entity_col,
            time_col=time_col,
            table_meta=table_meta,
            cutoff_strategy=None,
        )
        return problem

    def _dill_cutoff_strategy(self):
        """
        Function creates a dill for the problem's associated cutoff strategy

        This function requires cutoff time to be assigned.

        Parameters
        ----------

        Returns
        -------
        a dill of the cutoff strategy
        """
        cutoff_dill = dill.dumps(self.cutoff_strategy)
        return cutoff_dill

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
            "Beginning check type. Expected type is: {}, \
            Actual data is: {}, Actual type is: {}".format(
                expected_type,
                actual_data,
                type(actual_data),
            ),
        )

        allowed_types_category = [bool, int, str, float]
        allowed_types_bool = [bool, np.bool_]
        allowed_types_text = [str]
        allowed_types_int = [int, np.int64]
        allowed_types_float = [float, np.float64, np.float32]
        allowed_types_time = (
            allowed_types_bool
            + allowed_types_int
            + allowed_types_text
            + allowed_types_float
        )
        allowed_types_ordered = (
            allowed_types_bool
            + allowed_types_int
            + allowed_types_text
            + allowed_types_float
        )
        allowed_types_id = allowed_types_int + allowed_types_text + allowed_types_float

        if expected_type == TableMeta.TYPE_CATEGORY:
            assert type(actual_data) in allowed_types_category

        elif expected_type == TableMeta.TYPE_BOOL:
            assert type(actual_data) in allowed_types_bool

        elif expected_type == TableMeta.TYPE_ORDERED:
            assert type(actual_data) in allowed_types_ordered

        elif expected_type == TableMeta.TYPE_TEXT:
            assert type(actual_data) in allowed_types_text

        elif expected_type == TableMeta.TYPE_INTEGER:
            assert type(actual_data) in allowed_types_int

        elif expected_type == TableMeta.TYPE_FLOAT:
            assert type(actual_data) in allowed_types_float

        elif expected_type == TableMeta.TYPE_TIME:
            assert type(actual_data) in allowed_types_time

        elif expected_type == TableMeta.TYPE_IDENTIFIER:
            assert type(actual_data) in allowed_types_id

        else:
            logging.critical("check_type function received an unexpected type.")

    def set_parameters(self, **parameters):
        for operation in self.operations:
            settings = operation.hyper_parameter_settings
            for parameter in operation.REQUIRED_PARAMETERS:
                for key in parameter:
                    settings[key] = parameters.get(key, 0)
