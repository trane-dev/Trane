import humanize
import pandas as pd

from trane.core.utils import calculate_target_values
from trane.ops.aggregation_ops import AggregationOpBase, ExistsAggregationOp
from trane.ops.filter_ops import FilterOpBase
from trane.ops.threshold_functions import (
    find_threshold_to_maximize_uncertainty,
    get_k_most_frequent,
)
from trane.ops.transformation_ops import TransformationOpBase
from trane.parsing.denormalize import (
    denormalize,
)
from trane.typing.ml_types import MLType, convert_op_type


class Problem:
    def __init__(
        self,
        metadata,
        operations,
        entity_column=None,
        window_size=None,
        reasoning=None,
    ):
        self.operations = operations
        self.metadata = metadata

        self.entity_column = entity_column
        self.window_size = window_size
        self.reasoning = reasoning

    def __lt__(self, other):
        return self.__str__() < (other.__str__())

    def __le__(self, other):
        return self.__str__() <= (other.__str__())

    def __gt__(self, other):
        return self.__str__() > (other.__str__())

    def __ge__(self, other):
        return self.__str__() >= (other.__str__())

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            if (
                self.operations == other.operations
                and self.metadata == other.metadata
                and self.window_size == other.window_size
            ):
                return True
            return False
        return False

    def has_parameters_set(self):
        return self.operations[0].has_parameters_set()

    def get_required_parameters(self):
        return self.operations[0].required_parameters

    def set_parameters(self, threshold):
        self.operations[0].set_parameters(threshold)

    def set_reasoning(self, reasoning):
        self.reasoning = reasoning

    def get_reasoning(self):
        return self.reasoning

    def reset_reasoning(self):
        self.reasoning = None

    def is_classification(self):
        return isinstance(self.operations[2], ExistsAggregationOp)

    def is_regression(self):
        return not isinstance(self.operations[2], ExistsAggregationOp)

    def get_problem_type(self):
        if isinstance(self.operations[2], ExistsAggregationOp):
            return "classification"
        return "regression"

    def get_normalized_dataframe(self, dataframes):
        normalized_dataframe = None
        if isinstance(dataframes, pd.DataFrame):
            normalized_dataframe = dataframes
        elif len(dataframes) == 1 and self.metadata.get_metadata_type() == "single":
            normalized_dataframe = dataframes[list(dataframes.keys())[0]]
        else:
            multi_metadata = self.metadata.original_multi_table_metadata
            normalized_dataframe, _ = denormalize(
                dataframes=dataframes,
                metadata=multi_metadata,
                target_table=self.target_table,
            )
            normalized_dataframe = normalized_dataframe.sort_values(
                by=[self.metadata.time_index],
            )
        return normalized_dataframe

    def get_recommended_thresholds(self, dataframes, n_quantiles=10):
        # not an ideal threshold function
        # TODO: Add better threshold generation
        normalized_dataframe = self.get_normalized_dataframe(dataframes)
        thresholds = []
        for _, type_ in self.get_required_parameters().items():
            if type_ in [int, float]:
                filter_op = self.operations[0]
                recommended_threshold = find_threshold_to_maximize_uncertainty(
                    df=normalized_dataframe,
                    column_name=filter_op.column_name,
                    problem_type=self.get_problem_type(),
                    filter_op=filter_op,
                    n_quantiles=n_quantiles,
                )
                thresholds.append(recommended_threshold)
            else:
                column_name = self.operations[0].column_name
                thresholds.extend(
                    get_k_most_frequent(
                        normalized_dataframe[column_name],
                        k=3,
                    ),
                )
        return thresholds

    def create_target_values(
        self,
        dataframes,
        verbose=False,
        nrows=None,
        instance_ids=None,
    ):
        # Won't this always be normalized?
        normalized_dataframe = self.get_normalized_dataframe(dataframes)
        if self.has_parameters_set() is False:
            if verbose:
                print("Filter operation's parameters are not set, setting them now")
            thresholds = self.get_recommended_thresholds(dataframes)
            self.set_parameters(thresholds[-1])

        target_dataframe_index = self.entity_column
        if self.entity_column is None:
            # create a fake index with all rows to generate predictions problems "Predict X"
            normalized_dataframe["__identity__"] = 0
            target_dataframe_index = "__identity__"
        if instance_ids and len(instance_ids) > 0:
            if verbose:
                print("Only selecting given instance IDs")
            normalized_dataframe = normalized_dataframe[
                normalized_dataframe[self.entity_column].isin(instance_ids)
            ]

        lt = calculate_target_values(
            df=normalized_dataframe,
            target_dataframe_index=target_dataframe_index,
            labeling_function=self._execute_operations_on_df,
            time_index=self.metadata.time_index,
            window_size=self.window_size,
            verbose=verbose,
            nrows=nrows,
        )
        if "__identity__" in normalized_dataframe.columns:
            normalized_dataframe.drop(columns=["__identity__"], inplace=True)
            lt.drop(columns=["__identity__"], inplace=True)
        lt = lt.rename(columns={"_execute_operations_on_df": "target"})
        return lt

    def _execute_operations_on_df(self, df):
        df = df.copy()
        for operation in self.operations:
            df = operation.label_function(df)
        return df

    def is_valid(self):
        result, _ = _check_operations_valid(
            operations=self.operations,
            metadata=self.metadata,
        )
        if result:
            return True
        return False

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self):
        description = "Predict"
        if self.entity_column:
            description = "For each <" + self.entity_column + "> predict"

        agg_op = self.operations[2]
        description += agg_op.generate_description()

        transform_op = self.operations[1]
        description += transform_op.generate_description()

        filter_op = self.operations[0]
        description += filter_op.generate_description()

        if self.window_size:
            window_size = pd.to_timedelta(self.window_size)
            human_readble = humanize.naturaldelta(window_size)
            description += " " + "in next {}".format(
                human_readble,
            )
        return description


def _check_operations_valid(
    operations,
    metadata,
):
    ml_types = metadata.ml_types
    if not isinstance(operations[0], FilterOpBase):
        raise ValueError
    if not isinstance(operations[1], TransformationOpBase):
        raise ValueError
    if not isinstance(operations[2], AggregationOpBase):
        raise ValueError
    for op in operations:
        input_output_types = op.input_output_types
        for op_input_ml_type, op_output_type in input_output_types:
            op_input_ml_type = convert_op_type(op_input_ml_type)
            op_output_type = convert_op_type(op_output_type)

            # operation applies to all columns
            op_input_has_no_tags = op_input_ml_type.get_tags() == set()
            if isinstance(op_input_ml_type, MLType) and op_input_has_no_tags:
                if isinstance(op_output_type, MLType) and op_input_has_no_tags:
                    # op doesn't modify the column's type
                    pass
                elif op.column_name is None:
                    # TODO: fix CountAggregationOp modifies output type
                    # TODO: Related to problem type generated?
                    pass
                else:
                    # update the column's type (to indicate the operation has taken place)
                    ml_types[op.column_name] = op_output_type
                continue

            # check the operation is valid for the column
            column_ml_type = ml_types[op.column_name]
            column_tags = ml_types[op.column_name].get_tags()

            op_input_tags = op_input_ml_type.get_tags()
            op_restricted_tags = op.restricted_tags
            if not check_ml_type_valid(op_input_ml_type, column_ml_type):
                return False, {}
            if op_input_tags and len(column_tags.intersection(op_input_tags)) == 0:
                return False, {}
            if (
                op_restricted_tags
                and len(column_tags.intersection(op_restricted_tags)) > 0
            ):
                return False, {}

            # update the column's type (to indicate the operation has taken place)
            if isinstance(op_output_type, MLType):
                ml_types[op.column_name] = op_output_type
            else:
                ml_types[op.column_name] = op_output_type()
    return True, ml_types


def check_ml_type_valid(op_input_ml_type, column_ml_type):
    if isinstance(op_input_ml_type, MLType):
        # if the op takes in anything, the column ml type doesn't matter
        return True
    if column_ml_type == op_input_ml_type:
        return True
    return False
