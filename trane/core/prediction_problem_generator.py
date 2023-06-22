import copy
import heapq
import itertools
import random

from tqdm.notebook import tqdm
from woodwork.column_schema import ColumnSchema
from woodwork.logical_types import (
    Categorical,
    Datetime,
    Integer,
)

from trane.core.prediction_problem import PredictionProblem
from trane.ops import aggregation_ops as agg_ops
from trane.ops import filter_ops

__all__ = ["PredictionProblemGenerator"]


class PredictionProblemGenerator:
    """
    Object for generating prediction problems on data.
    """

    def __init__(self, table_meta, entity_col, time_col, cutoff_strategy=None):
        """
        Parameters
        ----------
        table_meta: a dictionary containing typing information about the data
        entity_col: column name of
            the column containing entities in the data
        time_col: column name of the column
            containing time information in the data

        Returns
        -------
        None
        """
        self.table_meta = table_meta
        self.entity_col = entity_col
        self.time_col = time_col
        self.cutoff_strategy = cutoff_strategy
        self.ensure_valid_inputs()

    def generate(
        self,
        df,
        generate_thresholds=False,
        n_problems=None,
        pbar_position=0,
    ):
        """
        Generate the prediction problems. The prediction problems operations
        hyper parameters are also set.

        Parameters
        ----------
        df (pd.DataFrame): Dataframe to use to generate relevant thresholds. You could use a sample of the
            dataframe to speed up the process.
        generate_thresholds (optional, bool): Whether to automatically generate thresholds for problems.
            Defaults to False.
        n_problems (optional, int): Maximum number of problems to generate. Defaults to generating all possible
            problems.

        Returns
        -------
        problems: a list of Prediction Problem objects.
        """
        if generate_thresholds and df is None:
            raise ValueError("Must provide a dataframe sample to generate thresholds")

        # a list of problems that will eventually be returned
        problems = []
        all_columns = list(self.table_meta.keys())

        possible_ops = []
        for agg, filter_ in itertools.product(
            agg_ops.AGGREGATION_OPS,
            filter_ops.FILTER_OPS,
        ):
            filter_columns = all_columns
            if filter_ == "AllFilterOp":
                filter_columns = [None]

            agg_columns = all_columns
            if agg == "CountAggregationOp":
                agg_columns = [None]
            for filter_col, agg_col in itertools.product(
                filter_columns,
                agg_columns,
            ):
                if filter_col != self.entity_col and agg_col != self.entity_col:
                    possible_ops.append((agg_col, filter_col, agg, filter_))

        total_attempts = len(possible_ops)
        all_attempts = 0
        success_attempts = 0
        for op_col_combo in tqdm(
            possible_ops,
            total=total_attempts,
            position=pbar_position,
        ):
            print(
                "\rSuccess/Attempt = {}/{}".format(success_attempts, all_attempts),
                end="",
            )
            all_attempts += 1
            ag_col, filter_col, agg_op_name, filter_op_name = op_col_combo

            agg_op_obj = getattr(agg_ops, agg_op_name)(ag_col)  # noqa
            filter_op_obj = getattr(filter_ops, filter_op_name)(filter_col)  # noqa

            operations = [filter_op_obj, agg_op_obj]

            problem = PredictionProblem(
                operations=operations,
                entity_col=self.entity_col,
                time_col=self.time_col,
                table_meta=self.table_meta,
                cutoff_strategy=self.cutoff_strategy,
            )
            if problem.is_valid() and generate_thresholds:
                for final_problem, _ in self._threshold_recommend(problem, df):
                    problems.append(final_problem)
                    success_attempts += 1
            elif problem.is_valid():
                problems.append(problem)
                success_attempts += 1
            if n_problems and success_attempts >= n_problems:
                break

        print("\rSuccess/Attempt = {}/{}".format(success_attempts, all_attempts))
        return problems

    def generate_thresholds(self, problems, df_sample):
        """
        Generate thresholds for all problems in a list of prediction problems if
        thresholds can be generated for them.

        Parameters
        ----------
        problems: a list of Prediction Problem objects that may need thresholds
        to be set
        df_sample: a sample of the dataframe that will be used to execute the
        prediction problems to generate thresholds from

        Returns
        -------
        problems: a list of Prediction Problem objects with thresholds set or the
        original problem if no thresholds are needed.
        """
        final_problems = []
        for problem in problems:
            for final_problem, _ in self._threshold_recommend(problem, df_sample):
                final_problems.append(final_problem)
        return final_problems

    def ensure_valid_inputs(self):
        """
        TypeChecking for the problem generator entity_col
        and label_col. Errors if types don't match up.
        """
        for col, col_type in self.table_meta.items():
            assert isinstance(col, str)
            assert isinstance(col_type, ColumnSchema)

        entity_col_type = self.table_meta[self.entity_col]
        assert entity_col_type.logical_type in [Integer(), Categorical()]
        assert "index" in entity_col_type.semantic_tags

        time_col_type = self.table_meta[self.time_col]
        assert time_col_type.logical_type == Datetime()

    def _threshold_recommend(self, problem, df):
        yielded_thresholds = []
        filter_op = problem.operations[0]
        if len(filter_op.REQUIRED_PARAMETERS) == 0:
            yield copy.deepcopy(problem), "no threshold"
        else:
            if filter_op.input_type == ColumnSchema(semantic_tags={"category"}):
                most_frequent_categories = get_k_most_frequent(
                    df[filter_op.column_name],
                    k=3,
                )
                for category in most_frequent_categories:
                    if category not in yielded_thresholds:
                        yielded_thresholds.append(category)
                        problem_final = copy.deepcopy(problem)
                        problem_final.operations[0].set_hyper_parameter(
                            parameter_name="threshold",
                            parameter_value=category,
                        )
                        yield problem_final, "threshold: {}".format(category)
                    else:
                        continue
            elif filter_op.input_type == ColumnSchema(semantic_tags={"numeric"}):
                for keep_rate in [0.25, 0.5, 0.75]:
                    threshold = filter_op.find_threshhold_by_remaining(
                        fraction_of_data_target=keep_rate,
                        df=df,
                        col=filter_op.column_name,
                    )
                    if threshold not in yielded_thresholds:
                        yielded_thresholds.append(threshold)
                        problem_final = copy.deepcopy(problem)
                        problem_final.operations[0].set_hyper_parameter(
                            parameter_name="threshold",
                            parameter_value=threshold,
                        )
                        yield problem_final, "threshold: {} (keep {}%)".format(
                            threshold,
                            keep_rate * 100,
                        )
                    else:
                        continue


def get_k_most_frequent(series, k=3):
    # get the top k most frequent values
    if series.dtype in ["category", "object"]:
        return series.value_counts()[:k].index.tolist()
    raise ValueError("Series must be categorical or object dtype")


def recommend_numeric_threshold(
    df,
    col,
    filter_op,
    num_random_samples=10,
    num_rows_to_execute_on=2000,
    keep_rates=[0.25, 0.5, 0.75],
):
    for keep_rate in keep_rates:
        df, unique_vals = _sample_df_and_unique_values(
            df=df,
            col=col,
            max_num_unique_values=num_random_samples,
            max_num_rows=num_rows_to_execute_on,
        )

        filter_op.find_threshhold_by_remaining(
            fraction_of_data_target=keep_rate,
            df=df,
            col=filter_op.column_name,
        )


def _sample_df_and_unique_values(
    df,
    col,
    max_num_unique_values,
    max_num_rows,
):
    unique_vals = set(df[col])

    if len(unique_vals) > max_num_unique_values:
        unique_vals = heapq.nlargest(
            max_num_unique_values,
            unique_vals,
            key=lambda L: random.random(),
        )

    if len(df) > max_num_rows:
        df = df.sample(max_num_rows)

    return (df, unique_vals)
