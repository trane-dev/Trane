import copy
import itertools

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
from trane.utils.table_meta import TableMeta

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
        df_sample=None,
        generate_thresholds=False,
        n_problems=None,
        pbar_position=0,
    ):
        """
        Generate the prediction problems. The prediction problems operations
        hyper parameters are also set.

        Parameters
        ----------
        df_sample (optional, pd.DataFrame): Dataframe sample to use to generate relevant thresholds.
            Defaults to None (does not automatically generate thresholds).
        generate_thresholds (optional, bool): Whether to automatically generate thresholds for problems.
            Defaults to False.
        n_problems (optional, int): Maximum number of problems to generate. Defaults to generating all possible
            problems.

        Returns
        -------
        problems: a list of Prediction Problem objects.
        """
        if generate_thresholds and df_sample is None:
            raise ValueError("Must provide a dataframe sample to generate thresholds")

        # a list of problems that will eventually be returned
        problems = []

        def iter_over_ops():
            for agg, filter_ in itertools.product(
                agg_ops.AGGREGATION_OPS,
                filter_ops.FILTER_OPS,
            ):
                all_columns = list(self.table_meta.keys())

                filter_cols = [None] if filter_ == "AllFilterOp" else all_columns
                agg_cols = [None] if agg == "CountAggregationOp" else all_columns
                for filter_col, agg_col in itertools.product(filter_cols, agg_cols):
                    if filter_col != self.entity_col and agg_col != self.entity_col:
                        yield agg_col, filter_col, agg, filter_

        # might be inefficent
        total_attempts = sum(1 for _ in iter_over_ops())
        all_attempts = 0
        success_attempts = 0
        for op_col_combo in tqdm(
            iter_over_ops(),
            total=total_attempts,
            position=pbar_position,
        ):
            # for op_col_combo in iter_over_ops():
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
                for final_problem, _ in self._threshold_recommend(problem, df_sample):
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

        # assert self.table_meta.get_type(self.entity_col) in [
        #     TableMeta.TYPE_IDENTIFIER,
        #     TableMeta.TYPE_TEXT,
        #     TableMeta.TYPE_CATEGORY,
        # ]

    def _categorical_threshold(self, df_col, k=3):
        counter = {}
        for item in df_col:
            try:
                counter[item] += 1
            except BaseException:
                counter[item] = 1

        counter_tuple = list(counter.items())
        counter_tuple = sorted(counter_tuple, key=lambda x: -x[1])
        counter_tuple = counter_tuple[:3]
        return [item[0] for item in counter_tuple]

    def _threshold_recommend(self, problem, df_sample):
        yielded_thresholds = []
        filter_op = problem.operations[0]
        if len(filter_op.REQUIRED_PARAMETERS) == 0:
            yield copy.deepcopy(problem), "no threshold"
        else:
            print(filter_op.input_type)
            breakpoint()
            if filter_op.input_type == TableMeta.TYPE_CATEGORY:
                for item in self._categorical_threshold(
                    df_sample[filter_op.column_name],
                ):
                    if item not in yielded_thresholds:
                        yielded_thresholds.append(item)
                        problem_final = copy.deepcopy(problem)
                        problem_final.operations[0].set_hyper_parameter(
                            parameter_name="threshold",
                            parameter_value=item,
                        )
                        yield problem_final, "threshold: {}".format(item)
                    else:
                        continue
            elif filter_op.input_type in [TableMeta.TYPE_FLOAT, TableMeta.TYPE_INTEGER]:
                for keep_rate in [0.25, 0.5, 0.75]:
                    threshold = filter_op.find_threshhold_by_remaining(
                        fraction_of_data_target=keep_rate,
                        df=df_sample,
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
