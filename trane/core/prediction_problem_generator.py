import copy
import itertools

from tqdm.notebook import tqdm

from trane.core.prediction_problem import PredictionProblem
from trane.core.utils import _parse_table_meta, get_semantic_tags
from trane.ops import aggregation_ops as agg_ops
from trane.ops import filter_ops
from trane.ops.filter_ops import AllFilterOp
from trane.ops.threshold_functions import get_k_most_frequent
from trane.typing.column_schema import ColumnSchema
from trane.typing.inference import infer_table_meta
from trane.typing.logical_types import (
    Categorical,
    Datetime,
    Integer,
)

__all__ = ["PredictionProblemGenerator"]


class PredictionProblemGenerator:
    """
    Object for generating prediction problems on data.
    """

    def __init__(self, df, entity_col, time_col, table_meta=None, cutoff_strategy=None):
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
        self.df = df
        self.entity_col = entity_col
        self.time_col = time_col
        self.cutoff_strategy = cutoff_strategy

        inferred_table_meta = False
        if table_meta is None:
            self.table_meta = infer_table_meta(df)
            inferred_table_meta = True
        else:
            self.table_meta = _parse_table_meta(table_meta)
        self.transform_data()
        self.ensure_valid_inputs(inferred_table_meta)

    def ensure_valid_inputs(self, inferred_table_meta=False):
        """
        TypeChecking for the problem generator entity_col
        and label_col. Errors if types don't match up.
        """
        for col, column_schema in self.table_meta.items():
            assert isinstance(col, str)
            assert isinstance(column_schema, ColumnSchema)
            assert col in self.df.columns
            assert column_schema.logical_type.dtype == str(self.df[col].dtype)

        entity_col_type = self.table_meta[self.entity_col]
        assert entity_col_type.logical_type in [Integer, Categorical]
        if inferred_table_meta is False:
            assert "index" in entity_col_type.semantic_tags
        else:
            self.table_meta[self.entity_col].semantic_tags.add("index")

        time_col_type = self.table_meta[self.time_col]
        assert time_col_type.logical_type == Datetime

    def transform_data(self):
        """
        Transform the data to the correct types.
        """
        for col, column_schema in self.table_meta.items():
            expected_logical_type = column_schema.logical_type
            if self.df[col].dtype != expected_logical_type.dtype:
                self.df[col] = expected_logical_type().transform(series=self.df[col])

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
                if filter_col == self.time_col or agg_col == self.time_col:
                    continue
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
            all_attempts += 1
            ag_col, filter_col, agg_op_name, filter_op_name = op_col_combo

            agg_op_obj = getattr(agg_ops, agg_op_name)(ag_col)  # noqa
            filter_op_obj = getattr(filter_ops, filter_op_name)(filter_col)  # noqa

            # Note: the order of the operations matters, the filter operation must be first
            operations = [filter_op_obj, agg_op_obj]

            problem = PredictionProblem(
                operations=operations,
                entity_col=self.entity_col,
                time_col=self.time_col,
                table_meta=self.table_meta,
                cutoff_strategy=self.cutoff_strategy,
            )
            if problem.is_valid() and generate_thresholds is True:
                filter_op = problem.operations[0]
                if isinstance(filter_op, AllFilterOp):
                    # the filter operation does not require a threshold
                    problems.append(problem)
                    success_attempts += 1
                else:
                    yielded_thresholds = self._threshold_recommend(filter_op, df)
                    for threshold in yielded_thresholds:
                        final_problem = copy.deepcopy(problem)
                        final_problem.operations[0].set_parameters(
                            threshold=threshold,
                        )
                        problems.append(final_problem)
                        success_attempts += 1
            elif problem.is_valid():
                problems.append(problem)
                success_attempts += 1
            if n_problems and success_attempts >= n_problems:
                break
        return problems

    def _threshold_recommend(self, filter_op, df):
        yielded_thresholds = []
        valid_semantic_tags = get_semantic_tags(filter_op)

        if "category" in valid_semantic_tags:
            yielded_thresholds = recommend_categorical_thresholds(
                df,
                filter_op,
            )
        elif "numeric" in valid_semantic_tags:
            yielded_thresholds = recommend_numeric_thresholds(
                df=df,
                filter_op=filter_op,
            )
        return yielded_thresholds


def recommend_categorical_thresholds(df, filter_op, k=3):
    thresholds = get_k_most_frequent(
        df[filter_op.column_name],
        k=k,
    )
    thresholds = list(set(thresholds))
    return thresholds


def recommend_numeric_thresholds(
    df,
    filter_op,
    keep_rates=[0.25, 0.5, 0.75],
):
    thresholds = []
    for keep_rate in keep_rates:
        threshold = filter_op.find_threshold_by_fraction_of_data_to_keep(
            fraction_of_data_target=keep_rate,
            df=df,
            label_col=filter_op.column_name,
        )
        thresholds.append(threshold)
    return thresholds
