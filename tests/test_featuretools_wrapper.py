from featuretools import (
    AggregationFeature,
    TransformFeature,
)

from trane.core.prediction_problem import PredictionProblem
from trane.ops.aggregation_ops import CountAggregationOp
from trane.ops.filter_ops import AllFilterOp
from trane.utils.featuretools_wrapper import FeaturetoolsWrapper


def convert_logical_types_to_woodwork(meta):
    logical_types = {}
    for col, (logical_type, _) in meta.items():
        logical_types[col] = logical_type
    return logical_types


def test_prediction_problem(make_fake_df, make_fake_meta, make_cutoff_strategy):
    entity_col = "id"
    time_col = "date"
    meta = make_fake_meta
    cutoff_strategy = make_cutoff_strategy
    dataframe = make_fake_df

    problem = PredictionProblem(
        operations=[AllFilterOp(None), CountAggregationOp(None)],
        entity_col=entity_col,
        time_col=time_col,
        table_meta=meta,
        cutoff_strategy=cutoff_strategy,
    )
    assert problem.is_valid()
    label_times = problem.execute(dataframe, num_examples_per_instance=-1)
    label_times.rename(columns={"_execute_operations_on_df": "label"}, inplace=True)

    logical_types = convert_logical_types_to_woodwork(meta)

    features = FeaturetoolsWrapper(
        dataframe_name="customers",
        dataframe=dataframe,
        entity_col=entity_col,
        time_col=time_col,
        entityset_name="customer_dataset",
        logical_types=logical_types,
    )
    feature_matrix, features = features.compute_features(label_times)
    for feature in features:
        assert isinstance(feature, (AggregationFeature, TransformFeature))
    assert feature_matrix.shape[0] == label_times.shape[0]
