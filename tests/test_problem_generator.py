import pandas as pd
import pytest
from tqdm import tqdm

from trane.core.problem import Problem
from trane.core.problem_generator import ProblemGenerator
from trane.metadata import MultiTableMetadata, SingleTableMetadata
from trane.utils.testing_utils import generate_mock_data


def test_problem_generator_single_table():
    tables = ["products"]
    target_table = "products"
    dataframe, ml_types, _, primary_key, time_index = generate_mock_data(
        tables=tables,
    )
    dataframe = dataframe[target_table]

    # 1. User creates single table metadata
    metadata = SingleTableMetadata(
        ml_types=ml_types,
        primary_key="id",
        time_index=time_index,
    )

    # 2. User specifies their window size (for each data slice)
    window_size = "2d"
    problem_generator = ProblemGenerator(
        metadata=metadata,
        window_size=window_size,
    )
    problems = problem_generator.generate()
    for p in problems:
        assert isinstance(p, Problem)

    # 3. Generate target values for each problem
    for p in problems:
        if p.has_parameters_set() is True:
            labels = p.create_target_values(dataframe)
            check_problem_type(labels, p.get_problem_type())
        else:
            thresholds = p.get_recommended_thresholds(dataframe)
            for threshold in thresholds:
                p.set_parameters(threshold)
                labels = p.create_target_values(dataframe)
                check_problem_type(labels, p.get_problem_type())


@pytest.mark.parametrize(
    "tables,target_table",
    [
        (["products", "logs"], "logs"),
        (["products", "logs", "sessions"], "sessions"),
        (["products", "logs", "sessions", "customers"], "logs"),
    ],
)
def test_problem_generator_multi(tables, target_table):
    (
        dataframes,
        ml_types,
        relationships,
        primary_keys,
        time_indices,
    ) = generate_mock_data(
        tables=tables,
    )
    metadata = MultiTableMetadata(
        ml_types=ml_types,
        primary_keys=primary_keys,
        relationships=relationships,
        time_indices=time_indices,
    )
    window_size = "2d"
    problem_generator = ProblemGenerator(
        metadata=metadata,
        window_size=window_size,
        target_table=target_table,
    )
    problems = problem_generator.generate()
    num_columns = len(problems[0].metadata.ml_types.keys())
    print(f"generated {len(problems)} problems from {num_columns} columns")
    for p in tqdm(problems):
        if p.has_parameters_set() is True:
            labels = p.create_target_values(dataframes)
            check_problem_type(labels, p.get_problem_type())
        else:
            thresholds = p.get_recommended_thresholds(dataframes)
            for threshold in thresholds:
                p.set_parameters(threshold)
                labels = p.create_target_values(dataframes)
                check_problem_type(labels, p.get_problem_type())


def check_problem_type(labels, problem_type):
    if "target" not in labels.columns:
        return None
    if pd.api.types.is_bool_dtype(labels["target"].dtype):
        assert problem_type == "classification"
    else:
        assert problem_type == "regression"
