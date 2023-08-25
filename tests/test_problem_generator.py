import pandas as pd

from trane.core.problem import Problem
from trane.core.problem_generator import ProblemGenerator
from trane.metadata import SingleTableMetadata
from trane.utils.testing_utils import generate_mock_data


def test_problem_generator_single_table():
    dataframe, ml_types, _, primary_key, time_index = generate_mock_data(
        tables=["products"],
    )
    dataframe = dataframe["products"]

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
            check_problem_type(labels, p)
        else:
            thresholds = p.get_recommended_thresholds(dataframe)
            for threshold in thresholds:
                p.set_parameters(threshold)
                labels = p.create_target_values(dataframe)
                check_problem_type(labels, p)


# def test_problem_generator_multi_table():
#     SingleTableMetadata(
#         ml_types=ml_types,
#         primary_key={"order"},
#         time_index=time_index,
#     )


def check_problem_type(labels, p):
    if pd.api.types.is_bool_dtype(labels["target"].dtype):
        assert p.get_problem_type() == "classification"
    else:
        assert p.get_problem_type() == "regression"
