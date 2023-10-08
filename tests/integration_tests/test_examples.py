import pandas as pd
from tqdm import tqdm

from trane.core.problem_generator import ProblemGenerator
from trane.datasets.load_functions import load_airbnb


def test_airbnb_reviews():
    data, metadata = load_airbnb(nrows=1000)
    assert data["id"].is_unique
    window_size = "1m"
    problem_generator = ProblemGenerator(
        metadata=metadata,
        window_size=window_size,
    )
    problems = problem_generator.generate()
    print(f"generated {len(problems)} problems from {data.shape} columns")
    num_target_values_created = 0
    for p in tqdm(problems):
        if p.has_parameters_set() is True:
            labels = p.create_target_values(data)
            num_target_values_created += 1
            if "target" not in labels.columns:
                continue
            if pd.api.types.is_bool_dtype(labels["target"].dtype):
                assert p.get_problem_type() == "classification"
            else:
                assert p.get_problem_type() == "regression"
    print(f"created {num_target_values_created} target values")
