import pandas as pd

from trane.typing.inference_functions import (
    boolean_func,
    categorical_func,
    datetime_func,
    double_func,
    integer_func,
    natural_language_func,
)
from trane.typing.ml_types import (
    Boolean,
    Categorical,
    Datetime,
    Double,
    Integer,
    MLType,
    NaturalLanguage,
    Unknown,
)


def _infer_ml_type(series: pd.Series) -> MLType:
    inference_functions = {
        boolean_func: Boolean,
        categorical_func: Categorical,
        datetime_func: Datetime,
        double_func: Double,
        integer_func: Integer,
        natural_language_func: NaturalLanguage,
    }
    for infer_func, ml_type in inference_functions.items():
        if infer_func(series) is True:
            return ml_type()
    return Unknown()


def infer_metadata(
    df: pd.DataFrame,
):
    metadata = {}
    for col in df.columns:
        metadata[col] = _infer_ml_type(df[col])
    return metadata
