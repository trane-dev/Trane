from datetime import datetime

import pandas as pd
from pandas.tseries.offsets import DateOffset


def set_dataframe_index(df, time_index):
    if df.index.name != time_index:
        df = df.set_index(time_index)
    return df


def determine_start_index(df, minimum_data):
    if isinstance(minimum_data, int):
        return minimum_data
    elif isinstance(minimum_data, str):
        return df.index.get_loc(pd.Timestamp(minimum_data))
    return 0


def determine_gap_size(gap):
    if isinstance(gap, str):
        return pd.Timedelta(gap)
    return int(gap)


def generate_data_slices(
    df,
    window_size,
    gap=1,
    min_data=None,
    drop_empty=True,
):
    if min_data is None:
        start_idx = 0
    else:
        start_idx = determine_start_index(df, min_data)

    gap = determine_gap_size(gap)
    end_idx = len(df) - 1

    while start_idx < end_idx:
        if isinstance(window_size, pd.Timedelta):
            timestamp_at_start = df.index[start_idx]
            slice_end_timestamp = timestamp_at_start + window_size
            slice_end = df.index[df.index >= slice_end_timestamp].min()
            if pd.isna(slice_end):
                break
            slice_end_idx = df.index.get_loc(slice_end)
            dataslice = df.iloc[start_idx:slice_end_idx]
            start_idx = slice_end_idx + gap  # start after the gap
        else:
            slice_end = start_idx + window_size
            if slice_end > end_idx:
                break
            dataslice = df.iloc[start_idx:slice_end]
            start_idx = slice_end + gap

        # Make sure slice_end is exclusive
        if not drop_empty or not dataslice.empty:
            yield dataslice, {"start": start_idx, "end": slice_end}


def calculate_target_values(
    df,
    target_dataframe_index,
    labeling_function,
    time_index,
    window_size,
    minimum_data=None,
    maximum_data=None,
    gap=1,
    drop_empty=True,
    verbose=False,
    num_examples_per_instance=-1,
):
    df = set_dataframe_index(df, time_index)
    records = []
    label_name = labeling_function.__name__

    for group_key, df_by_index in df.groupby(target_dataframe_index):
        for dataslice, metadata in generate_data_slices(
            df_by_index,
            window_size,
            gap,
            minimum_data,
            drop_empty,
        ):
            record = labeling_function(dataslice)
            records.append(
                {
                    target_dataframe_index: group_key,
                    "cutoff_time": dataslice.first_valid_index(),
                    label_name: record,
                },
            )

        if verbose:
            print(f"Processed label for group {group_key}")

    records = pd.DataFrame.from_records(records, index=None)
    return records


def _get_function_name(function):
    has_name = hasattr(function, "__name__")
    return function.__name__ if has_name else type(function).__name__


def to_offset(value):
    """Converts a pd.DateOffset object from string or pd.Timedelta object."""
    error_msg = "offset must be greater than zero"
    if isinstance(value, int):
        assert value > 0, error_msg
        offset = value
    elif isinstance(value, (str, pd.Timedelta)):
        offset = pd.tseries.frequencies.to_offset(value)
        assert offset.n > 0, error_msg
    elif isinstance(value, DateOffset):
        assert value.n > 0, error_msg
        offset = value
    return offset


def can_be_type(type_, string):
    try:
        type_(string)
        return True
    except ValueError:
        return False


def cutoff_data(df, threshold, type_="before"):
    """Cuts off data before/after the threshold. For example, if you have daily data points from
        "2022-01-01" to "2022-01-05", and select a threshold of 2/2D/"2022-01-03", the data
        will be cut off at "2022-01-03" (inclusive), which means the data will be from
        "2022-01-03" to "2022-01-05" (inclusive).

        By default,

        If the threshold is greater than the last time in the index, the data will be empty.

    Args:
        df (DataFrame) : Data frame to cutoff data.
        threshold (int or str or Timestamp) : Threshold to apply on data.
            If integer, the threshold will be the time at `n + 1` in the index.
            If string, the threshold can be an offset or timestamp.
            An offset will be applied relative to the first time in the index.

    Returns:
        DataFrame, Timestamp : Returns the data frame and the applied cutoff time.
    """
    if isinstance(threshold, int):
        assert threshold > 0, "threshold must be greater than zero"
        df = df.iloc[threshold:]
        if df.empty:
            return df, None
        cutoff_time = df.index[0]
    elif isinstance(threshold, str):
        if can_be_type(type_=pd.tseries.frequencies.to_offset, string=threshold):
            threshold = pd.tseries.frequencies.to_offset(threshold)
            assert threshold.n > 0, "threshold must be greater than zero"
            cutoff_time = df.index[0] + threshold
        elif can_be_type(type_=pd.Timestamp, string=threshold):
            cutoff_time = pd.Timestamp(threshold)
        else:
            raise ValueError("invalid threshold")
    else:
        is_timestamp = isinstance(threshold, pd.Timestamp)
        assert is_timestamp, "invalid threshold"
        cutoff_time = threshold

    if cutoff_time != df.index[0]:
        if type_ == "before":
            df = df[df.index >= cutoff_time]
        else:  # after
            df = df[df.index <= cutoff_time]
        if df.empty:
            return df, None
    return df, cutoff_time


def clean_date(date):
    if isinstance(date, str):
        return pd.Timestamp(datetime.strptime(date, "%Y-%m-%d"))
    return date
