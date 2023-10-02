from datetime import datetime

import pandas as pd


def clean_date(date):
    if isinstance(date, str):
        return pd.Timestamp(datetime.strptime(date, "%Y-%m-%d"))
    return date


def _bar_format(target_dataframe_index):
    """Template to format the progress bar during a label search."""
    value = "Elapsed: {elapsed} | "
    value += "Remaining: {remaining} | "
    value += "Progress: {l_bar}{bar}| "
    value += target_dataframe_index + ": {n}/{total} "
    return value


def generate_data_slices(df, window_size, gap=None, min_data=None, drop_empty=True):
    window_size = window_size or len(df)
    gap = to_offset(gap or window_size)

    df = df.loc[df.index.notnull()]
    assert (
        df.index.is_monotonic_increasing
    ), "Please sort your dataframe chronologically before calling search"

    if df.empty:
        return

    threshold = min_data or df.index[0]
    df, cutoff_time = cutoff_data(df=df, threshold=threshold)

    if df.empty:
        return

    if isinstance(gap, int):
        cutoff_time = df.index[0]

    metadata = {"slice": 0, "min_data": cutoff_time}

    def iloc(index, i):
        if i < index.size:
            return index[i]

    while not df.empty and cutoff_time <= df.index[-1]:
        if isinstance(window_size, int):
            df_slice = df.iloc[:window_size]
            window_end = iloc(df.index, window_size)

        else:
            if isinstance(window_size, str):
                window_end = cutoff_time + pd.Timedelta(window_size)
            else:
                window_end = cutoff_time + window_size
            df_slice = df[:window_end]

            # Pandas includes both endpoints when slicing by time.
            # This results in the right endpoint overlapping in consecutive data slices.
            # Resolved by making the right endpoint exclusive.
            # https://pandas.pydata.org/docs/user_guide/advanced.html#endpoints-are-inclusive

            if not df_slice.empty:
                is_overlap = df_slice.index == window_end

                if df_slice.index.size > 1 and is_overlap.any():
                    df_slice = df_slice[~is_overlap]

        metadata["window"] = (cutoff_time, window_end)

        if isinstance(gap, int):
            gap_end = iloc(df.index, gap)
            metadata["gap"] = (cutoff_time, gap_end)
            df = df.iloc[gap:]

            if not df.empty:
                cutoff_time = df.index[0]

        else:
            gap_end = cutoff_time + gap
            metadata["gap"] = (cutoff_time, gap_end)
            cutoff_time += gap

            if cutoff_time <= df.index[-1]:
                df = df[cutoff_time:]

        if df_slice.empty and drop_empty:
            continue

        metadata["slice"] += 1

        yield df_slice, metadata


def calculate_target_values(
    df,
    target_dataframe_index,
    labeling_function,
    time_index,
    window_size,
    minimum_data=None,
    maximum_data=None,
    gap=None,
    drop_empty=True,
    verbose=False,
    num_examples_per_instance=-1,
):
    assert labeling_function, "missing labeling function(s)"

    #
    if df.index.name != time_index:
        df = df.set_index(time_index)

    if isinstance(df[target_dataframe_index].dtype, pd.CategoricalDtype):
        df[target_dataframe_index] = df[
            target_dataframe_index
        ].cat.remove_unused_categories()

    records = []
    label_name = _get_function_name(labeling_function)
    for group_key, df_by_index in df.groupby(target_dataframe_index):
        for dataslice, metadata in generate_data_slices(
            df_by_index,
            window_size=window_size,
            gap=gap,
            min_data=minimum_data,
            drop_empty=drop_empty,
        ):
            record = labeling_function(dataslice)
            records.append(
                {
                    target_dataframe_index: group_key,
                    "cutoff_time": dataslice.first_valid_index(),
                    label_name: record,
                },
            )
    records = pd.DataFrame.from_records(records, index=None)
    return records


def _get_function_name(function):
    has_name = hasattr(function, "__name__")
    return function.__name__ if has_name else type(function).__name__


def to_offset(value):
    """Converts a value to an offset and validates the offset.

    Args:
        value (int or str or offset) : Value of offset.

    Returns:
        offset : Valid offset.
    """
    if isinstance(value, int):
        assert value > 0, "offset must be greater than zero"
        offset = value
    elif isinstance(value, str):
        error = "offset must be a valid string"
        assert can_be_type(type=pd.tseries.frequencies.to_offset, string=value), error
        offset = pd.tseries.frequencies.to_offset(value)
        assert offset.n > 0, "offset must be greater than zero"
    elif isinstance(value, pd.Timedelta):
        offset = pd.tseries.frequencies.to_offset(value)
        assert offset.n > 0, "offset must be greater than zero"
    else:
        assert issubclass(type(value), pd.tseries.offsets.BaseOffset), "invalid offset"
        assert value.n > 0, "offset must be greater than zero"
        offset = value
    return offset


def can_be_type(type, string):
    try:
        type(string)
        return True

    except ValueError:
        return False


def cutoff_data(df, threshold):
    """Cuts off data before the threshold.

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
        if can_be_type(type=pd.tseries.frequencies.to_offset, string=threshold):
            threshold = pd.tseries.frequencies.to_offset(threshold)
            assert threshold.n > 0, "threshold must be greater than zero"
            cutoff_time = df.index[0] + threshold

        elif can_be_type(type=pd.Timestamp, string=threshold):
            cutoff_time = pd.Timestamp(threshold)

        else:
            raise ValueError("invalid threshold")

    else:
        is_timestamp = isinstance(threshold, pd.Timestamp)
        assert is_timestamp, "invalid threshold"
        cutoff_time = threshold

    if cutoff_time != df.index[0]:
        df = df[df.index >= cutoff_time]

        if df.empty:
            return df, None

    return df, cutoff_time
