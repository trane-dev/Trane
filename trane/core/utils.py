from datetime import datetime

import pandas as pd


def set_dataframe_index(df, time_index):
    if df.index.name != time_index:
        df = df.set_index(time_index)
    return df


def determine_gap_size(gap):
    if isinstance(gap, str):
        return pd.Timedelta(gap)
    elif isinstance(gap, int) or isinstance(gap, pd.Timedelta):
        return gap
    elif not gap:
        return 1
    return int(gap)


def generate_data_slices(
    df,
    window_size,
    gap=1,
    drop_empty=True,
):
    start_idx = 0
    end_idx = len(df) - 1

    gap = determine_gap_size(gap)
    window_size = determine_gap_size(window_size)

    while start_idx < end_idx:
        if isinstance(window_size, pd.Timedelta):
            timestamp_at_start = df.index[start_idx]
            slice_end_timestamp = timestamp_at_start + window_size
            slice_end = df.index[df.index >= slice_end_timestamp].min()
            if pd.isna(slice_end):
                break
            slice_end_idx = df.index.get_loc(slice_end)
            if isinstance(df.index.get_loc(slice_end), slice):
                # multiple matching indices, so we want the first one (for now)
                # TODO: handle this better
                slice_end_idx = slice_end_idx.start
            dataslice = df.iloc[start_idx:slice_end_idx]
            if isinstance(gap, pd.Timedelta):
                start_idx_timestamp = slice_end_timestamp + gap
                nearest_idx = df.index[df.index >= start_idx_timestamp].min()
                if pd.isna(nearest_idx):
                    break
                start_idx = df.index.get_loc(nearest_idx)
            else:
                start_idx = slice_end_idx + gap
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
    gap=1,
    drop_empty=True,
    verbose=False,
):
    df = set_dataframe_index(df, time_index)
    records = []
    label_name = labeling_function.__name__

    for group_key, df_by_index in df.groupby(target_dataframe_index, observed=True):
        for dataslice, metadata in generate_data_slices(
            df_by_index,
            window_size,
            gap,
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


def clean_date(date):
    if isinstance(date, str):
        return pd.Timestamp(datetime.strptime(date, "%Y-%m-%d"))
    return date
