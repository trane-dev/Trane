import pandas as pd


def set_dataframe_index(df, index):
    if df.index.name != index:
        df = df.set_index(index, inplace=False)
    return df


def generate_data_slices(df, window_size, gap):
    # valid for a specify group of id
    # so we need to groupby id (before this function)
    window_size = pd.to_timedelta(window_size)
    gap = pd.to_timedelta(gap)
    if window_size != gap:
        raise NotImplementedError("window_size != gap is not supported yet")
    for start_ts, dataslice in df.resample(
        window_size,
        closed="left",
        label="left",
        kind="timestamp",
        origin="start",
        offset=gap,
    ):
        # inclusive start_ts and inclusive end_ts
        end_ts = dataslice.index[-1] if not dataslice.empty else start_ts
        yield dataslice, {"start": start_ts, "end": end_ts}


def calculate_target_values(
    df,
    target_dataframe_index,
    labeling_function,
    time_index,
    window_size,
    drop_empty=True,
    verbose=False,
):
    df = set_dataframe_index(df, time_index)
    records = []
    label_name = labeling_function.__name__

    for group_key, df_by_index in df.groupby(target_dataframe_index, observed=True):
        # TODO: support gap
        for dataslice, _ in generate_data_slices(
            df=df_by_index,
            window_size=window_size,
            gap=window_size,
        ):
            if dataslice.empty:
                continue
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
