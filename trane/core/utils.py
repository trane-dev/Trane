import pandas as pd


def set_dataframe_index(df, index, verbose=False):
    if df.index.name != index:
        if verbose:
            print(f"setting dataframe to index: {index}")
        df = df.set_index(index, inplace=False)
    return df


def generate_data_slices(df, window_size, gap, drop_empty=True, verbose=False):
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
        if drop_empty is True and not dataslice.empty:
            yield dataslice, {"start": start_ts, "end": end_ts}


def calculate_target_values(
    df,
    target_dataframe_index,
    labeling_function,
    time_index,
    window_size,
    drop_empty=True,
    verbose=False,
    nrows=None,
):
    df = set_dataframe_index(df, time_index, verbose=verbose)
    if str(df.index.dtype) == "timestamp[ns][pyarrow]":
        df.index = df.index.astype("datetime64[ns]")
    if nrows and nrows > 0 and nrows < len(df):
        if verbose:
            print("sampling {nrows} rows")
        df = df.sample(n=nrows)
    records = []
    label_name = labeling_function.__name__
    for group_key, df_by_index in df.groupby(target_dataframe_index, observed=True):
        # TODO: support gap
        for dataslice, _ in generate_data_slices(
            df=df_by_index,
            window_size=window_size,
            gap=window_size,
            drop_empty=drop_empty,
            verbose=verbose,
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
