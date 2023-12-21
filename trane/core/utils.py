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


# def generate_data_slices(
#     df,
#     window_size,
#     gap=1,
#     drop_empty=True,
# ):
#     pass
#     start_idx = 0
#     end_idx = len(df)

#     gap = determine_gap_size(gap)
#     window_size = determine_window_size(window_size)

#     while start_idx < end_idx:
#         if isinstance(window_size, pd.Timedelta):
#             timestamp_at_start = df.iloc[[start_idx]].index[0]
#             slice_end_timestamp = timestamp_at_start + window_size
#             slice_end = df.index[df.index >= slice_end_timestamp].min()
#             if pd.isna(slice_end):
#                 break
#             slice_end_idx = df.index.get_loc(slice_end)
#             if isinstance(df.index.get_loc(slice_end), slice):
#                 # multiple matching indices, so we want the first one (for now)
#                 # TODO: handle this better
#                 slice_end_idx = slice_end_idx.start
#             dataslice = df.iloc[start_idx:slice_end_idx]
#             if isinstance(gap, pd.Timedelta):
#                 start_idx_timestamp = slice_end_timestamp + gap
#                 nearest_idx = df.index[df.index >= start_idx_timestamp].min()
#                 if pd.isna(nearest_idx):
#                     break
#                 start_idx = df.index.get_loc(nearest_idx)
#             else:
#                 start_idx = slice_end_idx + gap
#         else:
#             slice_end = start_idx + window_size
#             if slice_end > end_idx:
#                 break
#             dataslice = df.iloc[start_idx:slice_end]
#             start_idx = slice_end + gap

#         # Make sure slice_end is exclusive
#         if not drop_empty or not dataslice.empty:
#             yield dataslice, {"start": start_idx, "end": slice_end}


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
    pass


#     df = set_dataframe_index(df, time_index)
#     records = []
#     label_name = labeling_function.__name__

#     for group_key, df_by_index in df.groupby(target_dataframe_index, observed=True):
#         print(f"Group {group_key}")
#         print(f"df_by_index {df_by_index}")
#         for dataslice, metadata in generate_data_slices(
#             df_by_index,
#             window_size,
#             gap,
#             drop_empty,
#         ):
#             print(f"dataslice {dataslice}")
#             record = labeling_function(dataslice)
#             records.append(
#                 {
#                     target_dataframe_index: group_key,
#                     "cutoff_time": dataslice.first_valid_index(),
#                     label_name: record,
#                 },
#             )
#     records = pd.DataFrame.from_records(records, index=None)
#     return records
