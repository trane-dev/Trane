import os

import pandas as pd


def read_json(
    dir_path,
    filename,
    nrows=None,
    lines=True,
    dtype_backend="numpy_nullable",
    engine="ujson",
):
    path = os.path.join(dir_path, filename)
    return pd.read_json(path, lines=lines, nrows=nrows)


def create_yelp_parquets():
    nrows = 50000
    dir_path = "."
    yelp_review_df = read_json(
        dir_path,
        "yelp_academic_dataset_review.json",
        nrows=nrows,
    )
    yelp_business_df = read_json(
        dir_path,
        "yelp_academic_dataset_business.json",
        engine="ujson",
        nrows=nrows,
    )
    yelp_user_df = read_json(dir_path, "yelp_academic_dataset_user.json")

    # don't care about these columns
    yelp_business_df = yelp_business_df.drop(
        columns=["attributes", "categories", "hours"],
    )

    # from our largest dataframe, find the unique for the foreign keys
    valid_business_ids = yelp_review_df["business_id"].unique()
    valid_user_ids = yelp_review_df["user_id"].unique()

    # now we need to make sure that the foreign keys in the review dataframe are valid
    yelp_business_df = yelp_business_df[
        yelp_business_df["business_id"].isin(valid_business_ids)
    ]
    yelp_user_df = yelp_user_df[yelp_user_df["user_id"].isin(valid_user_ids)]

    valid_user_ids = yelp_user_df["user_id"].unique()
    yelp_review_df = yelp_review_df[yelp_review_df["user_id"].isin(valid_user_ids)]

    # check our primary keys are unique
    assert yelp_review_df["review_id"].is_unique
    assert yelp_user_df["user_id"].is_unique
    assert yelp_business_df["business_id"].is_unique

    # check our foreign keys are valid
    assert len(yelp_review_df["user_id"].unique()) == len(yelp_user_df)
    assert len(yelp_review_df["business_id"].unique()) == len(yelp_business_df)

    print("Sampling Results ---")
    print("Number of reviews: {}".format(len(yelp_review_df)))
    print("Number of businesses: {}".format(len(yelp_business_df)))
    print("Number of users: {}".format(len(yelp_user_df)))

    merge_step_1 = yelp_review_df.merge(
        yelp_user_df,
        on="user_id",
        suffixes=("_review", "_user"),
    )
    merged_df = merge_step_1.merge(
        yelp_business_df,
        on="business_id",
        suffixes=(None, "_business"),
    )

    merged_df["date"] = pd.to_datetime(merged_df["date"])
    merged_df = merged_df.rename(columns={"stars_x": "stars"})

    merged_df.to_parquet("yelp.parquet")


create_yelp_parquets()
