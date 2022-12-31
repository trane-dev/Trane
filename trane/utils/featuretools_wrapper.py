from datetime import timedelta

import featuretools as ft
import pandas as pd

__all__ = ["FeaturetoolsWrapper"]


class FeaturetoolsWrapper(object):
    """docstring for FeaturetoolsWrapper."""

    def __init__(self, df, entity_col, time_col, name):
        assert name != entity_col

        self.entity_col = entity_col
        self.es = ft.EntitySet(id=name)
        self.es = self.es.add_dataframe(
            dataframe_name=name,
            dataframe=df,
            time_index=time_col,
            index="__id__",
            make_index=True,
        )

        entity_df = pd.DataFrame(
            [[i] for i in set(df[entity_col])],
            columns=[entity_col],
        )
        self.es = self.es.add_dataframe(
            dataframe_name=entity_col,
            dataframe=entity_df,
            index=entity_col,
        )

        self.es = self.es.add_relationship(entity_col, entity_col, name, entity_col)

    def compute_features(self, df, cutoff_df, feature_window):
        feature_matrix, features = ft.dfs(
            target_dataframe_name=self.entity_col,
            cutoff_time=cutoff_df,
            entityset=self.es,
            cutoff_time_in_index=True,
            verbose=True,
        )

        # encode categorical values
        fm_encoded, features_encoded = ft.encode_features(feature_matrix, features)

        features = fm_encoded.fillna(0)
        return features

    def get_feature(self, entity_name, cutoff_st):
        return list(self.features.loc[entity_name, cutoff_st - timedelta(days=1)])
