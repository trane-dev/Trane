from datetime import timedelta

import featuretools as ft
import pandas as pd

__all__ = ["FeaturetoolsWrapper"]


class FeaturetoolsWrapper(object):
    """docstring for FeaturetoolsWrapper."""

    def __init__(self, df, entity_col, time_col, variable_types, name):
        assert name != entity_col

        self.entity_col = entity_col
        self.es = ft.EntitySet(id=name)
        self.es = self.es.entity_from_dataframe(entity_id=name,
                                                dataframe=df,
                                                time_index=time_col,
                                                index="__id__",
                                                make_index=True,
                                                variable_types=variable_types
                                                )

        entity_df = pd.DataFrame([[i] for i in set(df[entity_col])], columns=[entity_col])
        self.es = self.es.entity_from_dataframe(entity_id=entity_col,
                                                dataframe=entity_df,
                                                index=entity_col
                                                )
        new_relationship = ft.Relationship(self.es[entity_col][entity_col],
                                           self.es[name][entity_col])
        self.es = self.es.add_relationship(new_relationship)

    def compute_features(self, df, cutoff_strategy, feature_window):
        assert cutoff_strategy.entity_col == self.entity_col

        cutoffs = cutoff_strategy.generate_cutoffs(df)

        cutoffs_ft = []

        for _id, row in cutoffs.iterrows():
            cutoffs_ft.append((row[self.entity_col], row['cutoff_st'] - timedelta(days=1)))

        cutoffs_ft = pd.DataFrame(cutoffs_ft, columns=['instance_id', 'time'])

        feature_matrix, features = ft.dfs(target_entity=self.entity_col,
                                          cutoff_time=cutoffs_ft,
                                          training_window="%dday" % feature_window,  # same as above
                                          entityset=self.es,
                                          cutoff_time_in_index=True,
                                          verbose=True)
        # encode categorical values
        fm_encoded, features_encoded = ft.encode_features(feature_matrix,
                                                          features)

        self.features = fm_encoded.fillna(0)

    def get_feature(self, entity_name, cutoff_st):
        return list(self.features.loc[entity_name, cutoff_st - timedelta(days=1)])
