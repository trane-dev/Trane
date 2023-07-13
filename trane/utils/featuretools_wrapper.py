import featuretools as ft
import pandas as pd


def create_unique_ids_df(df, entity_col):
    unique_entity_ids = df[entity_col].unique()
    entity_df = pd.Series(unique_entity_ids, name=entity_col).to_frame()
    return entity_df


class FeaturetoolsWrapper(object):
    def __init__(
        self,
        dataframe_name: str,
        dataframe: pd.DataFrame,
        entity_col: str,
        time_col: str,
        entityset_name,
        logical_types=None,
    ):
        assert dataframe_name != entity_col

        self.entity_col = entity_col
        self.es = ft.EntitySet(id=entityset_name)
        self.es = self.es.add_dataframe(
            dataframe_name=dataframe_name,
            dataframe=dataframe,
            time_index=time_col,
            index="__id__",
            make_index=True,
            logical_types=logical_types,
        )

        entity_df = create_unique_ids_df(dataframe, entity_col)
        self.es = self.es.add_dataframe(
            dataframe_name=entity_col,
            dataframe=entity_df,
            index=entity_col,
        )
        # each entity has multiple values in the base dataframe
        self.es = self.es.add_relationship(
            entity_col,
            entity_col,
            dataframe_name,
            entity_col,
        )

    def compute_features(
        self,
        label_times: pd.DataFrame,
        agg_primitives=None,
        trans_primitives=None,
        max_depth=2,
        n_jobs=1,
        verbose=False,
        max_features=-1,
    ):
        feature_matrix, features = ft.dfs(
            target_dataframe_name=self.entity_col,
            cutoff_time=label_times,
            entityset=self.es,
            cutoff_time_in_index=True,
            agg_primitives=agg_primitives,
            trans_primitives=trans_primitives,
            max_depth=max_depth,
            n_jobs=n_jobs,
            max_features=max_features,
            verbose=verbose,
        )
        return feature_matrix, features

    def encode_features(self, feature_matrix, features):
        feature_matrix_encoded, features_encoded = ft.encode_features(
            feature_matrix,
            features,
        )
        return feature_matrix_encoded.fillna(0), features_encoded

    # def get_feature(self, entity_name, cutoff_st):
    #     return list(self.features.loc[entity_name, cutoff_st - timedelta(days=1)])
