import pandas as pd
"""
Convert a dataframe with an entity_id column to a dictionary mapping entity id's to their relevant data.
Args:
	(Dataframe): Dataframe containing all the data
	(String) entity_id_column_name: the column name with entity id's 

Returns:
	(Dict): Mapping from entity id's to a dataframe containing only that entity id's data.
"""
def df_to_entity_id_to_df(dataframe, entity_id_column_name):
	entity_ids = set(dataframe[entity_id_column_name])
	entity_id_to_df = {}
	for entity_id in entity_ids:
		relevant_data = dataframe.loc[dataframe[entity_id_column_name] == entity_id]
		entity_id_to_df[entity_id] = relevant_data
	return entity_id_to_df

if __name__ == "__main__":
	df = pd.read_csv('../../test_datasets/synthetic_taxi_data.csv')
	print df
	print df_to_entity_id_to_df(df, 'taxi_id')[0]
	print df_to_entity_id_to_df(df, 'taxi_id')[1]
	print df_to_entity_id_to_df(df, 'taxi_id')[2]