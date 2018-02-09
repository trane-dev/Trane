import pandas as pd

df = pd.read_json('github_archive.json', lines = True)

for idx, row in enumerate(df.iterrows()):
	actor_id = row[1]['actor']['id']
	df.at[idx, 'actor'] = actor_id

	repo_id = row[1]['repo']['id']
	df.at[idx, 'repo'] = repo_id

print(df)

df.to_csv('github_archive_v0.csv')

