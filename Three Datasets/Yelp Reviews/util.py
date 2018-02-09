import pandas
from pandas import *

df = read_csv('yelp_review.csv')

df = df[:10000]

print(df)

df.to_csv('yelp_review_sampled.csv')