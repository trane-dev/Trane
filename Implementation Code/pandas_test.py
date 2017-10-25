import pandas as pd
df = pd.DataFrame([[74, 200, 22, "Alex"],[71, 140, 19, "Shea"], [75, 170, 20, "Abby"]], columns = ['height', 'weight', 'age', 'name'])
df.loc[3] = {"height":78, "name":"Future Alex", "weight":105, "age":25}
print df