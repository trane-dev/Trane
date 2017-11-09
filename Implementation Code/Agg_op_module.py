def last(dataframe):
	df = dataframe.copy()
	return df.tail(n = 1)
def first(dataframe):
	df = dataframe.copy()
	return df.head(n = 1)
def last_minus_first(dataframe):
	df = dataframe.copy()
	last_row = last(df)
	first_row = first(df)
	return last_row - first_row
possible_operations = {
	"last" : last,
	"first": first,
	"last minus first" : last_minus_first
}
