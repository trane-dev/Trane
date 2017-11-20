import pandas as pd
import random
columns = [
	"taxi_id",
	"trip_id",
	"fare",
	"num_passengers",
	"trip_distance",
	]

NUM_TAXIS = 3
NUM_TRIPS_PER_TAXI = 10
rows = []
for taxi_num in range(NUM_TAXIS):
	for trip_num in range(NUM_TRIPS_PER_TAXI):
		for column in columns:
			new_row = []
			new_row.append(taxi_num)
			new_row.append(trip_num)
			fare = random.random()*10
			new_row.append(fare)
			new_row.append(random.randint(1,4))
			distance = fare/2 + random.random()
			new_row.append(distance)
		rows.append(new_row)
df = pd.DataFrame(rows, columns = columns)
df.to_csv("synthetic_taxi_data.csv", index = False)