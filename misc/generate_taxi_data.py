import pandas as pd
import numpy as np
import random
COLUMNS = [
	"vendor_id",
	"taxi_id",
	"trip_id",
	"distance",
	"duration",
	"fare",
	"num_passengers",
	]

NUM_VENDORS = 5
NUM_TAXIS_PER_VENDOR = 40
NUM_TRIPS_PER_TAXI = 40

def get_trip_distance():
	mu = 5
	sigma = 2
	sample = np.random.normal(mu, sigma, 1)
	if sample < 0:
		sample = mu
	sample = round(sample, 2)
	return sample

def get_trip_time():
	mu = 15
	sigma = 5
	sample = np.random.normal(mu, sigma, 1)
	if sample < 0:
		sample = mu
	sample = round(sample, 2)
	return sample

def get_trip_fare(trip_distance, trip_time):
	#Based on DC TAXI fare schedule
	return round(3 + 2.16 * trip_distance + 2 * trip_time, 2)

def get_num_passengers():
	return random.randint(1,4)


def generate_row_of_data(vendor_id, taxi_id, trip_id):
	row = []
	row += [vendor_id, 
			taxi_id, 
			trip_id]

	trip_distance = get_trip_distance()
	trip_time = get_trip_time()
	trip_fare = get_trip_fare(trip_distance, trip_time)
	num_passengers = get_num_passengers()

	row += [trip_distance, 
			trip_time,
			trip_fare,
			num_passengers]
	return row

def generate_rows():
	rows = []
	for vendor_id in range(NUM_VENDORS):
		for taxi_id in range(NUM_TAXIS_PER_VENDOR):
			for trip_id in range(NUM_TRIPS_PER_TAXI):
				row = generate_row_of_data(vendor_id, taxi_id, trip_id)
				rows.append(row)
	return rows

def generate_data():
	rows = generate_rows()
	df = pd.DataFrame(rows, columns = COLUMNS)
	return df
	
df = generate_data()
df.to_csv("synthetic_taxi_data.csv", index = False)


















