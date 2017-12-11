import json

meta = [{"name": "time", "type": "time"},
        {"name": "taxi_id", "type": "identifier"},
        {"name": "trip_id", "type": "identifier"},
        {"name": "fare", "type": "value"},
        {"name": "num_passengers", "type": "value"},
        {"name": "trip_distance", "type": "value"},
        {"name": "description", "type": "text"},
        {"name": "cartype", "type": "category"}]

if __name__ == "__main__":
    print(json.dumps(meta))
    print(json.dumps(meta), file=open('taxi_meta.json', 'w'))
