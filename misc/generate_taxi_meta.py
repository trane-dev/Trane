import json

meta = [{"name": "vendor_id", "type": "identifier"},
        {"name": "taxi_id", "type": "identifier"},
        {"name": "trip_id", "type": "time"},
        {"name": "distance", "type": "value"},
        {"name": "duration", "type": "value"},
        {"name": "fare", "type": "value"},
        {"name": "num_passengers", "type": "value"}]

if __name__ == "__main__":
    print(json.dumps(meta))
    print(json.dumps(meta), file=open('taxi_meta.json', 'w'))
