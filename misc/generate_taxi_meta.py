import json

meta = {
    "path": "",
    "tables": [
        {
            "path": "synthetic_taxi_data.csv",
            "name": "taxi_data",
            "fields": [
                {"name": "vendor_id", "type": "id"},
                {"name": "taxi_id", "type": "id"},
                {"name": "trip_id", "type": "datetime"},
                {"name": "distance", "type": "number", "subtype": "float"},
                {"name": "duration", "type": "number", "subtype": "float"},
                {"name": "fare", "type": "number", "subtype": "float"},
                {"name": "num_passengers", "type": "number", "subtype": "float"}
            ]
        }
    ]
}


if __name__ == "__main__":
    print(json.dumps(meta))
    print(json.dumps(meta), file=open('taxi_meta.json', 'w'))
