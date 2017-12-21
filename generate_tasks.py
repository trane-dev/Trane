import sys
sys.dont_write_bytecode = True
import pandas as pd
import trane
import logging
import json
import numpy as np

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', 
                        datefmt='%Y/%m/%d %H:%M:%S', level=logging.DEBUG)

    # table_meta = open('../test_datasets/taxi_meta.json').read()
    table_meta = open('../test_datasets/donations_meta.json').read()
    table_meta = json.loads(table_meta)
    # gen = PredictionProblemGenerator(table_meta, entity_id_column='taxi_id')
    gen = trane.PredictionProblemGenerator(table_meta, entity_id_column='projectid')
    
    cnt = 0
    lst = []
    for problem in gen.generate():
        lst.append(problem)
        cnt += 1

    logging.info("Generate %d problems." % cnt)

    label_gen = trane.LabelGenerator(np.random.choice(lst, 5))
    jsonstr = label_gen.to_json()
    with open("tasks.json", "w") as f:
        print(jsonstr, file=f)
