import sys
sys.dont_write_bytecode = True
import pandas as pd
import trane
import logging
import json
import numpy as np
from configparser import ConfigParser

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', 
                        datefmt='%Y/%m/%d %H:%M:%S', level=logging.DEBUG)
    parser = ConfigParser()
    parser.read(sys.argv[1])
    metafile = parser.get('ALL', 'META')
    table_meta = open(metafile).read()
    table_meta = json.loads(table_meta)
    entity_id_column = parser.get('ALL', 'ENTITY_ID_COLUMN')
    gen = trane.PredictionProblemGenerator(table_meta, entity_id_column=entity_id_column)
    
    cnt = 0
    lst = []
    for problem in gen.generate():
        lst.append(problem)
        cnt += 1

    logging.info("Generate %d problems." % cnt)
    
    num_output = int(parser.get('ALL', 'NUM_OUTPUT'))
    label_gen = trane.LabelGenerator(np.random.choice(lst, num_output))
    jsonstr = label_gen.to_json()
    with open(parser.get('ALL', 'PROBLEM_OUTPUT'), "w") as f:
        json.dump(json.loads(jsonstr), f, indent=4, separators=(',', ': '))
