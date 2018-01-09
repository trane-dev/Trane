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

    jsonstr = open(parser.get('ALL', 'PROBLEM_OUTPUT')).read()
    label_gen = trane.LabelGenerator.from_json(jsonstr)
    dataframe = pd.read_csv(parser.get('ALL', 'DATA'))
    cutoff = parser.get('ALL', 'CUTOFF')
    try:
        cutoff_t = int(cutoff)
        cutoff = cutoff_t
    except:
        pass
    label_gen.set_cutoff_time(cutoff)
    results = label_gen.execute(dataframe)
    for prob, label in results:
        print(str(prob))
        print(prob.generate_nl_description())
        print(label)
        print()
