from multiprocessing import Pool
from copy import deepcopy
from functools import partial
import tqdm

__all__ = ['multi_process_evaluation']

def solve(task, evaluator, features):
    return evaluator.evaluate(task, features)

def multi_process_evaluation(evaluator, problems, features, processes=8):

    p = Pool(processes)
    result = []
    for _ in tqdm.tqdm(p.imap(partial(solve, evaluator=evaluator, features=features), problems), total=len(problems)):
        result.append(_)
    return result
