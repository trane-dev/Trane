from functools import partial
from multiprocessing import Manager, Pool

from tqdm.contrib.concurrent import process_map
from tqdm.notebook import tqdm

# def overall_prediction_helper(df, meta):
#     df["__fake_root_entity__"] = 0
#     meta.add_column("__fake_root_entity__", TM.TYPE_IDENTIFIER)
#     return df, meta


def _solve_single_problem(problem, ns, shared_dict):
    df = ns.df
    x = problem.execute(df, -1, verbose=False)
    shared_dict[str(problem)] = x


def multiprocess_prediction_problem(problems, df, processes=8):
    mgr = Manager()
    ns = mgr.Namespace()
    ns.df = df
    shared_dict = mgr.dict()

    partial_func = partial(_solve_single_problem, ns=ns, shared_dict=shared_dict)
    process_map(partial_func, problems, max_workers=processes)
    return shared_dict._getvalue()


def _solve_evaluation(task, evaluator, features):
    return evaluator.evaluate(task, features)


def multi_process_evaluation(evaluator, problems, features, processes=8):
    p = Pool(processes)
    result = []
    for _ in tqdm.tqdm(
        p.imap(
            partial(_solve_evaluation, evaluator=evaluator, features=features),
            problems,
        ),
        total=len(problems),
    ):
        result.append(_)
    return result
