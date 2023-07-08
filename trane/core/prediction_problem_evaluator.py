import copy

import numpy as np
from sklearn.ensemble import AdaBoostClassifier, AdaBoostRegressor
from sklearn.linear_model import LinearRegression
from sklearn.neighbors import KNeighborsClassifier
from sklearn.preprocessing import OneHotEncoder
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor


class PredictionProblemEvaluator(object):
    """docstring for PredictionProblemEvaluator."""

    def __init__(
        self,
        df,
        entity_col,
        cutoff_strategy,
        sample=2000,
        previous_k_as_feature=2,
        latest_k_as_test=2,
        min_train_set=100,
        min_test_set=100,
    ):
        self.df = df
        self.sampled_df = df.sample(sample)
        self.entity_col = entity_col
        self.cutoff_strategy = cutoff_strategy
        self.previous_k_as_feature = previous_k_as_feature
        self.latest_k_as_test = latest_k_as_test
        self.min_train_set = min_train_set
        self.min_test_set = min_test_set

        self.regressor = [
            {"name": "LinearRegression", "model": LinearRegression()},
            {
                "name": "DecisionTreeRegressor",
                "model": DecisionTreeRegressor(max_depth=5),
            },
            {"name": "AdaBoost", "model": AdaBoostRegressor()},
        ]

        self.classifier = [
            {"name": "KNeighborsClassifier", "model": KNeighborsClassifier(5)},
            {
                "name": "DecisionTreeClassifier",
                "model": DecisionTreeClassifier(max_depth=5),
            },
            {"name": "AdaBoost", "model": AdaBoostClassifier()},
        ]

    def _get_label_stats(self, labels, problem_type):
        labels = labels.reset_index()
        labels = labels[labels["label"].notnull()]

        if problem_type == "classification":
            majority_ratio = (
                labels["label"].groupby(labels["label"]).count() / len(labels)
            ).max()

            label_entity_count = (
                labels["label"]
                .groupby([labels[self.entity_col], labels["label"]])
                .count()
            )
            entity_majority_ratio = label_entity_count.groupby(level=0).apply(
                lambda x: (x / x.sum()).max(),
            )
            entity_majority_ratio = entity_majority_ratio.mean()

            return {
                "majority_ratio": majority_ratio,
                "entity_majority_ratio": entity_majority_ratio,
            }

        elif problem_type == "regression":
            overall_mean = labels["label"].mean()
            overall_std = labels["label"].std(ddof=0)
            entity_std = (
                labels["label"].groupby(labels[self.entity_col]).std(ddof=0).mean()
            )
            return {
                "overall_mean": overall_mean,
                "overall_std": overall_std,
                "entity_std": entity_std,
            }
        else:
            assert 0

    def _categorical_threshold(self, df_col, k=3):
        counter = {}
        for item in df_col:
            try:
                counter[item] += 1
            except BaseException:
                counter[item] = 1

        counter_tuple = list(counter.items())
        counter_tuple = sorted(counter_tuple, key=lambda x: -x[1])
        counter_tuple = counter_tuple[:3]
        return [item[0] for item in counter_tuple]

    def split_dataset(self, problem, problem_type, labels, features):
        X_train, X_test, Y_train, Y_test = [], [], [], []

        entity_names = set(labels.index.get_level_values(problem.entity_col))

        if problem_type == "classification":
            label_to_index = dict(
                [(lb, _id) for _id, lb in enumerate(set(labels["label"]))],
            )

        for entity_name in entity_names:
            sub_labels = labels.loc[entity_name].reset_index()

            assert len(sub_labels) > self.previous_k_as_feature + self.latest_k_as_test
            for i in range(len(sub_labels)):
                if i < self.previous_k_as_feature:
                    continue

                cutoff_st = sub_labels.iloc[i]["cutoff_st"]
                sample_feature = features.get_feature(entity_name, cutoff_st)

                for j in range(self.previous_k_as_feature):
                    prev_label = sub_labels.iloc[i - j - 1]["label"]
                    if problem_type == "classification":
                        sample_feature.append(label_to_index[prev_label])
                    else:
                        if prev_label is None or np.isnan(prev_label):
                            sample_feature += [False, 0]
                        else:
                            sample_feature += [True, prev_label]

                label = sub_labels.iloc[i]["label"]
                if label is None or (not isinstance(label, str) and np.isnan(label)):
                    continue
                if i >= len(sub_labels) - self.latest_k_as_test:
                    X_test.append(sample_feature)
                    Y_test.append(label)
                else:
                    X_train.append(sample_feature)
                    Y_train.append(label)

        if problem_type == "classification" and len(X_train) > 0 and len(X_test) > 0:
            enc = OneHotEncoder(
                sparse=False,
                categorical_features=[
                    -i - 1 for i in range(self.previous_k_as_feature)
                ],
            )
            enc.fit(X_train + X_test)
            X_train = enc.transform(X_train)
            X_test = enc.transform(X_test)

        return X_train, X_test, Y_train, Y_test

    def evaluate(self, problem, features, labels):
        # totally wrong, just for testing
        problem_type = "regression"
        # if problem.label_type in [TM.TYPE_INTEGER, TM.TYPE_FLOAT]:
        #     problem_type = "regression"
        # elif problem.label_type in [TM.TYPE_CATEGORY, TM.TYPE_IDENTIFIER]:
        #     problem_type = "classification"
        # else:
        #     return {"status": "fail", "description": "unknown problem type"}

        template_res = {"problem_type": problem_type, "template_nl": str(problem)}
        evaluations = []
        # for problem_final, threshold_description in self.threshold_recommend(problem):
        problem_final = problem
        problem_final.cutoff_strategy = self.cutoff_strategy
        labels = labels  # problem_final.execute(self.df)
        problem_result = {
            # "description": threshold_description,
            "problem": str(problem_final),
            "label_stats": self._get_label_stats(labels, problem_type),
        }

        X_train, X_test, Y_train, Y_test = self.split_dataset(
            problem_final,
            problem_type,
            labels,
            features,
        )

        if len(X_train) < self.min_train_set or len(X_test) < self.min_test_set:
            # TODO: Need to figure out why this is here
            continue  # noqa

        problem_result["N_train"] = len(X_train)
        problem_result["N_test"] = len(X_test)

        if problem_type == "regression":
            problem_result["R2"] = {}
            for regressor in self.regressor:
                model = copy.deepcopy(regressor["model"])
                model.fit(X_train, Y_train)
                score = model.score(X_test, Y_test)
                problem_result["R2"][regressor["name"]] = score
        elif problem_type == "classification":
            if len(set(Y_train)) == 1:
                # TODO: Need to figure out why this is here
                continue  # noqa
            problem_result["Accuracy"] = {}
            for classifier in self.classifier:
                model = copy.deepcopy(classifier["model"])
                model.fit(X_train, Y_train)
                score = model.score(X_test, Y_test)
                problem_result["Accuracy"][classifier["name"]] = score

        evaluations.append(problem_result)
        template_res["evaluations"] = evaluations
        return template_res
