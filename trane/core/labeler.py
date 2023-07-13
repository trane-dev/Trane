class Labeler:
    """
    Object for executing prediction problems on data in order
    to generate labels for many prediction problems.
    The execute method performs the labelling operation.
    """

    # def __init__(self, df, entity_col, cutoff_strategy, sample=2000):
    #     self.df = df
    #     self.sampled_df = df.sample(sample)
    #     self.entity_col = entity_col
    #     self.cutoff_strategy = cutoff_strategy

    # def threshold_recommend(self, problem):
    #     filter_op = problem.operations[0]
    #     if len(filter_op.REQUIRED_PARAMETERS) == 0:
    #         yield copy.deepcopy(problem), "no threshold"
    #     else:
    #         if filter_op.input_type == TM.TYPE_CATEGORY:
    #             for item in self._categorical_threshold(
    #                 self.sampled_df[filter_op.column_name],
    #             ):
    #                 problem_final = copy.deepcopy(problem)
    #                 problem_final.operations[0].set_hyper_parameter(
    #                     parameter_name="threshold",
    #                     parameter_value=item,
    #                 )
    #                 yield problem_final, "threshold: {}".format(item)
    #         elif filter_op.input_type in [TM.TYPE_FLOAT, TM.TYPE_INTEGER]:
    #             for keep_rate in [0.25, 0.5, 0.75]:
    #                 threshold = filter_op.find_threshhold_by_remaining(
    #                     fraction_of_data_target=keep_rate,
    #                     df=self.sampled_df,
    #                     col=filter_op.column_name,
    #                 )
    #                 problem_final = copy.deepcopy(problem)
    #                 problem_final.operations[0].set_hyper_parameter(
    #                     parameter_name="threshold",
    #                     parameter_value=threshold,
    #                 )
    #                 yield problem_final, "threshold: {} (keep {}%)".format(
    #                     threshold,
    #                     keep_rate * 100,
    #                 )

    # def execute(self, problem, entity_id_column):
    #     """
    #     Generate the labels.

    #     Parameters
    #     ----------
    #     cutoff_df: dataframe. Each row corresponds to an entity.
    #         entity_id (indexed) | training_cutoff | test_cutoff
    #     json_prediction_problems_filename: filename to read
    #         prediction problems from, structured in JSON.

    #     Returns
    #     -------
    #     dfs: a list of DataFrames. One dataframe for each problem.
    #         Each dataframe contains entities, cutoff times and labels.

    #     """
    #     dfs = []
    #     _ = [
    #         entity_id_column,
    #         "training_labels",
    #         "test_labels",
    #         "training_cutoff_time",
    #         "label_cutoff_time",
    #     ]
    #     for problem_final, threshold_description in self.threshold_recommend(problem):
    #         problem_final.cutoff_strategy = self.cutoff_strategy
    #         labels = problem_final.execute(self.df)

    #         dfs.append([self.df, labels])

    #     return dfs
