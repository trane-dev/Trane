# import unittest
# from datetime import datetime
# import pytest

# import numpy as np
# import pandas as pd
# from mock import patch

# from trane.core.cutoff_strategy import CutoffStrategy


# @pytest.fixture
# def df():
#     df = pd.DataFrame({
#         "user_id": [1, 1, 2, 1, 1, 1, 2],
#         "date": [datetime(2015),
#                  datetime(2015),
#                  datetime(2015),
#                  datetime(2016),
#                  datetime(2017),
#                  datetime(2017),
#                  datetime(2017)],
#         "amount": [10, 200, 50, 10, 1000, 20, 10]
#     })
#     return df

# def test_cutoff_strategy(df):
#     # entity_col / entity
#     target_dataframe_index = 'user_id'
#     cutoff = '
#     window_size =
#     cutoff_strategy = CutoffStrategy(entity,


# # class TestCutoffStrategy(unittest.TestCase):
# #     def setUp(self):

# #         # self.description = 'a description'

# #         # def generate_fn(group, entity_id):

# #         #     # cutoff is 5 days after first row's date
# #         #     date_of_first_row = group.iloc[0].dates
# #         #     cutoff = date_of_first_row + np.timedelta64(5, 'D')

# #         #     return(cutoff)
# #         # self.generate_fn = generate_fn

# #         # self.cutoff_strategy = CutoffStrategy(
# #         #     generate_fn=self.generate_fn, description=self.description)
# # 	    self.cutoff_strategy = CutoffStrategy(entity,
# #                                                        cutoff,
# #                                                        cutoff_base,
# #                                                        cutoff_end,
# #                                                        cutoff)

# #     def create_patch(self, name):
# #         # helper method for creating patches
# #         patcher = patch(name)
# #         thing = patcher.start()
# #         self.addCleanup(patcher.stop)
# #         return thing

# #     def test_generate_fn_assigned(self):
# #         self.assertEqual(self.generate_fn, self.cutoff_strategy.generate_fn)

# #     def test_description_assigned(self):
# #         self.assertEqual(self.description, self.cutoff_strategy.description)

# #     def test_cutoff_times_returned_as_expected_with_cutoff_strategy(self):
# #         # this is more of an integration test than a unittest, really

# #         data = {'entity_id': ['a', 'a', 'b'],
# #                 'dates': [
# #                     np.datetime64('1980-02-01'),
# #                     np.datetime64('1980-02-24'),
# #                     np.datetime64('1980-03-01')],
# #                 'other_col': ['d', 'e', 'f']}
# #         test_data = pd.DataFrame(data)

# #         cutoff_df = self.cutoff_strategy.generate_cutoffs(
# #             df=test_data, entity_id_col='entity_id')

# #         # test_cutoff should be a different date
# #         self.assertEqual(
# #             cutoff_df.loc['a'].cutoff,
# #             np.datetime64('1980-02-06'))

# #         self.assertEqual(
# #             cutoff_df.loc['b'].cutoff,
# #             np.datetime64('1980-03-06'))

# #     def test_cutoff_times_returned_as_expected_without_cutoff_strategy(self):
# #         # this is more of an integration test than a unittest, really
# #         # test to make sure that generate_cutoffs returns a df full of
# #         # None columns if generate_fn is not specified
# #         self.cutoff_strategy.generate_fn = None

# #         data = {'entity_id': ['a', 'a', 'b'],
# #                 'dates': [
# #                     np.datetime64('1980-02-25'),
# #                     np.datetime64('1980-02-24'),
# #                     np.datetime64('1980-02-23')],
# #                 'other_col': ['d', 'e', 'f']}
# #         test_data = pd.DataFrame(data)

# #         cutoff_df = self.cutoff_strategy.generate_cutoffs(
# #             df=test_data, entity_id_col='entity_id')

# #         # no variance in cutoff columns
# #         self.assertEqual(len(cutoff_df.cutoff.unique()), 1)

# #         # training and test cutoff should both be None
# #         self.assertEqual(cutoff_df.loc['a'].cutoff, None)

# #         self.assertEqual(cutoff_df.loc['b'].cutoff, None)
