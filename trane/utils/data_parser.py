import pandas as pd


class CsvMerge:
    """
    Simple class for helping with csv merge operations.
    """

    def __init__(self, csv_names, data):
        self.csv_names = csv_names
        self.data = data

    def contains_relevant_csv_name(self, name_to_check):
        return name_to_check in self.csv_names

    def get_data(self):
        return self.data

    def get_csv_names(self):
        return self.csv_names

    def merge_objects(self, other, left_key, right_key):
        csv_names = self.get_csv_names() + other.get_csv_names()
        data = pd.merge(
            self.get_data(),
            other.get_data(),
            left_on=left_key,
            right_on=right_key,
        )
        return CsvMerge(csv_names, data)


def denormalize(relationships):
    """
    Convert csv's to a dataframe according to relationships. Note, the function
        assumes that the csv's have header columns.

    Parameters
    ----------
    relationships: a list containing all the relationships among the csv's.
        the relationships are defined in a list as follows:
        here is how a one to many, one to one or many to many relationship is defined.
        [(parent_csv, parent_join_key, child_csv, child_join_key)].

    Returns
    ----------
    dataframe: a merge of all the csv's according to the relationships defined in relationships.
    """

    csv_merge_objs = []
    for relationship in relationships:
        parent_csv, parent_key, child_csv, child_key = relationship

        parent_merge_obj = CsvMerge([parent_csv], pd.read_csv(parent_csv))
        child_merge_obj = CsvMerge([child_csv], pd.read_csv(child_csv))

        parent_from_list = False
        child_from_list = False
        for csv_merge_obj in csv_merge_objs:
            if csv_merge_obj.contains_relevant_csv_name(parent_csv):
                parent_merge_obj = csv_merge_obj
                parent_from_list = True

            if csv_merge_obj.contains_relevant_csv_name(child_csv):
                child_merge_obj = csv_merge_obj
                child_from_list = True

        new_merge_obj = parent_merge_obj.merge_objects(
            child_merge_obj,
            parent_key,
            child_key,
        )

        if parent_from_list:
            csv_merge_objs.remove(parent_merge_obj)
        if child_from_list:
            csv_merge_objs.remove(child_merge_obj)

        csv_merge_objs.append(new_merge_obj)

    assert len(csv_merge_objs) == 1

    return csv_merge_objs[0].get_data()
