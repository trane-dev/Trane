# import csv
# import os

# import pandas as pd

# from trane.utils.data_parser import denormalize

# group_csv_contents = [
#     ('employee', 'group'),
#     ('Bob', 'Accounting'),
#     ('Jake', 'Engineering'),
#     ('Lisa', 'Engineering'),
#     ('Sue', 'HR'),
# ]

# hire_date_csv_contents = [
#     ('employee', 'hire_date'),
#     ('Bob', '2004'),
#     ('Jake', '2008'),
#     ('Lisa', '2012'),
#     ('Sue', '2014'),
# ]

# skills_csv_contents = [
#     ('group', 'skills'),
#     ('Accounting', 'math'),
#     ('Accounting', 'spreadsheets'),
#     ('Engineering', 'coding'),
#     ('Engineering', 'linux'),
#     ('HR', 'spreadsheets'),
#     ('HR', 'organization'),
# ]

# supervisor_csv_contents = [
#     ('group', 'supervisor'),
#     ('Accounting', 'Carly'),
#     ('Engineering', 'Guido'),
#     ('HR', 'Steve'),
# ]

# csv_lists_to_write = [
#     (group_csv_contents, 'group.csv'),
#     (hire_date_csv_contents, 'hire_date.csv'),
#     (skills_csv_contents, 'skills.csv'),
#     (supervisor_csv_contents, 'supervisor.csv'),
# ]

# for csv_list_to_write, filename in csv_lists_to_write:
#     with open(filename, 'wt') as myfile:
#         csv_out = csv.writer(myfile)

#         for row in csv_list_to_write:
#             csv_out.writerow(row)


# def test_denormalize():

#     relationships = [
#         ('group.csv', 'employee', 'hire_date.csv', 'employee'),
#         ('supervisor.csv', 'group', 'group.csv', 'group'),
#         ('group.csv', 'group', 'skills.csv', 'group'),
#     ]
#     res = denormalize(relationships)

#     expected = pd.DataFrame(
#         [
#             ('Accounting', 'Carly', 'Bob', 2004, 'math'),
#             ('Accounting', 'Carly', 'Bob', 2004, 'spreadsheets'),
#             ('Engineering', 'Guido', 'Jake', 2008, 'coding'),
#             ('Engineering', 'Guido', 'Jake', 2008, 'linux'),
#             ('Engineering', 'Guido', 'Lisa', 2012, 'coding'),
#             ('Engineering', 'Guido', 'Lisa', 2012, 'linux'),
#             ('HR', 'Steve', 'Sue', 2014, 'spreadsheets'),
#             ('HR', 'Steve', 'Sue', 2014, 'organization'),
#         ],
#         columns=['group', 'supervisor', 'employee', 'hire_date', 'skills'],
#     )

#     assert (res.equals(expected))

#     for _, filename in csv_lists_to_write:
#         os.remove(filename)
