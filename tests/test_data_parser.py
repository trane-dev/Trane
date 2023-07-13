import pandas as pd

from trane.utils.data_parser import denormalize


def test_denormalize_simple():
    users_df = pd.DataFrame(
        {
            "user_id": [1, 2, 3],
            "name": ["Charlie", "Dennis", "Mac"],
        },
    )
    orders_df = pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4, 5, 6],
            "user_id": [1, 1, 1, 2, 2, 3],
        },
    )
    denormalized = denormalize(
        dataframes={
            "users": users_df,
            "orders": orders_df,
        },
        relationships=[
            # one to many relationship
            ("users", "user_id", "orders", "user_id"),
        ],
    )
    expected_df = pd.DataFrame(
        {
            "user_id": [1, 1, 1, 2, 2, 3],
            "name": ["Charlie", "Charlie", "Charlie", "Dennis", "Dennis", "Mac"],
            "order_id": [1, 2, 3, 4, 5, 6],
        },
    )
    assert denormalized.equals(expected_df)


# def test_denormalize_complex():
#     group_df = pd.DataFrame({
#         "employee": ["Bob", "Jake", "Lisa", "Sue"],
#         "group": ["Accounting", "Engineering", "Engineering", "HR"],
#     })
#     hire_date_df = pd.DataFrame({
#         "employee": ["Bob", "Jake", "Lisa", "Sue"],
#         "hire_date": [2004, 2008, 2012, 2014],
#     })
#     skills_df = pd.DataFrame({
#         "group": ["Accounting", "Accounting", "Engineering", "Engineering", "HR", "HR"],
#         "skills": ["math", "spreadsheet", "coding", "linux", "spreadsheet", "organization"],
#     })
#     supervisor_df = pd.DataFrame({
#         "group": ["Accounting", "Engineering", "HR"],
#         "supervisor": ["Carly", "Guido", "Steve"],
#     })
#     denormalized = denormalize(
#         dataframes={
#             "group": group_df,
#             "hire_date": hire_date_df,
#             "skills": skills_df,
#             "supervisor": supervisor_df,
#         },
#         relationships=[
#             # one to one relationship
#             ("group", "employee", "hire_date", "employee"),
#             # one to many relationship
#             ("supervisor", "group", "group", "group"),
#             # many to many relationship
#             ("group", "group", "skills", "group"),
#         ]
#     )
#     expected = pd.DataFrame(
#         [
#             ("Accounting", "Carly", "Bob", 2004, "math"),
#             ("Accounting", "Carly", "Bob", 2004, "spreadsheets"),
#             ("Engineering", "Guido", "Jake", 2008, "coding"),
#             ("Engineering", "Guido", "Jake", 2008, "linux"),
#             ("Engineering", "Guido", "Lisa", 2012, "coding"),
#             ("Engineering", "Guido", "Lisa", 2012, "linux"),
#             ("HR", "Steve", "Sue", 2014, "spreadsheets"),
#             ("HR", "Steve", "Sue", 2014, "organization"),
#         ],
#         columns=["group", "supervisor", "employee", "hire_date", "skills"],
#     )
#     assert denormalized.equals(expected)


# def test_denormalize():
#     relationships = [
#         ("group.csv", "employee", "hire_date.csv", "employee"),
#         ("supervisor.csv", "group", "group.csv", "group"),
#         ("group.csv", "group", "skills.csv", "group"),
#     ]
#     res = denormalize(relationships)
#     expected = pd.DataFrame(
#         [
#             ("Accounting", "Carly", "Bob", 2004, "math"),
#             ("Accounting", "Carly", "Bob", 2004, "spreadsheets"),
#             ("Engineering", "Guido", "Jake", 2008, "coding"),
#             ("Engineering", "Guido", "Jake", 2008, "linux"),
#             ("Engineering", "Guido", "Lisa", 2012, "coding"),
#             ("Engineering", "Guido", "Lisa", 2012, "linux"),
#             ("HR", "Steve", "Sue", 2014, "spreadsheets"),
#             ("HR", "Steve", "Sue", 2014, "organization"),
#         ],
#         columns=["group", "supervisor", "employee", "hire_date", "skills"],
#     )
#     assert res.equals(expected)
