import os

import pandas as pd

from trane.typing.column_schema import ColumnSchema
from trane.typing.logical_types import (
    Categorical,
    Datetime,
    Double,
    Integer,
)


def sales_data():
    # The organizations your business interacts with.
    accounts = pd.DataFrame(
        {
            "account_id": ["a1", "a2", "a3", "a4", "a5"],
            "account_name": [
                "Acme Corporation",
                "Stark Industries",
                "Wayne Enterprises",
                "LexCorp",
                "Umbrella Corporation",
            ],
            "account_type": [
                "Partners",
                "Customer",
                "Customer",
                "Customer",
                "Competitors",
            ],
        },
    )
    # The people your business interacts with. This can include customers, prospects, or other stakeholders.
    contacts = pd.DataFrame(
        {
            "contact_id": ["001", "002", "003", "004", "005"],
            "contact_name": [
                "Wile E. Coyote",
                "Tony Stark",
                "Bruce Wayne",
                "Lex Luthor",
                "Albert Wesker",
            ],
            "contact_title": [
                "Director of Finance",
                "CEO",
                "CEO",
                "CEO",
                "VP of Marketing",
            ],
            "contact_email": [
                "coyote@gmail.com",
                "tony@stark.com",
                "bruce@wayne.com",
                "lex@luthor.com",
                "albert@wesker.com",
            ],
            "contact_phone": [
                "(555) 234-5678",
                "(555) 345-6789",
                "(555) 456-7890",
                "(555) 567-8901",
                "(555) 678-9012",
            ],
        },
    )
    # Potential sales deals. This can include the deal value, expected close date, and other relevant sales pipeline information.
    opportunities = pd.DataFrame(
        {
            "opportunity_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "opportunity_stage": [
                "Closed Won",
                "Prospecting",
                "Prospecting",
                "Closed Won",
                "Lost",
                "Proposal",
                "Proposal",
                "Proposal",
                "Proposal",
                "Proposal",
            ],
            "account_id": ["a1", "a2", "a3", "a4", "a5", "a1", "a2", "a3", "a4", "a5"],
            "deal_value": [
                100000,
                200000,
                300000,
                400000,
                500000,
                600000,
                700000,
                800000,
                900000,
                1000000,
            ],
            "expected_close_date": [
                "2018-01-01",
                "2018-02-01",
                "2018-03-01",
                "2018-04-01",
                "2018-05-01",
                "2018-06-01",
                "2018-07-01",
                "2018-08-01",
                "2018-09-01",
                "2018-10-01",
            ],
        },
    )
    dataframes = {
        "accounts": accounts,
        "contacts": contacts,
        "opportunities": opportunities,
    }
    metadata = {
        "relationships": [
            ("accounts", "account_id", "contacts", "account_id"),
            ("accounts", "account_id", "opportunities", "account_id"),
        ],
        "accounts": {
            "account_id": ColumnSchema(
                logical_type=Categorical,
                semantic_tags={"index"},
            ),
            "account_name": ColumnSchema(
                logical_type=Categorical,
                semantic_tags={"category"},
            ),
            "account_type": ColumnSchema(
                logical_type=Categorical,
                semantic_tags={"category"},
            ),
            "account_site": ColumnSchema(
                logical_type=Categorical,
                semantic_tags={"category"},
            ),
            "account_address": ColumnSchema(
                logical_type=Categorical,
                semantic_tags={"category"},
            ),
        },
        "contacts": {
            "contact_id": ColumnSchema(
                logical_type=Categorical,
                semantic_tags={"index"},
            ),
            "contact_name": ColumnSchema(
                logical_type=Categorical,
                semantic_tags={"category"},
            ),
            "contact_title": ColumnSchema(
                logical_type=Categorical,
                semantic_tags={"category"},
            ),
            "contact_email": ColumnSchema(
                logical_type=Categorical,
                semantic_tags={"category"},
            ),
            "contact_phone": ColumnSchema(
                logical_type=Categorical,
                semantic_tags={"category"},
            ),
        },
        "opportunities": {
            "opportunity_id": ColumnSchema(
                logical_type=Integer,
                semantic_tags={"index"},
            ),
            "opportunity_stage": ColumnSchema(
                logical_type=Categorical,
                semantic_tags={"category"},
            ),
            "account_id": ColumnSchema(
                logical_type=Categorical,
                semantic_tags={"foreign_key"},
            ),
            "deal_value": ColumnSchema(logical_type=Double, semantic_tags={"numeric"}),
            "expected_close_date": ColumnSchema(
                logical_type=Datetime,
                semantic_tags={"time_index"},
            ),
        },
    }
    return dataframes, metadata


def generate_local_filepath(key):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(dir_path, key)
