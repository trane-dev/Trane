from trane.datasets.load_functions import load_airbnb_reviews, load_store
from trane.typing.ml_types import MLType


def test_load_airbnb_reviews():
    df = load_airbnb_reviews()

    assert df["date"].dtype == "datetime64[ns]"
    assert df["listing_id"].dtype == "int64[pyarrow]"
    assert df["id"].dtype == "int64[pyarrow]"
    assert df["rating"].dtype == "int64[pyarrow]"


def test_load_store():
    dataframes, relationships = load_store()

    assert list(dataframes.keys()) == [
        "categories",
        "cust_hist",
        "customers",
        "inventory",
        "orderlines",
        "orders",
        "products",
        "reorder",
    ]
    assert relationships == [
        ("customers", "customerid", "cust_hist", "customerid"),
        ("products", "prod_id", "cust_hist", "prod_id"),
        ("products", "prod_id", "inventory", "prod_id"),
        ("products", "prod_id", "orderlines", "prod_id"),
        ("orders", "orderid", "orderlines", "orderid"),
        ("customers", "customerid", "orders", "customerid"),
        ("categories", "category", "products", "category"),
    ]

    dataframe_lengths = []
    for key in dataframes.keys():
        dataframe_lengths.append(len(dataframes[key][0]))
    assert dataframe_lengths == [16, 181050, 20000, 10000, 60350, 12000, 10000, 0]

    assert dataframes["categories"][0]["category"].dtype == "int64[pyarrow]"
    assert dataframes["categories"][0]["categoryname"].dtype == "string[pyarrow]"
    assert dataframes["cust_hist"][0]["customerid"].dtype == "int64[pyarrow]"
    assert dataframes["cust_hist"][0]["orderid"].dtype == "int64[pyarrow]"
    assert dataframes["cust_hist"][0]["prod_id"].dtype == "int64[pyarrow]"
    assert dataframes["customers"][0]["customerid"].dtype == "int64[pyarrow]"
    assert dataframes["customers"][0]["firstname"].dtype == "string[pyarrow]"
    assert dataframes["customers"][0]["lastname"].dtype == "string[pyarrow]"
    assert dataframes["customers"][0]["address1"].dtype == "string[pyarrow]"
    assert dataframes["customers"][0]["address2"].dtype == "null[pyarrow]"
    assert dataframes["customers"][0]["city"].dtype == "string[pyarrow]"
    assert dataframes["customers"][0]["state"].dtype == "string[pyarrow]"
    assert dataframes["customers"][0]["zip"].dtype == "int64[pyarrow]"
    assert dataframes["customers"][0]["country"].dtype == "string[pyarrow]"
    assert dataframes["customers"][0]["region"].dtype == "int64[pyarrow]"
    assert dataframes["customers"][0]["email"].dtype == "string[pyarrow]"
    assert dataframes["customers"][0]["phone"].dtype == "int64[pyarrow]"
    assert dataframes["customers"][0]["creditcardtype"].dtype == "int64[pyarrow]"
    assert dataframes["customers"][0]["creditcard"].dtype == "int64[pyarrow]"
    assert dataframes["customers"][0]["creditcardexpiration"].dtype == "string[pyarrow]"
    assert dataframes["customers"][0]["username"].dtype == "string[pyarrow]"
    assert dataframes["customers"][0]["password"].dtype == "string[pyarrow]"
    assert dataframes["customers"][0]["age"].dtype == "int64[pyarrow]"
    assert dataframes["customers"][0]["income"].dtype == "int64[pyarrow]"
    assert dataframes["customers"][0]["gender"].dtype == "string[pyarrow]"
    assert dataframes["inventory"][0]["prod_id"].dtype == "int64[pyarrow]"
    assert dataframes["inventory"][0]["quan_in_stock"].dtype == "int64[pyarrow]"
    assert dataframes["inventory"][0]["sales"].dtype == "int64[pyarrow]"
    assert dataframes["orderlines"][0]["orderlineid"].dtype == "int64[pyarrow]"
    assert dataframes["orderlines"][0]["orderid"].dtype == "int64[pyarrow]"
    assert dataframes["orderlines"][0]["prod_id"].dtype == "int64[pyarrow]"
    assert dataframes["orderlines"][0]["quantity"].dtype == "int64[pyarrow]"
    assert dataframes["orderlines"][0]["orderdate"].dtype == "string[pyarrow]"
    assert dataframes["orders"][0]["orderid"].dtype == "int64[pyarrow]"
    assert dataframes["orders"][0]["orderdate"].dtype == "string[pyarrow]"
    assert dataframes["orders"][0]["customerid"].dtype == "int64[pyarrow]"
    assert dataframes["orders"][0]["netamount"].dtype == "double[pyarrow]"
    assert dataframes["orders"][0]["tax"].dtype == "double[pyarrow]"
    assert dataframes["orders"][0]["totalamount"].dtype == "double[pyarrow]"
    assert dataframes["products"][0]["prod_id"].dtype == "int64[pyarrow]"
    assert dataframes["products"][0]["category"].dtype == "int64[pyarrow]"
    assert dataframes["products"][0]["title"].dtype == "string[pyarrow]"
    assert dataframes["products"][0]["actor"].dtype == "string[pyarrow]"
    assert dataframes["products"][0]["price"].dtype == "double[pyarrow]"
    assert dataframes["products"][0]["special"].dtype == "int64[pyarrow]"
    assert dataframes["products"][0]["common_prod_id"].dtype == "int64[pyarrow]"


def check_column_schema(columns, df, metadata):
    for col in columns:
        assert col in df.columns
        assert col in metadata.keys()
        assert issubclass(metadata[col], MLType)
