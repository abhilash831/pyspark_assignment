import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)

# Import functions from util.py
from src.Assignment1.util import (
    customers_only_iphone13,
    customers_upgraded_to_iphone14,
    customers_bought_all_products
)


# -------------------------------------------------------
# Create Spark Session (Runs once for all test cases)
# -------------------------------------------------------
@pytest.fixture(scope="session")
def spark():

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("Assignment1_Test")
        .getOrCreate()
    )

    yield spark

    spark.stop()


# -------------------------------------------------------
# Create Purchase DataFrame
# -------------------------------------------------------
@pytest.fixture
def purchase_df(spark):

    schema = StructType([
        StructField("customer", IntegerType(), False),
        StructField("product_model", StringType(), False)
    ])

    data = [
        (1, "iphone13"),
        (1, "dell i5 core"),
        (2, "iphone13"),
        (2, "dell i5 core"),
        (3, "iphone13"),
        (3, "dell i5 core"),
        (1, "dell i3 core"),
        (1, "hp i5 core"),
        (1, "iphone14"),
        (3, "iphone14"),
        (4, "iphone13")
    ]

    return spark.createDataFrame(data, schema)


# -------------------------------------------------------
# Create Product DataFrame
# -------------------------------------------------------
@pytest.fixture
def product_df(spark):

    schema = StructType([
        StructField("product_model", StringType(), False)
    ])

    data = [
        ("iphone13",),
        ("dell i5 core",),
        ("dell i3 core",),
        ("hp i5 core",),
        ("iphone14",)
    ]

    return spark.createDataFrame(data, schema)


# -------------------------------------------------------
# Test Case 1
# Customers who bought only iphone13
# Expected Output : Customer 4
# -------------------------------------------------------
def test_customers_only_iphone13(purchase_df):

    result = customers_only_iphone13(purchase_df)

    actual = sorted([row.customer for row in result.collect()])

    expected = [4]

    assert actual == expected


# -------------------------------------------------------
# Test Case 2
# Customers who upgraded from iphone13 to iphone14
# Expected Output : Customers 1 and 3
# -------------------------------------------------------
def test_customers_upgraded_to_iphone14(purchase_df):

    result = customers_upgraded_to_iphone14(purchase_df)

    actual = sorted([row.customer for row in result.collect()])

    expected = [1, 3]

    assert actual == expected


# -------------------------------------------------------
# Test Case 3
# Customers who bought all products
# Expected Output : Customer 1
# -------------------------------------------------------
def test_customers_bought_all_products(purchase_df, product_df):

    result = customers_bought_all_products(
        purchase_df,
        product_df
    )

    actual = sorted([row.customer for row in result.collect()])

    expected = [1]

    assert actual == expected