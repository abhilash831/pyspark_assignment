import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

from src.Assignment2.util import (
    get_partition_count,
    increase_partitions,
    decrease_partitions,
    mask_card,
    create_masked_dataframe
)


# -------------------------------------------------------
# Spark Session
# -------------------------------------------------------
@pytest.fixture(scope="session")
def spark():

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("Assignment2Test")
        .getOrCreate()
    )

    yield spark

    spark.stop()


# -------------------------------------------------------
# Credit Card DataFrame
# -------------------------------------------------------
@pytest.fixture
def credit_card_df(spark):

    schema = StructType([
        StructField("card_number", StringType(), False)
    ])

    data = [
        ("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)
    ]

    return spark.createDataFrame(data, schema)


# -------------------------------------------------------
# Test partition count
# -------------------------------------------------------
def test_partition_count(credit_card_df):

    partitions = get_partition_count(credit_card_df)

    assert partitions >= 1


# -------------------------------------------------------
# Test repartition
# -------------------------------------------------------
def test_repartition(credit_card_df):

    df = increase_partitions(
        credit_card_df,
        5
    )

    assert get_partition_count(df) == 5


# -------------------------------------------------------
# Test coalesce
# -------------------------------------------------------
def test_coalesce(credit_card_df):

    df = increase_partitions(
        credit_card_df,
        5
    )

    df = decrease_partitions(
        df,
        1
    )

    assert get_partition_count(df) == 1


# -------------------------------------------------------
# Test UDF
# -------------------------------------------------------
def test_mask_card():

    assert mask_card("1234567891234567") == "************4567"


# -------------------------------------------------------
# Test masked DataFrame
# -------------------------------------------------------
def test_masked_dataframe(credit_card_df):

    result = create_masked_dataframe(credit_card_df)

    actual = [
        row.masked_card_number
        for row in result.collect()
    ]

    expected = [
        "************4567",
        "************1234",
        "************5678",
        "************1122",
        "************1342"
    ]

    assert actual == expected