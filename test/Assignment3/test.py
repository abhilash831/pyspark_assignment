#assignment3
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)

from src.Assignment3.util import (
    rename_columns,
    convert_timestamp,
    actions_last_7_days,
    create_login_date
)


# -------------------------------------------------------
# Create Spark Session
# -------------------------------------------------------
@pytest.fixture(scope="session")
def spark():

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("Assignment3Test")
        .getOrCreate()
    )

    yield spark

    spark.stop()


# -------------------------------------------------------
# Create Sample DataFrame
# -------------------------------------------------------
@pytest.fixture
def log_df(spark):

    schema = StructType([
        StructField("log id", IntegerType(), False),
        StructField("user$id", IntegerType(), False),
        StructField("action", StringType(), False),
        StructField("timestamp", StringType(), False)
    ])

    data = [
        (1, 101, "login", "2023-09-05 08:30:00"),
        (2, 102, "click", "2023-09-06 12:45:00"),
        (3, 101, "click", "2023-09-07 14:15:00"),
        (4, 103, "login", "2023-09-08 09:00:00"),
        (5, 102, "logout", "2023-09-09 17:30:00"),
        (6, 101, "click", "2023-09-10 11:20:00"),
        (7, 103, "click", "2023-09-11 10:15:00"),
        (8, 102, "click", "2023-09-12 13:10:00")
    ]

    return spark.createDataFrame(data, schema)


# -------------------------------------------------------
# Test Dynamic Column Rename
# -------------------------------------------------------
def test_rename_columns(log_df):

    new_columns = [
        "log_id",
        "user_id",
        "user_activity",
        "time_stamp"
    ]

    result = rename_columns(log_df, new_columns)

    assert result.columns == new_columns


# -------------------------------------------------------
# Test Timestamp Conversion
# -------------------------------------------------------
def test_convert_timestamp(log_df):

    new_columns = [
        "log_id",
        "user_id",
        "user_activity",
        "time_stamp"
    ]

    df = rename_columns(log_df, new_columns)

    df = convert_timestamp(df)

    assert dict(df.dtypes)["time_stamp"] == "timestamp"


# -------------------------------------------------------
# Test Actions in Last 7 Days
# -------------------------------------------------------
def test_actions_last_7_days(log_df):

    new_columns = [
        "log_id",
        "user_id",
        "user_activity",
        "time_stamp"
    ]

    df = rename_columns(log_df, new_columns)

    df = convert_timestamp(df)

    result = actions_last_7_days(df)

    actual = {
        row["user_id"]: row["number_of_actions"]
        for row in result.collect()
    }

    expected = {
        101: 3,
        102: 3,
        103: 2
    }

    assert actual == expected


# -------------------------------------------------------
# Test login_date Creation
# -------------------------------------------------------
def test_create_login_date(log_df):

    new_columns = [
        "log_id",
        "user_id",
        "user_activity",
        "time_stamp"
    ]

    df = rename_columns(log_df, new_columns)

    df = convert_timestamp(df)

    df = create_login_date(df)

    assert "login_date" in df.columns

    assert dict(df.dtypes)["login_date"] == "date"