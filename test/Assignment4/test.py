import pytest

from pyspark.sql import SparkSession

from src.Assignment4.util import (
    read_json,
    flatten_dataframe,
    get_record_count,
    explode_dataframe,
    explode_outer_dataframe,
    posexplode_dataframe,
    filter_by_id,
    camel_to_snake,
    add_load_date,
    add_year_month_day
)


# -------------------------------------------------------
# Create Spark Session
# -------------------------------------------------------
@pytest.fixture(scope="session")
def spark():

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("Assignment4Test")
        .getOrCreate()
    )

    yield spark

    spark.stop()


# -------------------------------------------------------
# Read JSON File
# -------------------------------------------------------
@pytest.fixture
def employee_df(spark):

    path = "/Volumes/pyspark_catalog/assignment_schema/q4_assignment_file"

    return read_json(
        spark,
        path
    )


# -------------------------------------------------------
# Test JSON Read
# -------------------------------------------------------
def test_read_json(employee_df):

    assert employee_df.count() == 1


# -------------------------------------------------------
# Test Flatten DataFrame
# -------------------------------------------------------
def test_flatten_dataframe(employee_df):

    df = flatten_dataframe(employee_df)

    assert df.count() == 3

    expected_columns = [
        "id",
        "companyName",
        "storeSize",
        "empId",
        "empName"
    ]

    assert df.columns == expected_columns


# -------------------------------------------------------
# Test Record Count
# -------------------------------------------------------
def test_record_count(employee_df):

    original = get_record_count(employee_df)

    flattened = get_record_count(
        flatten_dataframe(employee_df)
    )

    assert original == 1

    assert flattened == 3


# -------------------------------------------------------
# Test explode()
# -------------------------------------------------------
def test_explode(employee_df):

    df = explode_dataframe(employee_df)

    assert df.count() == 3


# -------------------------------------------------------
# Test explode_outer()
# -------------------------------------------------------
def test_explode_outer(employee_df):

    df = explode_outer_dataframe(employee_df)

    assert df.count() == 3


# -------------------------------------------------------
# Test posexplode()
# -------------------------------------------------------
def test_posexplode(employee_df):

    df = posexplode_dataframe(employee_df)

    assert df.count() == 3


# -------------------------------------------------------
# Test Filter
# -------------------------------------------------------
def test_filter(employee_df):

    df = flatten_dataframe(employee_df)

    filtered = filter_by_id(
        df,
        1001
    )

    assert filtered.count() == 3


# -------------------------------------------------------
# Test Camel Case to Snake Case
# -------------------------------------------------------
def test_camel_to_snake(employee_df):

    df = flatten_dataframe(employee_df)

    snake_df = camel_to_snake(df)

    expected = [
        "id",
        "company_name",
        "store_size",
        "emp_id",
        "emp_name"
    ]

    assert snake_df.columns == expected


# -------------------------------------------------------
# Test load_date
# -------------------------------------------------------
def test_add_load_date(employee_df):

    df = flatten_dataframe(employee_df)

    df = camel_to_snake(df)

    df = add_load_date(df)

    assert "load_date" in df.columns


# -------------------------------------------------------
# Test Year Month Day
# -------------------------------------------------------
def test_add_year_month_day(employee_df):

    df = flatten_dataframe(employee_df)

    df = camel_to_snake(df)

    df = add_load_date(df)

    df = add_year_month_day(df)

    assert "year" in df.columns
    assert "month" in df.columns
    assert "day" in df.columns