import re

from pyspark.sql.functions import (
    col,
    explode,
    explode_outer,
    posexplode,
    current_date,
    year,
    month,
    dayofmonth
)


# -------------------------------------------------------
# Read JSON file dynamically
# -------------------------------------------------------
def read_json(spark, path):

    return (
        spark.read
        .option("multiline", "true")
        .json(path)
    )


# -------------------------------------------------------
# Flatten Nested JSON
# -------------------------------------------------------
def flatten_dataframe(df):

    return (
        df
        .select(
            col("id"),
            col("properties.name").alias("companyName"),
            col("properties.storeSize").alias("storeSize"),
            explode(col("employees")).alias("employee")
        )
        .select(
            col("id"),
            col("companyName"),
            col("storeSize"),
            col("employee.empId").alias("empId"),
            col("employee.empName").alias("empName")
        )
    )


# -------------------------------------------------------
# Record Count
# -------------------------------------------------------
def get_record_count(df):

    return df.count()


# -------------------------------------------------------
# explode()
# -------------------------------------------------------
def explode_dataframe(df):

    return (
        df.select(
            col("id"),
            explode(col("employees")).alias("employee")
        )
    )


# -------------------------------------------------------
# explode_outer()
# -------------------------------------------------------
def explode_outer_dataframe(df):

    return (
        df.select(
            col("id"),
            explode_outer(col("employees")).alias("employee")
        )
    )


# -------------------------------------------------------
# posexplode()
# -------------------------------------------------------
def posexplode_dataframe(df):

    return (
        df.select(
            col("id"),
            posexplode(col("employees"))
        )
    )


# -------------------------------------------------------
# Filter by ID
# -------------------------------------------------------
def filter_by_id(df, value):

    return df.filter(col("id") == value)


# -------------------------------------------------------
# Camel Case to Snake Case
# -------------------------------------------------------
def camel_to_snake(df):

    new_columns = []

    for column in df.columns:

        snake = re.sub(
            r'(?<!^)(?=[A-Z])',
            '_',
            column
        ).lower()

        new_columns.append(snake)

    return df.toDF(*new_columns)


# -------------------------------------------------------
# Add load_date
# -------------------------------------------------------
def add_load_date(df):

    return df.withColumn(
        "load_date",
        current_date()
    )


# -------------------------------------------------------
# Add year, month, day
# -------------------------------------------------------
def add_year_month_day(df):

    return (
        df.withColumn(
            "year",
            year(col("load_date"))
        )
        .withColumn(
            "month",
            month(col("load_date"))
        )
        .withColumn(
            "day",
            dayofmonth(col("load_date"))
        )
    )


# -------------------------------------------------------
# Write Managed Table
# -------------------------------------------------------
def write_table(df):

    (
        df.write
        .mode("overwrite")
        .partitionBy(
            "year",
            "month",
            "day"
        )
        .option(
            "replaceWhere",
            "year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL"
        )
        .saveAsTable(
            "pyspark_catalog.assignment_schema.employee_details"
        )
    )