from pyspark.sql.functions import (
    col,
    avg,
    lower,
    current_date
)


# -------------------------------------------------------
# Dynamically Create DataFrame
# -------------------------------------------------------
def create_dataframe(spark, data, schema):

    return spark.createDataFrame(
        data,
        schema
    )


# -------------------------------------------------------
# Find Average Salary of Each Department
# -------------------------------------------------------
def average_salary(employee_df):

    return (
        employee_df
        .groupBy("department")
        .agg(
            avg("salary").alias("average_salary")
        )
    )


# -------------------------------------------------------
# Employees whose name starts with 'm'
# -------------------------------------------------------
def employee_name_starts_with_m(employee_df, department_df):

    return (
        employee_df.join(
            department_df,
            employee_df.department == department_df.dept_id,
            "inner"
        )
        .filter(
            lower(col("employee_name")).startswith("m")
        )
        .select(
            "employee_name",
            "dept_name"
        )
    )


# -------------------------------------------------------
# Add Bonus Column
# -------------------------------------------------------
def add_bonus(employee_df):

    return employee_df.withColumn(
        "bonus",
        col("salary") * 2
    )


# -------------------------------------------------------
# Reorder Columns Dynamically
# -------------------------------------------------------
def reorder_columns(df, column_order):

    return df.select(*column_order)


# -------------------------------------------------------
# Dynamic Join Function
# -------------------------------------------------------
def join_dataframe(employee_df, department_df, join_type):

    return (
        employee_df.join(
            department_df,
            employee_df.department == department_df.dept_id,
            join_type
        )
    )


# -------------------------------------------------------
# Replace State Code with Country Name
# -------------------------------------------------------
def replace_state(employee_df, country_df):

    return (
        employee_df.join(
            country_df,
            employee_df.State == country_df.country_code,
            "left"
        )
        .drop("State")
        .drop("country_code")
        .withColumnRenamed(
            "country_name",
            "State"
        )
    )


# -------------------------------------------------------
# Convert Column Names to Lowercase
# -------------------------------------------------------
def lowercase_columns(df):

    return df.toDF(
        *[
            column.lower()
            for column in df.columns
        ]
    )


# -------------------------------------------------------
# Add Load Date
# -------------------------------------------------------
def add_load_date(df):

    return df.withColumn(
        "load_date",
        current_date()
    )


# -------------------------------------------------------
# Write External Parquet Table
# -------------------------------------------------------
def write_parquet_table(df, table_name, path):

    (
        df.write
        .mode("overwrite")
        .option("path", path)
        .format("parquet")
        .saveAsTable(table_name)
    )


# -------------------------------------------------------
# Write External CSV Table
# -------------------------------------------------------
def write_csv_table(df, table_name, path):

    (
        df.write
        .mode("overwrite")
        .option("header", True)
        .option("path", path)
        .format("csv")
        .saveAsTable(table_name)
    )