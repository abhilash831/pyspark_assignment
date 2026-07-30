#assignment3
from pyspark.sql.functions import (
    col,
    max,
    date_sub,
    count,
    to_timestamp,
    to_date,
    lit
)


# -------------------------------------------------------
# Dynamically rename DataFrame columns
# -------------------------------------------------------
def rename_columns(df, new_columns):

    for old_col, new_col in zip(df.columns, new_columns):
        df = df.withColumnRenamed(old_col, new_col)

    return df


# -------------------------------------------------------
# Convert timestamp column from StringType to TimestampType
# -------------------------------------------------------
def convert_timestamp(df):

    return df.withColumn(
        "time_stamp",
        to_timestamp(col("time_stamp"), "yyyy-MM-dd HH:mm:ss")
    )


# -------------------------------------------------------
# Find user actions performed in the last 7 days
# (Reference date = Maximum timestamp in the dataset)
# -------------------------------------------------------
def actions_last_7_days(df):

    max_date = df.select(
        max("time_stamp")
    ).first()[0]

    start_date = date_sub(
        lit(max_date),
        7
    )

    return (
        df.filter(col("time_stamp") >= start_date)
          .groupBy("user_id")
          .agg(
              count("user_activity").alias("number_of_actions")
          )
          .orderBy("user_id")
    )


# -------------------------------------------------------
# Create login_date column
# -------------------------------------------------------
def create_login_date(df):

    return df.withColumn(
        "login_date",
        to_date(col("time_stamp"))
    )


# -------------------------------------------------------
# Write DataFrame as CSV
# -------------------------------------------------------
def write_csv(df, path):

    (
        df.write
        .mode("overwrite")
        .option("header", True)
        .option("delimiter", ",")
        .option("quote", '"')
        .csv(path)
    )


# -------------------------------------------------------
# Save DataFrame as Managed Table
# -------------------------------------------------------
def write_table(df, table_name):

    (
        df.write
        .mode("overwrite")
        .saveAsTable(table_name)
    )