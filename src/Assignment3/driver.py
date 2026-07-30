#assignment3
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
    create_login_date,
    write_csv,
    write_table
)

# -------------------------------------------------------
# Create Spark Session
# -------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("Assignment3")
    .getOrCreate()
)

# -------------------------------------------------------
# Create Custom Schema
# -------------------------------------------------------
schema = StructType([
    StructField("log id", IntegerType(), False),
    StructField("user$id", IntegerType(), False),
    StructField("action", StringType(), False),
    StructField("timestamp", StringType(), False)
])

# -------------------------------------------------------
# Input Data
# -------------------------------------------------------
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

# -------------------------------------------------------
# Create DataFrame
# -------------------------------------------------------
log_df = spark.createDataFrame(data, schema)

print("Original DataFrame")
log_df.show(truncate=False)

# -------------------------------------------------------
# Rename Columns Dynamically
# -------------------------------------------------------
new_columns = [
    "log_id",
    "user_id",
    "user_activity",
    "time_stamp"
]

log_df = rename_columns(log_df, new_columns)

print("After Renaming Columns")
log_df.show()

# -------------------------------------------------------
# Convert String to TimestampType
# -------------------------------------------------------
log_df = convert_timestamp(log_df)

print("After Timestamp Conversion")
log_df.printSchema()

# -------------------------------------------------------
# Find User Actions in Last 7 Days
# -------------------------------------------------------
print("User Actions in Last 7 Days")

actions_df = actions_last_7_days(log_df)

actions_df.show()

# -------------------------------------------------------
# Create login_date Column
# -------------------------------------------------------
log_df = create_login_date(log_df)

print("Login Date Column")

log_df.select(
    "log_id",
    "time_stamp",
    "login_date"
).show()

log_df.printSchema()

# -------------------------------------------------------
# Write DataFrame as CSV
# Replace the catalog, schema and volume names with yours
# -------------------------------------------------------
write_csv(
    log_df,
    "/Volumes/pyspark_catalog/assignment_schema/assignment_volume/login_csv"
)

print("CSV Written Successfully")

# -------------------------------------------------------
# Write DataFrame as Managed Table
# Replace catalog and schema names with yours
# -------------------------------------------------------
write_table(
    log_df,
    "pyspark_catalog.assignment_schema.login_details"
)

print("Managed Table Created Successfully")