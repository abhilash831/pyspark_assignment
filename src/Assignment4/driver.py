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
    add_year_month_day,
    write_table
)

# -------------------------------------------------------
# Create Spark Session
# -------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("Assignment4")
    .getOrCreate()
)

# -------------------------------------------------------
# Read JSON File Dynamically
# -------------------------------------------------------
json_path = "/Volumes/pyspark_catalog/assignment_schema/q4_assignment_file"

employee_df = read_json(
    spark,
    json_path
)

print("Original Data")
employee_df.show(truncate=False)

employee_df.printSchema()

# -------------------------------------------------------
# Record Count Before Flattening
# -------------------------------------------------------
original_count = get_record_count(employee_df)

print(f"Record Count Before Flattening : {original_count}")

# -------------------------------------------------------
# Flatten DataFrame
# -------------------------------------------------------
flattened_df = flatten_dataframe(employee_df)

print("Flattened Data")

flattened_df.show(truncate=False)

flattened_df.printSchema()

# -------------------------------------------------------
# Record Count After Flattening
# -------------------------------------------------------
flatten_count = get_record_count(flattened_df)

print(f"Record Count After Flattening : {flatten_count}")

print(
    "Difference : explode() creates one row for each employee "
    "present inside the employees array."
)

# -------------------------------------------------------
# explode()
# -------------------------------------------------------
print("Using explode()")

explode_df = explode_dataframe(employee_df)

explode_df.show(truncate=False)

# -------------------------------------------------------
# explode_outer()
# -------------------------------------------------------
print("Using explode_outer()")

explode_outer_df = explode_outer_dataframe(employee_df)

explode_outer_df.show(truncate=False)

# -------------------------------------------------------
# posexplode()
# -------------------------------------------------------
print("Using posexplode()")

posexplode_df = posexplode_dataframe(employee_df)

posexplode_df.show(truncate=False)

# -------------------------------------------------------
# Filter ID
# -------------------------------------------------------
filtered_df = filter_by_id(
    flattened_df,
    1001
)

print("Filtered Data")

filtered_df.show(truncate=False)

# -------------------------------------------------------
# Convert Camel Case to Snake Case
# -------------------------------------------------------
snake_df = camel_to_snake(filtered_df)

print("Snake Case Columns")

print(snake_df.columns)

# -------------------------------------------------------
# Add load_date
# -------------------------------------------------------
load_df = add_load_date(snake_df)

print("Added load_date")

load_df.show(truncate=False)

# -------------------------------------------------------
# Create year, month and day columns
# -------------------------------------------------------
final_df = add_year_month_day(load_df)

print("Final DataFrame")

final_df.show(truncate=False)

final_df.printSchema()

# -------------------------------------------------------
# Write DataFrame as Managed Table
# Format : JSON
# Partition : year, month, day
# ReplaceWhere : year, month, day
# -------------------------------------------------------
write_table(final_df)

print("Table pyspark_catalog.assignment_schema.employee_details created successfully.")