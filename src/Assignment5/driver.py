from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)

from src.Assignment5.util import (
    create_dataframe,
    average_salary,
    employee_name_starts_with_m,
    add_bonus,
    reorder_columns,
    join_dataframe,
    replace_state,
    lowercase_columns,
    add_load_date,
    write_parquet_table,
    write_csv_table
)

# -------------------------------------------------------
# Create Spark Session
# -------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("Assignment5")
    .getOrCreate()
)

# -------------------------------------------------------
# Employee Schema
# -------------------------------------------------------
employee_schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("employee_name", StringType(), False),
    StructField("department", StringType(), False),
    StructField("State", StringType(), False),
    StructField("salary", IntegerType(), False),
    StructField("Age", IntegerType(), False)
])

# -------------------------------------------------------
# Department Schema
# -------------------------------------------------------
department_schema = StructType([
    StructField("dept_id", StringType(), False),
    StructField("dept_name", StringType(), False)
])

# -------------------------------------------------------
# Country Schema
# -------------------------------------------------------
country_schema = StructType([
    StructField("country_code", StringType(), False),
    StructField("country_name", StringType(), False)
])

# -------------------------------------------------------
# Employee Data
# -------------------------------------------------------
employee_data = [
    (11, "james", "D101", "ny", 9000, 34),
    (12, "michel", "D101", "ny", 8900, 32),
    (13, "robert", "D102", "ca", 7900, 29),
    (14, "scott", "D103", "ca", 8000, 36),
    (15, "jen", "D102", "ny", 9500, 38),
    (16, "jeff", "D103", "uk", 9100, 35),
    (17, "maria", "D101", "ny", 7900, 40)
]

# -------------------------------------------------------
# Department Data
# -------------------------------------------------------
department_data = [
    ("D101", "sales"),
    ("D102", "finance"),
    ("D103", "marketing"),
    ("D104", "hr"),
    ("D105", "support")
]

# -------------------------------------------------------
# Country Data
# -------------------------------------------------------
country_data = [
    ("ny", "newyork"),
    ("ca", "California"),
    ("uk", "Russia")
]

# -------------------------------------------------------
# Create DataFrames
# -------------------------------------------------------
employee_df = create_dataframe(
    spark,
    employee_data,
    employee_schema
)

department_df = create_dataframe(
    spark,
    department_data,
    department_schema
)

country_df = create_dataframe(
    spark,
    country_data,
    country_schema
)

print("Employee DataFrame")
employee_df.show()

print("Department DataFrame")
department_df.show()

print("Country DataFrame")
country_df.show()

# -------------------------------------------------------
# Average Salary by Department
# -------------------------------------------------------
print("Average Salary by Department")

average_salary(employee_df).show()

# -------------------------------------------------------
# Employee Names Starting With M
# -------------------------------------------------------
print("Employee Names Starting With M")

employee_name_starts_with_m(
    employee_df,
    department_df
).show()

# -------------------------------------------------------
# Bonus Column
# -------------------------------------------------------
employee_bonus_df = add_bonus(employee_df)

print("Employee Bonus")

employee_bonus_df.show()

# -------------------------------------------------------
# Reorder Columns
# -------------------------------------------------------
column_order = [
    "employee_id",
    "employee_name",
    "salary",
    "State",
    "Age",
    "department"
]

employee_reordered_df = reorder_columns(
    employee_df,
    column_order
)

print("Reordered DataFrame")

employee_reordered_df.show()

# -------------------------------------------------------
# Inner Join
# -------------------------------------------------------
print("Inner Join")

join_dataframe(
    employee_df,
    department_df,
    "inner"
).show()

# -------------------------------------------------------
# Left Join
# -------------------------------------------------------
print("Left Join")

join_dataframe(
    employee_df,
    department_df,
    "left"
).show()

# -------------------------------------------------------
# Right Join
# -------------------------------------------------------
print("Right Join")

join_dataframe(
    employee_df,
    department_df,
    "right"
).show()

# -------------------------------------------------------
# Replace State Code with Country Name
# -------------------------------------------------------
country_employee_df = replace_state(
    employee_df,
    country_df
)

print("Country Name Replaced")

country_employee_df.show()

# -------------------------------------------------------
# Convert Column Names to Lowercase
# -------------------------------------------------------
lowercase_df = lowercase_columns(
    country_employee_df
)

print("Lowercase Columns")

print(lowercase_df.columns)

# -------------------------------------------------------
# Add Load Date
# -------------------------------------------------------
final_df = add_load_date(
    lowercase_df
)

print("Final DataFrame")

final_df.show()

# -------------------------------------------------------
# Write External Parquet Table
# -------------------------------------------------------
write_parquet_table(
    final_df,
    "pyspark_catalog.assignment_schema.employee_parquet",
    "/Volumes/pyspark_catalog/assignment_schema/assignment_volume/employee_parquet"
)

print("Parquet Table Created Successfully")

# -------------------------------------------------------
# Write External CSV Table
# -------------------------------------------------------
write_csv_table(
    final_df,
    "pyspark_catalog.assignment_schema.employee_csv",
    "/Volumes/pyspark_catalog/assignment_schema/assignment_volume/employee_csv"
)

print("CSV Table Created Successfully")