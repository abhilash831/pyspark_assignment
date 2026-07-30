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
    create_masked_dataframe
)

spark = SparkSession.builder.appName("Assignment2").getOrCreate()

# -------------------------------------------------------
# Method 1 : Create DataFrame using list
# -------------------------------------------------------

data = [
    ("1234567891234567",),
    ("5678912345671234",),
    ("9123456712345678",),
    ("1234567812341122",),
    ("1234567812341342",)
]

schema = StructType([
    StructField("card_number", StringType(), False)
])

credit_card_df = spark.createDataFrame(data, schema)

print("Original Data")
credit_card_df.show()


# -------------------------------------------------------
# Number of partitions
# -------------------------------------------------------

original_partition = get_partition_count(credit_card_df)

print(f"Original Partitions : {original_partition}")


# -------------------------------------------------------
# Increase partitions
# -------------------------------------------------------

credit_card_df = increase_partitions(
    credit_card_df,
    5
)

print("Partitions after repartition :",
      get_partition_count(credit_card_df))


# -------------------------------------------------------
# Decrease partitions
# -------------------------------------------------------

credit_card_df = decrease_partitions(
    credit_card_df,
    original_partition
)

print("Partitions after coalesce :",
      get_partition_count(credit_card_df))


# -------------------------------------------------------
# Mask Card Numbers
# -------------------------------------------------------

masked_df = create_masked_dataframe(credit_card_df)

masked_df.show(truncate=False)