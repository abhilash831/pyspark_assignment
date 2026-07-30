from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType


# -------------------------------------------------------
# Print number of partitions
# -------------------------------------------------------
def get_partition_count(df):
    return df.rdd.getNumPartitions()


# -------------------------------------------------------
# Increase partitions
# -------------------------------------------------------
def increase_partitions(df, num_partitions):
    return df.repartition(num_partitions)


# -------------------------------------------------------
# Decrease partitions
# -------------------------------------------------------
def decrease_partitions(df, num_partitions):
    return df.coalesce(num_partitions)


# -------------------------------------------------------
# UDF to mask credit card number
# Example:
# 1234567891234567
# ************4567
# -------------------------------------------------------
def mask_card(card_number):

    if card_number is None:
        return None

    return "*" * (len(card_number) - 4) + card_number[-4:]


mask_card_udf = udf(mask_card, StringType())


# -------------------------------------------------------
# Create masked DataFrame
# -------------------------------------------------------
def create_masked_dataframe(df):

    return df.withColumn(
        "masked_card_number",
        mask_card_udf(col("card_number"))
    )