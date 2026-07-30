from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from util import (
    customers_only_iphone13,
    customers_upgraded_to_iphone14,
    customers_bought_all_products
)


spark = SparkSession.builder.appName("PurchaseAssignment").getOrCreate()


# ---------------------------------------
# Purchase Data
# ---------------------------------------

purchase_schema = StructType([
    StructField("customer", IntegerType(), False),
    StructField("product_model", StringType(), False)
])


purchase_data = [
    (1, "iphone13"),
    (1, "dell i5 core"),
    (2, "iphone13"),
    (2, "dell i5 core"),
    (3, "iphone13"),
    (3, "dell i5 core"),
    (1, "dell i3 core"),
    (1, "hp i5 core"),
    (1, "iphone14"),
    (3, "iphone14"),
    (4, "iphone13")
]


purchase_data_df = spark.createDataFrame(
    purchase_data,
    schema=purchase_schema
)


# ---------------------------------------
# Product Data
# ---------------------------------------

product_schema = StructType([
    StructField("product_model", StringType(), False)
])


product_data = [
    ("iphone13",),
    ("dell i5 core",),
    ("dell i3 core",),
    ("hp i5 core",),
    ("iphone14",)
]


product_data_df = spark.createDataFrame(
    product_data,
    schema=product_schema
)


print("Purchase Data")
purchase_data_df.show()

print("Product Data")
product_data_df.show()


# ---------------------------------------
# Question 2
# ---------------------------------------

print("Customers who bought only iphone13")

customers_only_iphone13(purchase_data_df).show()


# ---------------------------------------
# Question 3
# ---------------------------------------

print("Customers upgraded from iphone13 to iphone14")

customers_upgraded_to_iphone14(
    purchase_data_df
).show()


# ---------------------------------------
# Question 4
# ---------------------------------------

print("Customers who bought all products")

customers_bought_all_products(
    purchase_data_df,
    product_data_df
).show()