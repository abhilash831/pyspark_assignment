from pyspark.sql.functions import col, countDistinct


def customers_only_iphone13(purchase_data_df):
    """
    Find customers who have bought only iphone13
    """

    result = (
        purchase_data_df
        .groupBy("customer")
        .agg(countDistinct("product_model").alias("product_count"))
        .join(
            purchase_data_df.filter(col("product_model") == "iphone13"),
            "customer"
        )
        .filter(col("product_count") == 1)
        .select("customer")
        .distinct()
    )

    return result


def customers_upgraded_to_iphone14(purchase_data_df):
    """
    Find customers who upgraded from iphone13 to iphone14
    """

    iphone13_customers = (
        purchase_data_df
        .filter(col("product_model") == "iphone13")
        .select("customer")
    )

    iphone14_customers = (
        purchase_data_df
        .filter(col("product_model") == "iphone14")
        .select("customer")
    )

    result = iphone13_customers.intersect(iphone14_customers)

    return result


def customers_bought_all_products(purchase_data_df, product_data_df):
    """
    Find customers who bought all products
    """

    total_products = product_data_df.count()

    result = (
        purchase_data_df
        .groupBy("customer")
        .agg(countDistinct("product_model").alias("products_bought"))
        .filter(col("products_bought") == total_products)
        .select("customer")
    )

    return result