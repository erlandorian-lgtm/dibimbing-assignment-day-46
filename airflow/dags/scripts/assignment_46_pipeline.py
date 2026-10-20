from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    DoubleType, IntegerType)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

def main():
    spark = (
        SparkSession.builder 
        .appName("Assignment46")
        .getOrCreate()
            )
    
    data_path = "/opt/airflow/data_day_46"

    orders_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("order_status", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("internal_notes", StringType(), True)
    ])

    order_items_schema = StructType([
        StructField("order_item_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount_rate", DoubleType(), True)
    ])
    
    df_orders = spark.read.csv(
        f"{data_path}/orders.csv",
        header=True,
        schema=orders_schema
    )
                 
    df_order_items = spark.read.csv(
        f"{data_path}/order_items.csv",
        header=True,
        schema=order_items_schema
    )
                 
    df_order_summary = df_orders.join(
        df_order_items.select(
            "order_id",
            "product_category",
            "quantity", 
            "price",
            "discount_rate"),
        on="order_id",
        how="left")

    df_order_summary = (df_order_summary
                        .drop("internal_notes")
                        .fillna("Unknown"))
    
    df_order_summary = (df_order_summary
                        .withColumn("order_status", 
                                    F.initcap(F.trim("order_status")))
                        .withColumn(
                                    "payment_method", 
                                    F.initcap(F.trim("payment_method")))
                        .withColumn(
                                    "product_category", 
                                    F.initcap(F.trim("product_category")))
    )

    df_order_summary = df_order_summary.filter(F.col("order_status") == "Completed")

    df_order_summary = (
        df_order_summary
        .drop("payment_method", "product_category")
        .drop("product_category")
        .withColumn("total_price", F.round(F.col("quantity") * F.col("price"),2))
        .withColumn("total_after_discount", F.round(F.col("total_price") * (1 - F.col("discount_rate")),2))
    )

    window_cumulative= (Window.orderBy("order_date")
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow))

    cumulative_gmv = (
        df_order_summary
        .withColumn("cumulative_gmv", F.round(F.sum("total_after_discount")
                .over(window_cumulative),2)).orderBy("order_date")
    )

    output_path = "/tmp/transformed_orders.parquet"

    cumulative_gmv.write \
    .mode("overwrite") \
    .parquet(output_path)

    cumulative_gmv.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
