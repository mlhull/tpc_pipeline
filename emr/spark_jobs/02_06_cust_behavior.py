#import dependencies for spark job
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *
from datetime import datetime
import os

# Create Spark session
spark = SparkSession.builder.appName("q06_cust_behavior").getOrCreate()

#call in variables set in spark submit
tpc_results = os.getenv('tpc_results')
tpc_parquet = os.getenv('tpc_parquet')

# Set for partition
current_date = datetime.today()
current_year = current_date.year
current_month = current_date.month
current_day = current_date.day

partition_path = f"year={current_year}/month={current_month:02d}/day={current_day:02d}"

#create necessary df from parquet files, grabbing latest partition
orders_df = spark.read.parquet(f"{tpc_parquet}/orders/") \
        .filter(
                (col("year") == current_year) &
                (col("month") == current_month) &
                (col("day") == current_day)
        )

customer_df = spark.read.parquet(f"{tpc_parquet}/customer/") \
        .filter(
                (col("year") == current_year) &
                (col("month") == current_month) &
                (col("day") == current_day)
        )

#define a window specification to partition by customer key and year.
window_spec = Window.partitionBy("C_CUSTKEY", "YEAR")

#join the customer and orders dfs, filter for orders placed in the years 1996, 1997, and 1998,
#then select relevant fields (customer name, customer key, and order year).
#calculate total orders placed per customer per year.
by_customer_df = customer_df \
.join(orders_df, orders_df.O_CUSTKEY == customer_df.C_CUSTKEY, "inner") \
.filter((year(to_date(col("O_ORDERDATE"), 'yyyy-MM-dd')).isin([1996, 1997, 1998]))) \
.select(
        col("C_NAME").alias("CUSTOMER_NAME"),
        col("C_CUSTKEY"),
        year(to_date(col("O_ORDERDATE"), 'yyyy-MM-dd')).alias("YEAR")
) \
.withColumn("TOT_ORDERS_PLACED", count("*").over(window_spec)) \
.distinct()

#pivot the df to arrange the total orders placed in 1996, 1997, and 1998 as columns.
#rename the columns for the current year (CY) and previous year (PY) orders, then calculate the percentage change in orders between 1997 and 1998.
#manage cases where the previous year (PY) or current year (CY) has no orders.
#move data to parquet results
by_customer_df.groupBy("C_CUSTKEY", "CUSTOMER_NAME") \
.pivot("YEAR", [1996, 1997, 1998]) \
.agg(max("TOT_ORDERS_PLACED")) \
.withColumnRenamed("1998", "CY_ORDERS_PLACED") \
.withColumn("CY_ORDERS_PLACED", coalesce(col("CY_ORDERS_PLACED"), lit(0))) \
.withColumnRenamed("1997", "PY_ORDERS_PLACED") \
.withColumn("PY_ORDERS_PLACED", coalesce(col("PY_ORDERS_PLACED"), lit(0))) \
.withColumn("PCT_CHG_IN_ORDERS",
                when(col("PY_ORDERS_PLACED") == 0, 100.0)
                .when(col("CY_ORDERS_PLACED") == 0, -100.0)
                .otherwise((col("CY_ORDERS_PLACED") - col("PY_ORDERS_PLACED")) / col("PY_ORDERS_PLACED") * 100)) \
.drop("C_CUSTKEY") \
.write.mode("overwrite").parquet(f"{tpc_results}/q06_cust_behavior/{partition_path}")

spark.stop()
