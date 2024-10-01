#import dependencies for spark job
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *
from datetime import datetime
import os

# Create Spark session
spark = SparkSession.builder.appName("q03_delay_ords_pct_by_cust").getOrCreate()

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
customer_df = spark.read.parquet(f"{tpc_parquet}/customer/") \
        .filter(
                (col("year") == current_year) &
                (col("month") == current_month) &
                (col("day") == current_day)
        )

orders_df = spark.read.parquet(f"{tpc_parquet}/orders/") \
        .filter(
                (col("year") == current_year) &
                (col("month") == current_month) &
                (col("day") == current_day)
        )

lineitem_df = spark.read.parquet(f"{tpc_parquet}/lineitem/") \
        .filter(
                (col("year") == current_year) &
                (col("month") == current_month) &
                (col("day") == current_day)
        )

#identify delayed line items (note: a single order may have multiple line items with different ship dates)
delay_lineitems_df = lineitem_df \
        .withColumn("DAY_DIFF", datediff(to_date(col("L_SHIPDATE"),'yyyy-MM-dd'), to_date(col("L_COMMITDATE"), 'yyyy-MM-dd'))) \
        .filter(col("DAY_DIFF") > 0) \
        .select("L_ORDERKEY", "DAY_DIFF", lit(1).alias("DELAY_FLAG"))

#join customer and orders data to the delayed line items to associate delays with customers
delay_by_customer_df = customer_df.join(orders_df, col("C_CUSTKEY") == col("O_CUSTKEY"), "inner") \
        .join(delay_lineitems_df, col("O_ORDERKEY") == col("L_ORDERKEY"), "inner") \
        .select("C_CUSTKEY", "C_NAME", "O_ORDERKEY", "DAY_DIFF") 

#define a window spec to calculate the average delay days for each customer
window_spec = Window.partitionBy("C_CUSTKEY")

#calculate the average delay in days per customer using a window function
delay_with_avg_df = delay_by_customer_df \
        .withColumn("AVG_DELAY_DAYS", avg("DAY_DIFF").over(window_spec)) \
        .select("C_CUSTKEY", "C_NAME", "AVG_DELAY_DAYS") \
        .dropDuplicates()  #drop duplicates to get one row per customer

#calculate the total number of orders per customer.
total_orders_df = orders_df.groupBy("O_CUSTKEY") \
        .agg(countDistinct("O_ORDERKEY").alias("TOTAL_ORDERS"))

#calculate the total number of delayed orders per customer, ensuring no duplicates in order-level delays
sum_delay_order_df = delay_by_customer_df.dropDuplicates(["C_CUSTKEY", "O_ORDERKEY"]) \
        .groupBy("C_CUSTKEY") \
        .agg(count("O_ORDERKEY").alias("TOT_DELAY_ORDERS"))

#join the total delayed orders and total orders with the average delay per customer
#sort data to get highest delays and then limit to those results and export to parquet
sum_delay_order_df.join(total_orders_df, total_orders_df.O_CUSTKEY == sum_delay_order_df.C_CUSTKEY, "inner") \
        .join(delay_with_avg_df, delay_with_avg_df.C_CUSTKEY == sum_delay_order_df.C_CUSTKEY, "inner") \
        .select("C_NAME", "AVG_DELAY_DAYS",
                ((col("TOT_DELAY_ORDERS") / col("TOTAL_ORDERS"))*100).alias("PCT_DELAYED_ORDERS")) \
        .orderBy(col("PCT_DELAYED_ORDERS").desc()) \
        .limit(5) \
        .write.mode("overwrite").parquet(f"{tpc_results}/q03_delay_ords_pct_by_cust/{partition_path}")

spark.stop()
