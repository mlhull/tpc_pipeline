#import dependencies for spark job
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *
from datetime import datetime
import os

# Create Spark session
spark = SparkSession.builder.appName("q02_sales_per_by_region").getOrCreate()

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
nation_df = spark.read.parquet(f"{tpc_parquet}/nation/") \
        .filter(
                (col("year") == current_year) &
                (col("month") == current_month) &
                (col("day") == current_day)
        )

region_df = spark.read.parquet(f"{tpc_parquet}/region/") \
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

#join region, nation, customer, orders, and lineitem df to calculate monthly revenue per region
#use broadcast for small table for efficiency
#calculate revenue, then on top of calculate monthly revenue
monthly_revenue_df = region_df.join(broadcast(nation_df), region_df.R_REGIONKEY == nation_df.N_REGIONKEY, "inner") \
        .join(customer_df, nation_df.N_NATIONKEY == customer_df.C_NATIONKEY, "right") \
        .join(orders_df, customer_df.C_CUSTKEY == orders_df.O_CUSTKEY, "inner") \
        .join(lineitem_df, orders_df.O_ORDERKEY == lineitem_df.L_ORDERKEY, "inner") \
        .select(
                col("R_NAME").alias("REGION"), 
                date_format((to_date(col("O_ORDERDATE"), 'yyyy-MM-dd')), 'yyyy-MM').alias("MONTH_YEAR"),
                year(to_date(col("O_ORDERDATE"), 'yyyy-MM-dd')).alias("YEAR"),
                (col("L_EXTENDEDPRICE")).alias("REVENUE")
        ) \
        .groupBy("REGION", "MONTH_YEAR", "YEAR") \
        .agg(sum("REVENUE").alias("MONTHLY_REVENUE"))
        
#calculate annual revenue per region and year-over-year growth
annual_revenue_df = monthly_revenue_df.groupBy("REGION", "YEAR") \
        .agg(sum("MONTHLY_REVENUE").alias("ANNUAL_REVENUE")) \
        .withColumn("PREVIOUS_YEAR_REVENUE", lag("ANNUAL_REVENUE").over(Window.partitionBy("REGION").orderBy("YEAR"))) \
        .withColumn("YEAR_OVER_YEAR_GROWTH_PERCENT", (col("ANNUAL_REVENUE") - col("PREVIOUS_YEAR_REVENUE")) / col("PREVIOUS_YEAR_REVENUE") * 100)

#join annual revenue data with monthly revenue data, then write aggregated results out to parquet formatted tables
annual_revenue_df.join(monthly_revenue_df, ["REGION", "YEAR"], "inner") \
        .select("REGION", "YEAR", "MONTH_YEAR", "MONTHLY_REVENUE", "ANNUAL_REVENUE", "YEAR_OVER_YEAR_GROWTH_PERCENT") \
        .write.mode("overwrite").parquet(f"{tpc_results}/q02_sales_per_by_region/{partition_path}")

spark.stop()
