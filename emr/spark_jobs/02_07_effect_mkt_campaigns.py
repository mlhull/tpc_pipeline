#import dependencies for spark job
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *
from datetime import datetime
import os

# Create Spark session
spark = SparkSession.builder.appName("q07_effect_mkt_campaigns").getOrCreate()

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
lineitem_df = spark.read.parquet(f"{tpc_parquet}/lineitem/") \
        .filter(
                (col("year") == current_year) &
                (col("month") == current_month) &
                (col("day") == current_day)
        )

partsupp_df = spark.read.parquet(f"{tpc_parquet}/partsupp/") \
        .filter(
                (col("year") == current_year) &
                (col("month") == current_month) &
                (col("day") == current_day)
        )

part_df = spark.read.parquet(f"{tpc_parquet}/part/") \
        .filter(
                (col("year") == current_year) &
                (col("month") == current_month) &
                (col("day") == current_day)
        )

#since there is no existing field to indicate customers targeted by marketing campaigns,
#we are assigning a random flag to simulate campaign targeting.
c_customer_df = customer_df.withColumn("MKT_CAMPAIGN",when(expr("rand() < 0.5"), 1).otherwise(0))

#define window specification partitioned by product and marketing campaign.
window_spec = Window.partitionBy("PRODUCT", "MKT_CAMPAIGN")

#join the relevant dfs to gather product, revenue, and marketing campaign information.
#calculate the total revenue for each product, segmented by marketing campaign flag.
revenue_df = part_df.join(partsupp_df, part_df.P_PARTKEY == partsupp_df.PS_PARTKEY, "inner") \
.join(lineitem_df, (partsupp_df.PS_PARTKEY == lineitem_df.L_PARTKEY) & (partsupp_df.PS_SUPPKEY == lineitem_df.L_SUPPKEY), "inner") \
.join(orders_df, orders_df.O_ORDERKEY == lineitem_df.L_ORDERKEY) \
.join(c_customer_df, c_customer_df.C_CUSTKEY == orders_df.O_CUSTKEY) \
.select(
        col("P_NAME").alias("PRODUCT"), 
        col("L_EXTENDEDPRICE").alias("REVENUE"),
        col("MKT_CAMPAIGN")
) \
.withColumn("TOTAL_REVENUE", sum("REVENUE").over(window_spec)) \
.drop("REVENUE") \
.distinct()

#rank total revenue by mkt campaign. grab highest 5 by mkt campaign
window_spec = Window.partitionBy("MKT_CAMPAIGN").orderBy(col("TOTAL_REVENUE").desc())

#filter the top 5 products by total revenue within each marketing campaign and write the result to Parquet.
revenue_df.withColumn("Rank", rank().over(window_spec)) \
.filter(col("Rank") <= 5) \
.orderBy("MKT_CAMPAIGN", "Rank") \
.drop("Rank") \
.write.mode("overwrite").parquet(f"{tpc_results}/q07_effect_mkt_campaigns/{partition_path}")

spark.stop()
