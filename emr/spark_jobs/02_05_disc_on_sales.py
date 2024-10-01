#import dependencies for spark job
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *
from datetime import datetime
import os

# Create Spark session
spark = SparkSession.builder.appName("q05_disc_on_sales").getOrCreate()

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
part_df = spark.read.parquet(f"{tpc_parquet}/part/") \
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

lineitem_df = spark.read.parquet(f"{tpc_parquet}/lineitem/") \
        .filter(
                (col("year") == current_year) &
                (col("month") == current_month) &
                (col("day") == current_day)
        )

#define a window specification to partition data by product category
window_spec = Window.partitionBy("CATEGORY")

#join part, partsupp, and lineitem dfs, selecting relevant columns for revenue, discounted revenue, and discount percentage
#use window function to calculate total revenue, total discounted revenue, and average discount percentage for each category
#remove row-level columns for revenue, discount revenue, and discount percentage as we now have aggregated values
#use distinct() to distinct rows after aggregation
#write aggregated reseults out to parquet formatted tables
part_df \
.join(partsupp_df, part_df.P_PARTKEY == partsupp_df.PS_PARTKEY, "inner") \
.join(lineitem_df, (partsupp_df.PS_PARTKEY == lineitem_df.L_PARTKEY) & (partsupp_df.PS_SUPPKEY == lineitem_df.L_SUPPKEY), "inner") \
.select(
        col("P_TYPE").alias("CATEGORY"), 
        (col("L_EXTENDEDPRICE")).alias("REVENUE"),
        (col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))).alias("DISCOUNT_REVENUE"),
        col("L_DISCOUNT").alias("DISCOUNT_PCT")
) \
.withColumn("TOTAL_REVENUE", sum("REVENUE").over(window_spec)) \
.withColumn("TOTAL_DISCOUNT_REVENUE", sum("DISCOUNT_REVENUE").over(window_spec)) \
.withColumn("AVG_DISCOUNT_PCT", avg("DISCOUNT_PCT").over(window_spec)) \
.drop("REVENUE","DISCOUNT_REVENUE","DISCOUNT_PCT") \
.distinct() \
.write.mode("overwrite").parquet(f"{tpc_results}/q05_disc_on_sales/{partition_path}")

spark.stop()
