#import dependencies
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *
from datetime import datetime
import os

#creation spark session
spark = SparkSession.builder.appName("q09_sale_lead_time").getOrCreate()

#call in variables set in spark submit
tpc_results = os.getenv('tpc_results')
tpc_parquet = os.getenv('tpc_parquet')

#set for partition
current_date = datetime.today()
year = current_date.year
month = current_date.month
day = current_date.day

partition_path = f"year={year}/month={month:02d}/day={day:02d}" 

#create necessary df from parquet files, grabbing latest partition
orders_df_path = spark.read.parquet(f"{tpc_parquet}/orders/")
orders_df = orders_df_path.filter(
        (col("year") == year) &
        (col("month") == month) &
        (col("day") ==  day)
)

supplier_df_path = spark.read.parquet(f"{tpc_parquet}/supplier/")
supplier_df = supplier_df_path.filter(
        (col("year") == year) &
        (col("month") == month) &
        (col("day") ==  day)
)

lineitem_df_path = spark.read.parquet(f"{tpc_parquet}/lineitem/")
lineitem_df = lineitem_df_path.filter(
        (col("year") == year) &
        (col("month") == month) &
        (col("day") ==  day)
)

#define window specification to calculate total revenue and average lead time for each supplier
window_spec = Window.partitionBy("SUPPLIER")

#join supplier, lineitem and orders df by keys
#calculate revenue per supplier and lead time (ship date minus order date), ensuring lead time is positive
#remove raw revenue and lead time columns, deduplicate to avoid duplicate records, and export the final dataset to Parquet
supplier_df \
        .join(lineitem_df, supplier_df.S_SUPPKEY == lineitem_df.L_SUPPKEY, "inner") \
        .join(orders_df, orders_df.O_ORDERKEY == lineitem_df.L_ORDERKEY, "inner") \
        .select(
                col("S_SUPPKEY"),
                col("S_NAME").alias("SUPPLIER"),
                col("L_EXTENDEDPRICE").alias("REVENUE"),
                datediff(
                        to_date(col("L_SHIPDATE"), 'yyyy-MM-dd'),
                        to_date(col("O_ORDERDATE"), 'yyyy-MM-dd')
                        ).alias("LEAD_TIME")
                ) \
        .withColumn("TOTAL_REVENUE", sum("REVENUE").over(window_spec)) \
        .withColumn("AVG_LEAD_TIME", avg("LEAD_TIME").over(window_spec)) \
        .drop("REVENUE", "LEAD_TIME") \
        .distinct() \
        .write.mode("overwrite").parquet(f"{tpc_results}/q09_sale_lead_time/{partition_path}")

spark.stop()
