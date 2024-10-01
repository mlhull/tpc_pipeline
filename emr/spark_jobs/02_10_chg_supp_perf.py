from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *
from datetime import datetime
import os

# Create Spark session
spark = SparkSession.builder.appName("q10_chg_supp_perf").getOrCreate()

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
supplier_df = spark.read.parquet(f"{tpc_parquet}/supplier/") \
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

#calculate total parts shipped using a window function
by_supplier_df = supplier_df \
        .join(lineitem_df, lineitem_df.L_SUPPKEY == supplier_df.S_SUPPKEY) \
        .select(
                col("S_NAME").alias("SUPPLIER_NAME"), 
                year(to_date(col("L_SHIPDATE"), 'yyyy-MM-dd')).alias("SHIPDATE_YEAR"),
                col("L_QUANTITY")
        ) 
#define a window spec to partition by supplier name and shipment year
window_spec = Window.partitionBy("SUPPLIER_NAME", "SHIPDATE_YEAR")

#calculate total shipped parts for each supplier and year
by_supplier_df = by_supplier_df \
        .withColumn("TOT_SHIPPED_PARTS", sum("L_QUANTITY").over(window_spec)) \
        .drop("L_QUANTITY") \
        .distinct() \
        .orderBy("SUPPLIER_NAME", "SHIPDATE_YEAR")

#rivot the data to calculate year-over-year shipped parts and percentage change
#handle instances where pct change is zero
result_df = by_supplier_df.groupBy("SUPPLIER_NAME") \
        .pivot("SHIPDATE_YEAR", [1997, 1998]) \
        .agg(max("TOT_SHIPPED_PARTS")) \
        .withColumnRenamed("1998", "CY_SHIPPED_PARTS") \
        .withColumn("CY_SHIPPED_PARTS", coalesce(col("CY_SHIPPED_PARTS"), lit(0))) \
        .withColumnRenamed("1997", "PY_SHIPPED_PARTS") \
        .withColumn("PY_SHIPPED_PARTS", coalesce(col("PY_SHIPPED_PARTS"), lit(0))) \
        .withColumn("PCT_CHG_IN_ORDERS",
                when(col("PY_SHIPPED_PARTS") == 0, 100.0)
                .when(col("CY_SHIPPED_PARTS") == 0, -100.0)
                .otherwise((col("CY_SHIPPED_PARTS") - col("PY_SHIPPED_PARTS")) / col("PY_SHIPPED_PARTS") * 100)
        )

#write aggregated results out to parquet formatted tables
result_df.write.mode("overwrite").parquet(f"{tpc_results}/q10_chg_supp_perf/{partition_path}")

spark.stop()
