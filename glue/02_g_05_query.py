import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','TPC_RESULTS','TPC_PARQUET'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

tpc_parquet = args['TPC_PARQUET']
tpc_results = args['TPC_RESULTS']

from pyspark.sql import Window
from pyspark.sql.functions import *
from datetime import datetime

# Set for partition
current_date = datetime.today()
current_year = current_date.year
current_month = current_date.month
current_day = current_date.day

partition_path = f"year={current_year}/month={current_month:02d}/day={current_day:02d}"

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

window_spec = Window.partitionBy("CATEGORY")

#calculate columns and then use a window function to report aggregated metrics by category
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