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

agg_df = part_df.join(partsupp_df, part_df.P_PARTKEY == partsupp_df.PS_PARTKEY, "inner") \
.join(lineitem_df, (partsupp_df.PS_PARTKEY == lineitem_df.L_PARTKEY) & (partsupp_df.PS_SUPPKEY == lineitem_df.L_SUPPKEY), "inner") \
.select(
        "P_NAME", 
        "P_MFGR", 
        "PS_SUPPLYCOST",
        (col("L_EXTENDEDPRICE")).alias("REVENUE"),
        (col("L_EXTENDEDPRICE") - (col("PS_SUPPLYCOST"))).alias("PROFIT")
) \
.repartition(16, "P_NAME", "P_MFGR") \
.groupBy("P_NAME", "P_MFGR") \
.agg(
        sum("REVENUE").alias("TOTAL_REVENUE_GENERATED"),
        sum("PS_SUPPLYCOST").alias("TOTAL_COST"),
        sum("PROFIT").alias("TOTAL_PROFIT")
) \
.withColumn("PROFIT_MARGIN_PCT", (col("TOTAL_PROFIT") / col("TOTAL_REVENUE_GENERATED")) * 100) 

window_spec = Window.orderBy(col("TOTAL_PROFIT").desc())

agg_df.withColumn("Rank", rank().over(window_spec)) \
.filter(col("Rank") <= 10) \
.orderBy("Rank")\
.drop("Rank") \
.limit(10) \
.write.mode("overwrite").parquet(f"{tpc_results}/q01_most_profit_prods/{partition_path}")
