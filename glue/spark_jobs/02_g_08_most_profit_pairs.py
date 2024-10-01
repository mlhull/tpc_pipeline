#import dependencies for spark and glue context, incl glue job
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','TPC_RESULTS','TPC_PARQUET'])

#set up glue and spark context, including spark call to spark job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

#grab s3 paths from glue job arg
tpc_parquet = args['TPC_PARQUET']
tpc_results = args['TPC_RESULTS']

#import dependencies for spark job
from pyspark.sql import Window
from pyspark.sql.functions import *
from datetime import datetime

# Set for partition
current_date = datetime.today()
current_year = current_date.year
current_month = current_date.month
current_day = current_date.day

partition_path = f"year={current_year}/month={current_month:02d}/day={current_day:02d}"

#create necessary df from parquet files, grabbing latest partition
lineitem_df = spark.read.parquet(f"{tpc_parquet}/lineitem/") \
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
#self-join lineitem df to identify partkey combinations within the same order
#create columns to capture profit for each product in a pair, then calculate the combined profit for the pair
partkey_pairs_df = lineitem_df.alias("a") \
.join(lineitem_df.alias("b"), 
        (col("a.L_ORDERKEY") == col("b.L_ORDERKEY")) & 
        (col("a.L_PARTKEY") < col("b.L_PARTKEY"))) \
.select(
        col("a.L_ORDERKEY").alias("L_ORDERKEY"),
        col("a.L_PARTKEY").alias("PARTKEY_1"),
        (col("a.L_EXTENDEDPRICE") / col("a.L_QUANTITY")).alias("PARTKEY_1_PROFIT"),
        col("b.L_PARTKEY").alias("PARTKEY_2"),
        (col("b.L_EXTENDEDPRICE") / col("b.L_QUANTITY")).alias("PARTKEY_2_PROFIT")
) \
.withColumn("PAIR_PROFIT", col("PARTKEY_1_PROFIT") + col("PARTKEY_2_PROFIT"))

#define a window spec to order pairs by combined profit in descending order
window_spec = Window.orderBy(col("PAIR_PROFIT").desc())

#retrieve top 5 most profitable part pairs based on combined profit
#create an ordered pair struct (PARTKEY_PAIR) to facilitate downstream joins and prevent duplication
top_pairs_df = partkey_pairs_df.withColumn("Rank", rank().over(window_spec)) \
.filter(col("Rank") <= 5) \
.orderBy("Rank") \
.drop("Rank") \
.withColumn("PARTKEY_PAIR", struct(least(col("PARTKEY_1"), col("PARTKEY_2")), greatest(col("PARTKEY_1"), col("PARTKEY_2")))) \
.join(part_df.alias("p1"), col("p1.P_PARTKEY") == col("PARTKEY_1"), "left") \
.join(part_df.alias("p2"), col("p2.P_PARTKEY") == col("PARTKEY_2"), "left") \
.select(
        col("PARTKEY_1"),
        col("PARTKEY_2"),
        struct(
        col("p1.P_NAME").alias("P_NAME_1"),
        col("p2.P_NAME").alias("P_NAME_2")
        ).alias("PART_NAMES"),
        col("PAIR_PROFIT").alias("TOP_PAIR_PROFIT"),
        col("PARTKEY_PAIR")
)

#create PARTKEY_PAIR column to capture the ordered pair struct for all partkey pairs
partkey_pairs_df = partkey_pairs_df.withColumn("PARTKEY_PAIR",
struct(least(col("PARTKEY_1"), col("PARTKEY_2")), greatest(col("PARTKEY_1"), col("PARTKEY_2")))
)

#join the original partkey pairs df with top_pairs_df on PARTKEY_PAIR to match part pairs with their names and profits
partkey_pairs_df = partkey_pairs_df \
.join(top_pairs_df.select("PARTKEY_PAIR", "PART_NAMES", "TOP_PAIR_PROFIT"), on="PARTKEY_PAIR") \
.select(
        col("L_ORDERKEY").alias("ORDERKEY"),
        col("PART_NAMES"),
        col("TOP_PAIR_PROFIT").alias("PAIR_PROFIT")
)

#create a window spec to calculate total revenue per order
window_spec_total_revenue = Window.partitionBy("ORDERKEY")

#determine what percentage of the revenue comes from the top part pair
#calculate total order revenue and the percentage of revenue attributed to the top part pair
#remove unnecessary columns, ensure distinct rows, and export the result to Parquet
partkey_pairs_df \
.join(lineitem_df, partkey_pairs_df.ORDERKEY == lineitem_df.L_ORDERKEY, "inner") \
.select(
        partkey_pairs_df.ORDERKEY,
        col("PART_NAMES"),
        col("L_EXTENDEDPRICE").alias("REVENUE"),
        col("PAIR_PROFIT")
) \
.withColumn("TOTAL_REVENUE", sum("REVENUE").over(window_spec_total_revenue)) \
.withColumn("PCT_PAIR_PROFIT", col("PAIR_PROFIT") / col("TOTAL_REVENUE")) \
.drop("REVENUE") \
.distinct() \
.write.mode("overwrite").parquet(f"{tpc_results}/q08_most_profit_pairs/{partition_path}")
