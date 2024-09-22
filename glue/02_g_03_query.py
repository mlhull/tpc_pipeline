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

#grab delayed lineitems (note: 1 order can have >1 lineitem w/ diff shipdate values)
delay_lineitems_df = lineitem_df \
        .withColumn("DAY_DIFF", datediff(to_date(col("L_SHIPDATE"),'yyyy-MM-dd'), to_date(col("L_COMMITDATE"), 'yyyy-MM-dd'))) \
        .filter(col("DAY_DIFF") > 0) \
        .select("L_ORDERKEY", "DAY_DIFF", lit(1).alias("DELAY_FLAG"))

delay_by_customer_df = customer_df.join(orders_df, col("C_CUSTKEY") == col("O_CUSTKEY"), "inner") \
        .join(delay_lineitems_df, col("O_ORDERKEY") == col("L_ORDERKEY"), "inner") \
        .select("C_CUSTKEY", "C_NAME", "O_ORDERKEY", "DAY_DIFF") 

#use a window function to calculate avg days delayed by customer. more performant than an agg/groupby
window_spec = Window.partitionBy("C_CUSTKEY")

#add avg delays to customer df that's limited to customers that experienced delays
delay_with_avg_df = delay_by_customer_df \
        .withColumn("AVG_DELAY_DAYS", avg("DAY_DIFF").over(window_spec)) \
        .select("C_CUSTKEY", "C_NAME", "AVG_DELAY_DAYS") \
        .dropDuplicates()  #drop duplicates to get one row per customer

#get PCT_ORDERS_DELAY by CUSTOMER
total_orders_df = orders_df.groupBy("O_CUSTKEY") \
        .agg(countDistinct("O_ORDERKEY").alias("TOTAL_ORDERS"))

#not using a window function here since that'd require a pre processing df with no dups
sum_delay_order_df = delay_by_customer_df.dropDuplicates(["C_CUSTKEY", "O_ORDERKEY"]) \
        .groupBy("C_CUSTKEY") \
        .agg(count("O_ORDERKEY").alias("TOT_DELAY_ORDERS"))

sum_delay_order_df.join(total_orders_df, total_orders_df.O_CUSTKEY == sum_delay_order_df.C_CUSTKEY, "inner") \
        .join(delay_with_avg_df, delay_with_avg_df.C_CUSTKEY == sum_delay_order_df.C_CUSTKEY, "inner") \
        .select("C_NAME", "AVG_DELAY_DAYS",
                ((col("TOT_DELAY_ORDERS") / col("TOTAL_ORDERS"))*100).alias("PCT_DELAYED_ORDERS")) \
        .orderBy(col("PCT_DELAYED_ORDERS").desc()) \
        .limit(5) \
        .write.mode("overwrite").parquet(f"{tpc_results}/q03_delay_ords_pct_by_cust/{partition_path}")
