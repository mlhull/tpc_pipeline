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

window_spec = Window.partitionBy("C_CUSTKEY", "YEAR")

#join, filter, and calculate total orders placed
by_customer_df = customer_df \
.join(orders_df, orders_df.O_CUSTKEY == customer_df.C_CUSTKEY, "inner") \
.filter((year(to_date(col("O_ORDERDATE"), 'yyyy-MM-dd')).isin([1996, 1997, 1998]))) \
.select(
        col("C_NAME").alias("CUSTOMER_NAME"),
        col("C_CUSTKEY"),
        year(to_date(col("O_ORDERDATE"), 'yyyy-MM-dd')).alias("YEAR")
) \
.withColumn("TOT_ORDERS_PLACED", count("*").over(window_spec)) \
.distinct()

#pivot, rename columns, and calculate percentage change in a single step
by_customer_df.groupBy("C_CUSTKEY", "CUSTOMER_NAME") \
.pivot("YEAR", [1996, 1997, 1998]) \
.agg(max("TOT_ORDERS_PLACED")) \
.withColumnRenamed("1998", "CY_ORDERS_PLACED") \
.withColumn("CY_ORDERS_PLACED", coalesce(col("CY_ORDERS_PLACED"), lit(0))) \
.withColumnRenamed("1997", "PY_ORDERS_PLACED") \
.withColumn("PY_ORDERS_PLACED", coalesce(col("PY_ORDERS_PLACED"), lit(0))) \
.withColumn("PCT_CHG_IN_ORDERS",
                when(col("PY_ORDERS_PLACED") == 0, 100.0)
                .when(col("CY_ORDERS_PLACED") == 0, -100.0)
                .otherwise((col("CY_ORDERS_PLACED") - col("PY_ORDERS_PLACED")) / col("PY_ORDERS_PLACED") * 100)) \
.drop("C_CUSTKEY") \
.write.mode("overwrite").parquet(f"{tpc_results}/q06_cust_behavior/{partition_path}")
