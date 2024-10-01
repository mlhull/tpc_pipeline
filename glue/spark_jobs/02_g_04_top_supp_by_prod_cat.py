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

partsupp_df = spark.read.parquet(f"{tpc_parquet}/partsupp/") \
        .filter(
                (col("year") == current_year) &
                (col("month") == current_month) &
                (col("day") == current_day)
        )

supplier_df = spark.read.parquet(f"{tpc_parquet}/supplier/") \
        .filter(
                (col("year") == current_year) &
                (col("month") == current_month) &
                (col("day") == current_day)
        )

#define a window spec to calculate the total parts supplied, partitioned by product category and supplier
window_spec_sum = Window.partitionBy("P_TYPE", "S_NAME")

#create another window spec to rank suppliers by total parts supplied within each category
window_spec = Window.partitionBy("CATEGORY").orderBy(col("TOT_PARTS_SUPPLIED").desc())

#calculate the total parts supplied for each product category and supplier, using one of the window functions
#remove duplicates and then rank suppliers within each category based on total parts supplied
#keep only the top 3 suppliers by total parts supplied for each category
tot_prod_by_cat_df = part_df.repartition(16, part_df.P_PARTKEY) \
.join(partsupp_df, part_df.P_PARTKEY == partsupp_df.PS_PARTKEY, "inner") \
.join(supplier_df, partsupp_df.PS_SUPPKEY == supplier_df.S_SUPPKEY, "inner") \
.join(lineitem_df, (partsupp_df.PS_PARTKEY == lineitem_df.L_PARTKEY) & (partsupp_df.PS_SUPPKEY == lineitem_df.L_SUPPKEY), "inner") \
.withColumn("TOT_PARTS_SUPPLIED", sum("PS_AVAILQTY").over(window_spec_sum)) \
.select(
        col("P_TYPE").alias("CATEGORY"),
        col("S_NAME").alias("SUPPLIER_NAME"),
        "TOT_PARTS_SUPPLIED"
) \
.dropDuplicates(["CATEGORY", "SUPPLIER_NAME"]) \
.withColumn("rank", rank().over(window_spec)) \
.filter(col("rank") <= 3) \
.drop("rank")

#define a new window spec to calculate the total parts supplied per product category (across all suppliers).
window_spec = Window.partitionBy("P_TYPE")

#calculate the total parts supplied for each product category, ignoring supplier breakdown
part_category_totals_df = part_df.join(partsupp_df, part_df.P_PARTKEY == partsupp_df.PS_PARTKEY, "inner") \
.join(supplier_df, partsupp_df.PS_SUPPKEY == supplier_df.S_SUPPKEY, "inner") \
.join(lineitem_df, (partsupp_df.PS_PARTKEY == lineitem_df.L_PARTKEY) & (partsupp_df.PS_SUPPKEY == lineitem_df.L_SUPPKEY), "inner") \
.withColumn("TOT_PARTS_BY_CATEGORY", sum("PS_AVAILQTY").over(window_spec)) \
.select(
        col("P_TYPE").alias("CATEGORY"),
        "TOT_PARTS_BY_CATEGORY"
) \
.distinct()

#join the total parts supplied by each supplier with the total parts by category to calculate the percentage contribution of each supplier.
#calculate the percentage of total parts supplied by each supplier, sorting for readability
#write aggregated reseults out to parquet formatted tables
part_category_totals_df.join(tot_prod_by_cat_df, tot_prod_by_cat_df.CATEGORY == part_category_totals_df.CATEGORY, "inner") \
.select(part_category_totals_df.CATEGORY,
        tot_prod_by_cat_df.SUPPLIER_NAME,
        col("TOT_PARTS_SUPPLIED"),
        ((col("TOT_PARTS_SUPPLIED") / col("TOT_PARTS_BY_CATEGORY"))*100).alias("PCT_TOT_SUPPLIED")) \
        .orderBy("CATEGORY") \
        .write.mode("overwrite").parquet(f"{tpc_results}/q04_top_supp_by_prod_cat/{partition_path}")
