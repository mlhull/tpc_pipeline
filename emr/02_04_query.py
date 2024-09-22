from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *
from datetime import datetime
import os

# Create Spark session
spark = SparkSession.builder.appName("q04_top_supp_by_prod_cat").getOrCreate()

tpc_results = os.getenv('tpc_results')
tpc_parquet = os.getenv('tpc_parquet')

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

#window function used to calculate sum supplied by category and supplier
window_spec_sum = Window.partitionBy("P_TYPE", "S_NAME")

#window function used for rank
window_spec = Window.partitionBy("CATEGORY").orderBy(col("TOT_PARTS_SUPPLIED").desc())

#calculate total parts supplied by part category and supplier 
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

#calculate total parts supplied by part category (NOT by supplier)
window_spec = Window.partitionBy("P_TYPE")

part_category_totals_df = part_df.join(partsupp_df, part_df.P_PARTKEY == partsupp_df.PS_PARTKEY, "inner") \
.join(supplier_df, partsupp_df.PS_SUPPKEY == supplier_df.S_SUPPKEY, "inner") \
.join(lineitem_df, (partsupp_df.PS_PARTKEY == lineitem_df.L_PARTKEY) & (partsupp_df.PS_SUPPKEY == lineitem_df.L_SUPPKEY), "inner") \
.withColumn("TOT_PARTS_BY_CATEGORY", sum("PS_AVAILQTY").over(window_spec)) \
.select(
        col("P_TYPE").alias("CATEGORY"),
        "TOT_PARTS_BY_CATEGORY"
) \
.distinct()

#calculate total parts by each supplied by part category per supplier
part_category_totals_df.join(tot_prod_by_cat_df, tot_prod_by_cat_df.CATEGORY == part_category_totals_df.CATEGORY, "inner") \
.select(part_category_totals_df.CATEGORY,
        tot_prod_by_cat_df.SUPPLIER_NAME,
        col("TOT_PARTS_SUPPLIED"),
        ((col("TOT_PARTS_SUPPLIED") / col("TOT_PARTS_BY_CATEGORY"))*100).alias("PCT_TOT_SUPPLIED")) \
        .orderBy("CATEGORY") \
        .write.mode("overwrite").parquet(f"{tpc_results}/q04_top_supp_by_prod_cat/{partition_path}")

spark.stop()
