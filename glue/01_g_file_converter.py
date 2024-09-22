import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','TPC_SRC','TPC_PARQUET'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

import zipfile
import io
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from datetime import datetime

tpc_src = args['TPC_SRC']
tpc_parquet = args['TPC_PARQUET']

#set partitioning. since based on s3 key sensor it'll really reflect sense date
current_date = datetime.now()
year = current_date.year
month = current_date.month
day = current_date.day

partition_path = f"year={year}/month={month:02d}/day={day:02d}"

#function to extract zip files
def zip_extract(x):
    in_memory_data = io.BytesIO(x[1])
    file_obj = zipfile.ZipFile(in_memory_data, "r")
    files = [i for i in file_obj.namelist()]
    return dict(zip(files, [file_obj.read(file).decode('utf-8') for file in files]))

#split file contents
def process_file_content(file_content):
    lines = file_content.splitlines()
    return [line.split('|') for line in lines if line]

##

#CUSTOMER
#read in zip files as binary
s3_zip_path = f"{tpc_src}/customer.tbl.zip"
zips = spark.sparkContext.binaryFiles(s3_zip_path)

#extract zip files and convert to df
files_data = zips.map(zip_extract)

#flatten and process file contents
rows_rdd = files_data.flatMap(lambda x: [row for content in x.values() for row in process_file_content(content)])

#set schema
schema = StructType([
    StructField("C_CUSTKEY", IntegerType(), True),
    StructField("C_NAME", StringType(), True),
    StructField("C_ADDRESS", StringType(), True),
    StructField("C_NATIONKEY", IntegerType(), True),
    StructField("C_PHONE", StringType(), True),   
    StructField("C_ACCTBAL", FloatType(), True),  
    StructField("C_MKTSEGMENT", StringType(), True), 
    StructField("C_COMMENT", StringType(), True)
])

#convert rdd to df using schema but converting each rols to match expected schema values
def map_row(row):
    return (int(row[0]), row[1], row[2], int(row[3]), row[4], float(row[5]), row[6], row[7])

rows_rdd_mapped = rows_rdd.map(map_row)
files_df = spark.createDataFrame(rows_rdd_mapped, schema)

#write df to parquet
s3_output_path = f"{tpc_parquet}/customer/{partition_path}"
files_df.write.mode("overwrite").parquet(s3_output_path)


##

#LINEITEM
#read in zip files as binary
s3_zip_path = f"{tpc_src}/lineitem.tbl.zip"
zips = spark.sparkContext.binaryFiles(s3_zip_path)

#function to extract zip files and convert to df
files_data = zips.map(zip_extract)

#flatten and process file contents
rows_rdd = files_data.flatMap(lambda x: [row for content in x.values() for row in process_file_content(content)])

#set schema
schema = StructType([
    StructField("L_ORDERKEY", IntegerType(), True),
    StructField("L_PARTKEY", IntegerType(), True),
    StructField("L_SUPPKEY", IntegerType(), True),
    StructField("L_LINENUMBER", IntegerType(), True),
    StructField("L_QUANTITY", FloatType(), True),
    StructField("L_EXTENDEDPRICE", FloatType(), True),
    StructField("L_DISCOUNT", FloatType(), True),
    StructField("L_TAX", FloatType(), True),
    StructField("L_RETURNFLAG", StringType(), True),
    StructField("L_LINESTATUS", StringType(), True),
    StructField("L_SHIPDATE", StringType(), True), #changed from datetype
    StructField("L_COMMITDATE", StringType(), True), #changed from datetype
    StructField("L_RECEIPTDATE", StringType(), True), #changed from datetype
    StructField("L_SHIPINSTRUCT", StringType(), True),
    StructField("L_SHIPMODE", StringType(), True),
    StructField("L_COMMENT", StringType(), True)
    ])

#convert rdd to df using schema but converting each rols to match expected schema values
def map_row(row):
    return (int(row[0]), int(row[1]), int(row[2]), int(row[3]), float(row[4]), float(row[5]), float(row[6]), float(row[7]), row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15])

rows_rdd_mapped = rows_rdd.map(map_row)
files_df = spark.createDataFrame(rows_rdd_mapped, schema)

#write df to parquet
s3_output_path = f"{tpc_parquet}/lineitem/{partition_path}"
files_df.write.mode("overwrite").parquet(s3_output_path)

##

#ORDERS
#read in zip files as binary
s3_zip_path = f"{tpc_src}/orders.tbl.zip"
zips = spark.sparkContext.binaryFiles(s3_zip_path)

#function to extract zip files and convert to df
files_data = zips.map(zip_extract)

#flatten and process file contents
rows_rdd = files_data.flatMap(lambda x: [row for content in x.values() for row in process_file_content(content)])

#set schema
schema = StructType([
    StructField("O_ORDERKEY", IntegerType(), True),
    StructField("O_CUSTKEY", IntegerType(), True),
    StructField("O_ORDERSTATUS", StringType(), True),
    StructField("O_TOTALPRICE", FloatType(), True),
    StructField("O_ORDERDATE", StringType(), True), #changed to string
    StructField("O_ORDERPRIORITY", StringType(), True),
    StructField("O_ORDERCLERK", StringType(), True),
    StructField("O_SHIPPRIORITY", IntegerType(), True),
    StructField("O_COMMENT", StringType(), True)
    ])

#convert rdd to df using schema but converting each rols to match expected schema values
def map_row(row):
    return (int(row[0]), int(row[1]), row[2], float(row[3]), row[4], row[5], row[6], int(row[7]), row[8])

rows_rdd_mapped = rows_rdd.map(map_row)
files_df = spark.createDataFrame(rows_rdd_mapped, schema)

#write df to parquet
s3_output_path = f"{tpc_parquet}/orders/{partition_path}"
files_df.write.mode("overwrite").parquet(s3_output_path)

##

#PART
#read in zip files as binary
s3_zip_path = f"{tpc_src}/part.tbl.zip"
zips = spark.sparkContext.binaryFiles(s3_zip_path)

#function to extract zip files and convert to df
files_data = zips.map(zip_extract)

#flatten and process file contents
rows_rdd = files_data.flatMap(lambda x: [row for content in x.values() for row in process_file_content(content)])

#set schema
schema = StructType([
    StructField("P_PARTKEY", IntegerType(), True),
    StructField("P_NAME", StringType(), True),
    StructField("P_MFGR", StringType(), True),
    StructField("P_BRAND", StringType(), True),
    StructField("P_TYPE", StringType(), True),
    StructField("P_SIZE", IntegerType(), True),
    StructField("P_CONTAINER", StringType(), True),
    StructField("P_RETAILPRICE", FloatType(), True), 
    StructField("P_COMMENT", StringType(), True)
    ])

#convert rdd to df using schema but converting each rols to match expected schema values
def map_row(row):
    return (int(row[0]), row[1], row[2], row[3], row[4], int(row[5]), row[6], float(row[7]), row[8])

rows_rdd_mapped = rows_rdd.map(map_row)
files_df = spark.createDataFrame(rows_rdd_mapped, schema)

#write df to parquet
s3_output_path = f"{tpc_parquet}/part/{partition_path}"
files_df.write.mode("overwrite").parquet(s3_output_path)

##

#PARTSUPP
#read in zip files as binary
s3_zip_path = f"{tpc_src}/partsupp.tbl.zip"
zips = spark.sparkContext.binaryFiles(s3_zip_path)

#function to extract zip files and convert to df
files_data = zips.map(zip_extract)

#flatten and process file contents
rows_rdd = files_data.flatMap(lambda x: [row for content in x.values() for row in process_file_content(content)])

#set schema
schema = StructType([
    StructField("PS_PARTKEY", IntegerType(), True),
    StructField("PS_SUPPKEY", IntegerType(), True),
    StructField("PS_AVAILQTY", IntegerType(), True),
    StructField("PS_SUPPLYCOST", FloatType(), True),
    StructField("PS_COMMENT", StringType(), True)
    ])

#convert rdd to df using schema but converting each rols to match expected schema values
def map_row(row):
    return (int(row[0]), int(row[1]), int(row[2]), float(row[3]), row[4])

rows_rdd_mapped = rows_rdd.map(map_row)
files_df = spark.createDataFrame(rows_rdd_mapped, schema)

#write df to parquet
s3_output_path = f"{tpc_parquet}/partsupp/{partition_path}"
files_df.write.mode("overwrite").parquet(s3_output_path)

##

#SUPPLIER
#read in zip files as binary
s3_zip_path = f"{tpc_src}/supplier.tbl.zip"
zips = spark.sparkContext.binaryFiles(s3_zip_path)

#function to extract zip files and convert to df
files_data = zips.map(zip_extract)

#flatten and process file contents
rows_rdd = files_data.flatMap(lambda x: [row for content in x.values() for row in process_file_content(content)])

#set schema
schema = StructType([
    StructField("S_SUPPKEY", IntegerType(), True),
    StructField("S_NAME", StringType(), True),
    StructField("S_ADDRESS", StringType(), True),
    StructField("S_NATIONKEY", IntegerType(), True),
    StructField("S_PHONE", StringType(), True),   
    StructField("S_ACCTBAL", FloatType(), True),
    StructField("S_COMMENT", StringType(), True)
    ])

#convert rdd to df using schema but converting each rols to match expected schema values
def map_row(row):
    return (int(row[0]),row[1],row[2],int(row[3]),row[4],float(row[5]),row[6])

rows_rdd_mapped = rows_rdd.map(map_row)
files_df = spark.createDataFrame(rows_rdd_mapped, schema)

#write df to parquet
s3_output_path = f"{tpc_parquet}/supplier/{partition_path}"
files_df.write.mode("overwrite").parquet(s3_output_path)


#Region
s3_zip_path = f"{tpc_src}/region.tbl"

schema = StructType([
    StructField("R_REGIONKEY", IntegerType(), True),
    StructField("R_NAME", StringType(), True),
    StructField("R_COMMENT", StringType(), True)
    ])

df = spark.read.option("delimiter", "|").schema(schema).csv(s3_zip_path)

s3_output_path = f"{tpc_parquet}/region/{partition_path}"
df.write.mode("overwrite").parquet(s3_output_path)

#Nation
s3_zip_path = f"{tpc_src}/nation.tbl"

schema = StructType([
    StructField("N_NATIONKEY", IntegerType(), True),
    StructField("N_NAME", StringType(), True),
    StructField("N_REGIONKEY", IntegerType(), True),
    StructField("N_COMMENT", StringType(), True)
    ])
df = spark.read.option("delimiter", "|").schema(schema).csv(s3_zip_path)

s3_output_path = f"{tpc_parquet}/nation/{partition_path}"
df.write.mode("overwrite").parquet(s3_output_path)
