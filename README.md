# TPC-H ETL Pipeline Using AWS and Airflow

## Table of Contents
- [Description](#description)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Dataset](#dataset)

## Description

This project implements an ETL pipeline using AWS services and Apache Airflow to extract, transform, and load TPC-H data. The pipeline processes the data through various stages:
- Extracting TPC-H data from an S3 bucket
- Running transformations via a Spark cluster on AWS EMR or using Glue jobs
- Answering key business questions through SQL queries 
- Storing the staging data and results in S3 as partitioned Parquet files and cataloging the data using AWS Glue 

## Features
  - **Airflow orchestration**: The pipeline is triggered by detecting files in the source S3 bucket.
  - **Two versions for processing data using Apache Spark:
    - [AWS EMR](https://github.com/mlhull/tpc_pipeline/tree/main/emr)
    - [AWS Glue](https://github.com/mlhull/tpc_pipeline/tree/main/glue)
  - **S3 for storage**: Data is stored in different phases of the ETL process (source, staging, and target).
  - **Glue Crawler**: AWS Glue crawlers catalog metadata into the Glue Data Catalog for easier querying.

## Installation
- Set up Airflow and ensure the necessary connections to S3, EMR, and Glue.
- Pipelines used different application files and DAGs
  - AWS EMR pipeline uses: tpc_emr_pipeline.py, 01_file_converter.py, and 02_0*_query.py files
  - AWS Glue pipeline uses: tpc_glue_pipeline.py, 01_g_file_converter.py, and 02_g_0*_query.py files

## Usage
1. Trigger the DAG in Apache Airflow to start the pipeline.
2. The pipeline will automatically detect data in S3 and begin processing.
3. Results will be saved in the target S3 bucket, and metadata will be available in AWS Glue Data Catalog.

## Dataset
- **Reference**: [TPC-H Specification](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf)
- **Download**: [TPC-H-Source](https://www.kaggle.com/datasets/razasiddique/ddos-tcp-dataset)

