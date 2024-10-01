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
    
<table>
  <tr>
    <td align="center">
      <img src="https://github.com/mlhull/tpc_pipeline/blob/main/emr/TPC%20Pipeline%20Architecture%20Diagram_EMR.png" alt="EMR" width="300"/><br>
      <p>Figure: TPC Pipeline Architecture Diagram (EMR)</p>
    </td>
    <td align="center">
      <img src="https://github.com/mlhull/tpc_pipeline/blob/main/glue/TPC%20Pipeline%20Architecture%20Diagram_Glue.png" alt="Glue" width="300"/><br>
      <p>Figure: TPC Pipeline Architecture Diagram (Glue)</p>
    </td>
  </tr>
</table>

Based on Airflow task durations, Glue and EMR-based Spark Jobs took ~15 minutes to complete. This was even when using a spot instance for the EMR-based pipeline. However, using the AWS pricing calculator, we see that the EMR-based pipeline cost slightly less to run 1x/month than the Glue-based pipeline. Different factors (e.g., security, flexibility in Spark versioning, etc) need to be weighed when choosing which approach to use:
  
| **Service** | **Cost Breakdown**                                                                                                                                                     | **Total Monthly Cost**  |
|-------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|
| **EMR**     | 1 instance [(1 m4.xlarge master + 1 m4.xlarge worker)] x 0.06 USD hourly x (1 / 24 hours in a day) x 730 hours in a month = 1.8250 USD (core node cost)           | 1.8250 USD (Core Node) + 0.12 USD (1X Month) = 1.9450 USD  |
| **Glue**    | 2 DPUs x 0.25 hours x 0.44 USD per DPU-Hour = 0.22 USD (Apache Spark ETL job cost)                                                                                 | 0.22 USD (1X Month)    |

  
  - **S3 for storage**: Data is stored in different phases of the ETL process (source, staging, and target).
  - **Glue Crawler**: AWS Glue crawlers catalog metadata into the Glue Data Catalog for easier querying.

## Installation
- Install Airflow. I recommend using Astro's Docker Image because, as one of many benefits, this ensures you're using the last instance of Airflow.
- Create AWS service account with permissions to interact S3, EMR and Glue (job, crawler) resources.
- Within Airflow's Connections, add your AWS service account from the above step. Ensure your DAG references this by its connection name
- Two pipeline options and their application files + DAGs:
    - [AWS EMR](https://github.com/mlhull/tpc_pipeline/tree/main/emr)
    - [AWS Glue](https://github.com/mlhull/tpc_pipeline/tree/main/glue)

## Usage
1. Trigger the DAG in Apache Airflow to start the pipeline.
2. The pipeline will automatically detect data in the S3 source bucket and begin processing.
3. Results will be saved in the target S3 bucket, and metadata will be available in AWS Glue Data Catalog.

## Dataset
- **Reference**: [TPC-H Specification](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf)
- **Download**: [TPC-H-Source](https://www.kaggle.com/datasets/razasiddique/ddos-tcp-dataset)

