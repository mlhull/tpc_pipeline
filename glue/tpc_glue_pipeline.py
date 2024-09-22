from airflow.decorators import dag
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

##config

#pre-req: aws_conn_id entered in Airflow UI (Connection)

#pre-req: env vars entered in Airflow UI (Variables)
src_bucket = Variable.get('s3_tpc_src') 
apps = Variable.get('s3_tpc_apps')

#pre-req: create glue etl jobs using 01_g_file_converter.py and 02_g_0*_query.py applications

#pre-req: tpc_crawler created in AWS console
glue_crawler_config = {
    "Name": "tpc_crawler",
}

##

##dag; for daily run change schedule=timedelta(days=1)
@dag(
    start_date=datetime(2024, 9, 16),
    schedule=None,
    catchup=False,
    tags=['s3_sensor', 'emr', 'glue', 'tpc_pipeline'],
    default_args={
        'owner': '[NAME]',
        'retries': 0
    }
)
def tpc_glue_pipeline():

    check_for_tps_data = S3KeySensor.partial(
        task_id='check_for_tps_data',
        aws_conn_id = 'AWSConnection',
        wildcard_match=False,
        ).expand(bucket_key=[f's3://{src_bucket}/customer.tbl.zip',
                        f's3://{src_bucket}/orders.tbl.zip',
                        f's3://{src_bucket}/lineitem.tbl.zip',
                        f's3://{src_bucket}/part.tbl.zip',
                        f's3://{src_bucket}/supplier.tbl.zip',
                        f's3://{src_bucket}/partsupp.tbl.zip',
                        f's3://{src_bucket}/nation.tbl',
                        f's3://{src_bucket}/region.tbl'
                        ])
    
    convert_file = GlueJobOperator(
        task_id="file_converter",
        job_name="01_g_file_converter",
        aws_conn_id = 'AWSConnection',
        )

    with TaskGroup(group_id='queries') as task_group:

        run_query_1 = GlueJobOperator(
        task_id="02_01_query",
        job_name="02_g_01_query",
        aws_conn_id = 'AWSConnection',
        )

        run_query_2 = GlueJobOperator(
        task_id="02_02_query",
        job_name="02_g_02_query",
        aws_conn_id = 'AWSConnection',
        )

        run_query_3 = GlueJobOperator(
        task_id="02_03_query",
        job_name="02_g_03_query",
        aws_conn_id = 'AWSConnection',
        )

        run_query_4 = GlueJobOperator(
        task_id="02_04_query",
        job_name="02_g_04_query",
        aws_conn_id = 'AWSConnection',
        )

        run_query_5 = GlueJobOperator(
        task_id="02_05_query",
        job_name="02_g_05_query",
        aws_conn_id = 'AWSConnection',
        )

        run_query_6 = GlueJobOperator(
        task_id="02_06_query",
        job_name="02_g_06_query",
        aws_conn_id = 'AWSConnection',
        )

        run_query_7 = GlueJobOperator(
        task_id="02_07_query",
        job_name="02_g_07_query",
        aws_conn_id = 'AWSConnection',
        )

        run_query_8 = GlueJobOperator(
        task_id="02_08_query",
        job_name="02_g_08_query",
        aws_conn_id = 'AWSConnection',
        )

        run_query_9 = GlueJobOperator(
        task_id="02_09_query",
        job_name="02_g_09_query",
        aws_conn_id = 'AWSConnection',
        )

        run_query_10 = GlueJobOperator(
        task_id="02_10_query",
        job_name="02_g_10_query",
        aws_conn_id = 'AWSConnection',
        )

        run_query_1 >> run_query_2 >> run_query_3 >> run_query_4 >> run_query_5 >> run_query_6 >> run_query_7 >> run_query_8 >> run_query_9 >> run_query_10

    glue_crawler = GlueCrawlerOperator(
        task_id="catalogue_metadata",
        config=glue_crawler_config,
        aws_conn_id='AWSConnection',
        poll_interval=60
    )
        
    check_for_tps_data >>  convert_file >> task_group >> glue_crawler

tpc_glue_pipeline()
