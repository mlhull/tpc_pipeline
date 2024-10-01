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

        q01_most_profit_prods = GlueJobOperator(
        task_id="02_01_most_profit_prods",
        job_name="02_g_01_most_profit_prods",
        aws_conn_id = 'AWSConnection',
        )

        q02_sales_per_by_region = GlueJobOperator(
        task_id="02_02_sales_per_by_region",
        job_name="02_g_02_sales_per_by_region",
        aws_conn_id = 'AWSConnection',
        )

        q03_delay_ords_pct_by_cust = GlueJobOperator(
        task_id="02_03_delay_ords_pct_by_cust",
        job_name="02_g_03_delay_ords_pct_by_cust",
        aws_conn_id = 'AWSConnection',
        )

        q04_top_supp_by_prod_cat = GlueJobOperator(
        task_id="02_04_top_supp_by_prod_cat",
        job_name="02_g_04_top_supp_by_prod_cat",
        aws_conn_id = 'AWSConnection',
        )

        q05_disc_on_sales = GlueJobOperator(
        task_id="02_05_disc_on_sales",
        job_name="02_g_05_disc_on_sales",
        aws_conn_id = 'AWSConnection',
        )

        q06_cust_behavior = GlueJobOperator(
        task_id="02_06_cust_behavior",
        job_name="02_g_06_cust_behavior",
        aws_conn_id = 'AWSConnection',
        )

        q07_effect_mkt_campaigns = GlueJobOperator(
        task_id="02_07_effect_mkt_campaigns",
        job_name="02_g_07_effect_mkt_campaigns",
        aws_conn_id = 'AWSConnection',
        )

        q08_most_profit_pairs = GlueJobOperator(
        task_id="02_08_most_profit_pairs",
        job_name="02_g_08_most_profit_pairs",
        aws_conn_id = 'AWSConnection',
        )

        q09_sale_lead_time = GlueJobOperator(
        task_id="02_09_sale_lead_time",
        job_name="02_g_09_sale_lead_time",
        aws_conn_id = 'AWSConnection',
        )

        q10_chg_supp_perf = GlueJobOperator(
        task_id="02_10_chg_supp_perf",
        job_name="02_g_10_chg_supp_perf",
        aws_conn_id = 'AWSConnection',
        )

        q01_most_profit_prods >> q02_sales_per_by_region >> q03_delay_ords_pct_by_cust >> q04_top_supp_by_prod_cat >> q05_disc_on_sales >> q06_cust_behavior >> q07_effect_mkt_campaigns >> q08_most_profit_pairs >> q09_sale_lead_time >> q10_chg_supp_perf

    glue_crawler = GlueCrawlerOperator(
        task_id="catalogue_metadata",
        config=glue_crawler_config,
        aws_conn_id='AWSConnection',
        poll_interval=60
    )
        
    check_for_tps_data >>  convert_file >> task_group >> glue_crawler

tpc_glue_pipeline()
