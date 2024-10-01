from airflow.decorators import dag
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator, EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

##config

#pre-req: aws_conn_id entered in Airflow UI (Connection)

#pre-req: env vars entered in Airflow UI (Variables)
src_bucket = Variable.get('s3_tpc_src') 
apps = Variable.get('s3_tpc_apps')

#emr cluster
JOB_FLOW_OVERRIDES = {
    'Name': 'emr_tpc_cluster',
    'ReleaseLabel': 'emr-7.2.0',
    'LogUri': f's3://{apps}/logs/',
    'Applications': [{'Name': 'Spark'}],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Driver Node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Worker Nodes',
                'Market': 'SPOT',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,  
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'JobFlowRole': 'EC2_Default_Role',
    'ServiceRole': 'EMR_DefaultRole',
}

#emr steps
file_converter = [
    {
        'Name': 'File Converter Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--master',
                'yarn',
                '--conf',
                'spark.yarn.submit.waitAppCompletion=true',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_src=s3://tpc-src/',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_parquet=s3://tpc-parquet/',
                f's3://{apps}/01_file_converter.py'],
        },
    }
]

step_01_most_profit_prods = [
    {
        'Name': 'Query 1 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--master',
                'yarn',
                '--conf',
                'spark.yarn.submit.waitAppCompletion=true',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_parquet=s3://tpc-parquet/',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_results=s3://tpc-results/',
                f's3://{apps}/02_01_most_profit_prods.py'],
        },
    }
]

step_02_sales_per_by_region = [
    {
        'Name': 'Query 2 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--master',
                'yarn',
                '--conf',
                'spark.yarn.submit.waitAppCompletion=true',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_parquet=s3://tpc-parquet/',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_results=s3://tpc-results/',
                f's3://{apps}/02_02_sales_per_by_region.py'],
        },
    }
]

step_03_delay_ords_pct_by_cust = [
    {
        'Name': 'Query 3 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--master',
                'yarn',
                '--conf',
                'spark.yarn.submit.waitAppCompletion=true',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_parquet=s3://tpc-parquet/',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_results=s3://tpc-results/',
                f's3://{apps}/02_03_delay_ords_pct_by_cust.py'],
        },
    }
]

step_04_top_supp_by_prod_cat = [
    {
        'Name': 'Query 4 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--master',
                'yarn',
                '--conf',
                'spark.yarn.submit.waitAppCompletion=true',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_parquet=s3://tpc-parquet/',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_results=s3://tpc-results/',
                f's3://{apps}/02_04_top_supp_by_prod_cat.py'],
        },
    }
]
step_05_disc_on_sales = [
    {
        'Name': 'Query 5 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--master',
                'yarn',
                '--conf',
                'spark.yarn.submit.waitAppCompletion=true',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_parquet=s3://tpc-parquet/',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_results=s3://tpc-results/',
                f's3://{apps}/02_05_disc_on_sales.py'],
        },
    }
]

step_06_cust_behavior = [
    {
        'Name': 'Query 6 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--master',
                'yarn',
                '--conf',
                'spark.yarn.submit.waitAppCompletion=true',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_parquet=s3://tpc-parquet/',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_results=s3://tpc-results/',
                f's3://{apps}/02_06_cust_behavior.py'],
        },
    }
]

step_07_effect_mkt_campaigns = [
    {
        'Name': 'Query 7 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--master',
                'yarn',
                '--conf',
                'spark.yarn.submit.waitAppCompletion=true',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_parquet=s3://tpc-parquet/',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_results=s3://tpc-results/',
                f's3://{apps}/02_07_effect_mkt_campaigns.py']
        },
    }
]

step_08_most_profit_pairs = [
    {
        'Name': 'Query 8 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--master',
                'yarn',
                '--conf',
                'spark.yarn.submit.waitAppCompletion=true',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_parquet=s3://tpc-parquet/',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_results=s3://tpc-results/',
                f's3://{apps}/02_08_most_profit_pairs.py'],
        },
    }
]

step_09_sale_lead_time = [
    {
        'Name': 'Query 9 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--master',
                'yarn',
                '--conf',
                'spark.yarn.submit.waitAppCompletion=true',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_parquet=s3://tpc-parquet/',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_results=s3://tpc-results/',
                f's3://{apps}/02_09_sale_lead_time.py']
        },
    }
]

step_10_chg_supp_perf = [
    {
        'Name': 'Query 10 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--master',
                'yarn',
                '--conf',
                'spark.yarn.submit.waitAppCompletion=true',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_parquet=s3://tpc-parquet/',
                '--conf',
                'spark.yarn.appMasterEnv.tpc_results=s3://tpc-results/',
                f's3://{apps}/02_10_chg_supp_perf.py'],
        },
    }
]

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
def tpc_emr_pipeline():

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

    create_job_flow = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id = 'AWSConnection',
        wait_for_completion=True
    )   

    convert_file = EmrAddStepsOperator(
        task_id="file_converter",
        job_flow_id=create_job_flow.output,
        steps=file_converter,
        aws_conn_id = 'AWSConnection',
        wait_for_completion=True
    )

    #start task group definition
    with TaskGroup(group_id='queries') as task_group:
        q01_most_profit_prods = EmrAddStepsOperator(
        task_id="run_query_1",
        job_flow_id=create_job_flow.output,
        steps=step_01_most_profit_prods,
        aws_conn_id = 'AWSConnection',
        wait_for_completion=True)

        q02_sales_per_by_region = EmrAddStepsOperator(
        task_id="run_query_2",
        job_flow_id=create_job_flow.output,
        steps=step_02_sales_per_by_region,
        aws_conn_id = 'AWSConnection',
        wait_for_completion=True)

        q03_delay_ords_pct_by_cust = EmrAddStepsOperator(
        task_id="run_query_3",
        job_flow_id=create_job_flow.output,
        steps=step_03_delay_ords_pct_by_cust,
        aws_conn_id = 'AWSConnection',
        wait_for_completion=True)

        q04_top_supp_by_prod_cat = EmrAddStepsOperator(
        task_id="run_query_4",
        job_flow_id=create_job_flow.output,
        steps=step_04_top_supp_by_prod_cat,
        aws_conn_id = 'AWSConnection',
        wait_for_completion=True)

        q05_disc_on_sales = EmrAddStepsOperator(
        task_id="run_query_5",
        job_flow_id=create_job_flow.output,
        steps=step_05_disc_on_sales,
        aws_conn_id = 'AWSConnection',
        wait_for_completion=True)

        q06_cust_behavior = EmrAddStepsOperator(
        task_id="run_query_6",
        job_flow_id=create_job_flow.output,
        steps=step_06_cust_behavior,
        aws_conn_id = 'AWSConnection',
        wait_for_completion=True)

        q07_effect_mkt_campaigns = EmrAddStepsOperator(
        task_id="run_query_7",
        job_flow_id=create_job_flow.output,
        steps=step_07_effect_mkt_campaigns,
        aws_conn_id = 'AWSConnection',
        wait_for_completion=True)

        q08_most_profit_pairs = EmrAddStepsOperator(
        task_id="run_query_8",
        job_flow_id=create_job_flow.output,
        steps=step_08_most_profit_pairs,
        aws_conn_id = 'AWSConnection',
        wait_for_completion=True)

        q09_sale_lead_time = EmrAddStepsOperator(
        task_id="run_query_9",
        job_flow_id=create_job_flow.output,
        steps=step_09_sale_lead_time,
        aws_conn_id = 'AWSConnection',
        wait_for_completion=True)

        q10_chg_supp_perf = EmrAddStepsOperator(
        task_id="run_query_10",
        job_flow_id=create_job_flow.output,
        steps=step_10_chg_supp_perf,
        aws_conn_id = 'AWSConnection',
        wait_for_completion=True)

        #order tasks within group so that they run sequentially
        q01_most_profit_prods >> q02_sales_per_by_region >> q03_delay_ords_pct_by_cust >> q04_top_supp_by_prod_cat >> q05_disc_on_sales >> q06_cust_behavior >> q07_effect_mkt_campaigns >> q08_most_profit_pairs >> q09_sale_lead_time >> q10_chg_supp_perf

    #ensure termination of EMR cluster after ETL steps
    terminate_job_flow = EmrTerminateJobFlowOperator(
        task_id="terminate_job_flow",
        job_flow_id=create_job_flow.output,
        aws_conn_id = 'AWSConnection',
        trigger_rule=TriggerRule.ALL_DONE
    )

    #run glue crawler to catalogue metadata on this pipeline
    glue_crawler = GlueCrawlerOperator(
        task_id="catalogue_metadata",
        config=glue_crawler_config,
        aws_conn_id='AWSConnection',
        poll_interval=60
    )

    #set pipeline dependencies
    check_for_tps_data >> create_job_flow >> convert_file >> task_group >> terminate_job_flow >> glue_crawler

tpc_emr_pipeline()
