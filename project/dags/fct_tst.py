from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import pandas as pd
import requests
import random
import os

wasb_conn_id="azure_blob_connection"
DBT_EXEC_PATH=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
DBT_PROJECT_PATH=f"{os.environ['AIRFLOW_HOME']}/dags/dbt_2/trabajo_final"



profile_config = ProfileConfig(
    profile_name="defualt",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn_final",
        profile_args={
            "database":"FINAL_DB",
            "schema":"FINAL_SCHEMA"
        }
    ) 
)

execution_config = ExecutionConfig(dbt_executable_path=DBT_EXEC_PATH)



# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=0.5),
}

# Define the DAG
dag = DAG(
    'final_gestion',
    default_args=default_args,
    description='trabajo final gestion',
    schedule_interval=None,
    start_date=datetime(2024, 5, 20),
    catchup=False,
)


def get_data(**kwargs):

    ti = kwargs["ti"]
    responses = []
    for i in range(random.randint(3, 5)):
        d = requests.get("https://randomuser.me/api/?results=7")
        if d.status_code == 200:
            data = d.json()["results"]
            responses.append(pd.json_normalize(data))
    df = pd.concat(responses)
    now_name = datetime.now()
    df.to_csv(f"/tmp/{now_name}.csv",index=False)

    vals_2_pass = [
                f"{now_name}",
                len(df)
            ]
    ti.xcom_push(key= "data", value = vals_2_pass )




def decide_1(**kwargs):

    var = kwargs['ti'].xcom_pull(key='data', task_ids='process_data')
    print(var)
    print(type(var))

    if int(var[1]) <= 20:
        return "erase_data"
    else:
        return "load_azure_blob"


process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=get_data,
    provide_context=True,
    dag=dag,
)


files = BashOperator(
    dag = dag,
    task_id = "List_files",
    bash_command = "ls /tmp/"
)

decision = BranchPythonOperator(
    task_id = "decide",
    dag=dag,
    python_callable=decide_1,
    provide_context=True

)

erase_data = BashOperator(
    dag = dag,
    task_id = "erase_data",
    bash_command = "rm -rf /tmp/*"
)

load_azure_blob = LocalFilesystemToWasbOperator(
    task_id='load_azure_blob',
    wasb_conn_id='azure_blob_connection',
    container_name='final-blob-cont',
    blob_name="{{ task_instance.xcom_pull(task_ids='process_data', key='data')[0]+'.csv' }}",
    file_path="/tmp/"+"{{ task_instance.xcom_pull(task_ids='process_data', key='data')[0]+'.csv' }}",
    dag=dag,
)



external_stage = SnowflakeOperator(
    task_id="crear_task_external",
    sql="""
    USE SCHEMA FINAL_SCHEMA;
    CREATE OR REPLACE STAGE azure_input_load
    URL='azure://storage4alejandro.blob.core.windows.net/final-blob-cont'
    CREDENTIALS=(AZURE_SAS_TOKEN='sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2026-06-07T02:32:29Z&st=2024-06-03T18:32:29Z&spr=https,http&sig=mYfJW%2B5n2eAN2RLpS5Tia1SjCpOH%2FnK0ig8pKnpN3e8%3D')
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
    """,
    snowflake_conn_id="snowflake_conn_final",
    dag=dag
)


load_raw_data = SnowflakeOperator(
    task_id = "load_raw_data",
    snowflake_conn_id = "snowflake_conn_final",
    dag = dag,
    sql = """
        USE SCHEMA FINAL_SCHEMA;
        COPY INTO FINAL_SCHEMA.raw_user_data
        FROM  @azure_input_load
        PATTERN='{{ task_instance.xcom_pull(task_ids='process_data', key='data')[0]+'.csv' }}';
    """
)


dbt_tg = DbtTaskGroup(
	group_id="transformation",
	project_config = ProjectConfig(DBT_PROJECT_PATH),
	profile_config = profile_config,
	dag= dag,
	execution_config=execution_config,
	default_args={"retries":1}
)


truncate_users = SnowflakeOperator(
	dag=dag,
	task_id="truncate_users",
	snowflake_conn_id="snowflake_conn_final",
	sql = """ 
	USE SCHEMA FINAL_SCHEMA;
	TRUNCATE TABLE raw_user_data ;
	"""
)

def final_func():
    print("final")


final = PythonOperator(
    dag = dag,
    task_id = "final",
    python_callable=final_func

)

process_data_task >> decision
process_data_task >> files

decision >> erase_data
decision >> load_azure_blob

load_azure_blob >> external_stage
external_stage >> load_raw_data
load_raw_data >> dbt_tg

dbt_tg >> truncate_users
dbt_tg >> erase_data

truncate_users >> final
erase_data >> final


