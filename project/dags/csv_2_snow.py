from airflow.decorators import dag
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from astro import sql as aql
from astro.sql.table import Table,Metadata
from astro.files import File
from pendulum import datetime
import pandas as pd
import os
import logging

task_logger = logging.getLogger("airflow.task")

CONN_ID = "snowflake_conn"
DB_NAME= "PROJECT_DB"
SCHEMA_NAME = "PROJECT_SCHEMA"
CSV_FILEPATH= "include/subset_energy_capacity.csv"
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/file_2_snow"


profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id=CONN_ID,
            profile_args={
                "database":DB_NAME,
                "schema":SCHEMA_NAME
                }
            )
        )
execution_config = ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH)



@aql.dataframe()
def analyze(df: pd.DataFrame):

    #print(df)
    latest_year = df["year"].max()
    yr_high_solar = df.loc[df["renewables_pct"].idxmax(),"year"]
    df["solar%"] = round(df["solar_pct"] *100,2)
    df["% Renewable Energy Sources"] = round(df["renewables_pct"] * 100, 2)

    task_logger.info(
            df[["year","solar%","% Renewable Energy Sources"]].sort_values(by="year",ascending= True).drop_duplicates()
            )

    if latest_year == yr_high_solar:
        task_logger.info(
                f"In {df['country'].unique()[0]} adoption of solar energy is growing"
                )

@dag(
        start_date=datetime(2024,5,29),
        schedule=None,
        catchup=False
    )

def energy_dag():

    load_data = aql.load_file(
            input_file=File(CSV_FILEPATH),
            output_table=Table(
                name="create_pct",
                conn_id=CONN_ID,
                metadata=Metadata(
                    database=DB_NAME,
                    schema=SCHEMA_NAME
            )
        )
    )


    dbt_tg = DbtTaskGroup(
            group_id="Data_Transformation",
            project_config=ProjectConfig(DBT_PROJECT_PATH),
            profile_config=profile_config,
            execution_config=execution_config,
            operator_args ={
                "vars":'{"country_code":"CH"}'
                },
            default_args={"retries":2}
        )

    (
        load_data
        >> dbt_tg
        >> analyze(
                Table(
                    name="create_pct",
                    metadata=Metadata(
                        database=DB_NAME,
                        schema=SCHEMA_NAME
                        ),
                    conn_id=CONN_ID
            )
        )
    )


energy_dag()

 
