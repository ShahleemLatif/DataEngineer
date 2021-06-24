from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    "owner": "udacity",
    "start_date": datetime(2019, 1, 12),
    "depends_on_past": False,
    "email": ["dmax37100@hotmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("udac_example_dag",
          default_args=default_args,
          description="Load and transform data in Redshift with Airflow",
          schedule_interval=None,
        )
dag.doc_md = """
### DAG Summary
This is the Data Pipeline Project
"""


start_operator = DummyOperator(task_id="Begin_execution",  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    table = "staging_events",
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key = "log_data"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    table = "staging_songs",
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key = "song_songs"
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    table="songplays",
    redshift_conn_id="redshift",
    select_sql=SqlQueries.songplay_table_insert
)

load_user_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    table="users",
    redshift_conn_id="redshift",
    select_sql=SqlQueries.user_table_insert
)

load_song_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    table="songs",
    redshift_conn_id="redshift",
    select_sql=SqlQueries.song_table_insert
)

load_artist_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    table="artists",
    redshift_conn_id="redshift",
    select_sql=SqlQueries.artist_table_insert,
    primary_key="artistid",
    append_insert=True
)

load_time_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    table="time",
    redshift_conn_id="redshift",
    select_sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    test_query="select count(*) from songs where songid is null;",
    expected_result=0
)

end_operator = DummyOperator(task_id="Stop_execution",  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_table >> run_quality_checks
load_songplays_table >> load_user_table >> run_quality_checks
load_songplays_table >> load_artist_table  >> run_quality_checks
load_songplays_table >> load_time_table >> run_quality_checks
run_quality_checks >> end_operator