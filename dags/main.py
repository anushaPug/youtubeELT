from airflow import DAG
import pendulum 
from datetime import datetime, timedelta
from api.video_stats import get_playlist_upload_id, get_video_ids, extract_video_stats, save_to_json


from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality


#define local timezone
local_tz = pendulum.timezone("America/Los_Angeles")

#default args
default_args = {
    "owner": "anu_user",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "",
    #"retries": 1,
    #"retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(minutes=60),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz)
    #"end_date": datetime(2023, 12, 31, tzinfo=local_tz)
}

#variables
staging_schema = "staging"
core_schema = "core"

with DAG(
    dag_id = 'youtube_video_stats_dag',
    default_args =  default_args,
    description = 'A DAG to extract YouTube video statistics and save to JSON',
    schedule = '0 6 * * *', #daily at 6 AM,
    catchup = False
) as dag:
    #define tasks
    playlist_id = get_playlist_upload_id()
    video_ids = get_video_ids(playlist_id)
    video_data = extract_video_stats(video_ids)
    save_to_json_task = save_to_json(video_data)

    #define task dependencies
    playlist_id >> video_ids >> video_data >> save_to_json_task

with DAG(
    dag_id = 'update_db',
    default_args =  default_args,
    description = 'A DAG to process JSON file and insert data into staging and core tables',
    schedule = '0 7 * * *', #daily at 7 AM,
    catchup = False
) as dag:
    #define tasks
    update_staging = staging_table()
    update_core = core_table()

    #define task dependencies
    update_staging >> update_core

with DAG(
    dag_id = 'data_quality',
    default_args =  default_args,
    description = 'A DAG to check data quality on both layers in db',
    schedule = '0 8 * * *', #daily at 8 AM,
    catchup = False
) as dag:
    #define tasks
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)

    #define task dependencies
    soda_validate_staging >> soda_validate_core