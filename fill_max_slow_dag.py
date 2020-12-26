"""
Даг берет записи из таблицы public.noise_tracks, у которых max_slow_time is null and max_slow = 0.
Затем из полученной таблицы для каждой записи делает запрос к сырым данным eco_monitoring.noise_raw_data и
согласно first_time_data & last_time_data находит max_slow & max_slow_time

Для работы DockerOperator необходимо:
1) в Dockerfile прописать установку:
    RUN pip install docker==4.1.0
2) в docker-compouse.yml
    volumes:
        # Mount the docker socket from the host (currently my laptop) into the webserver container
        - //var/run/docker.sock:/var/run/docker.sock
"""

import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 10, 29),
    "end_date": datetime(2020, 11, 15),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15)
}

refs = dict()
refs['execution_date'] = '{{ ds }}'
refs['update_status'] = 'false'

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="@daily",
    max_active_runs=1,
    concurrency=1
)

task = DockerOperator(
    dag=dag,
    task_id='fill_noise_tracks_gaps',
    auto_remove=True,
    docker_url='unix://var/run/docker.sock',
    api_version='auto',
    image='fill_max_slow:v1.0',
    environment=refs
)