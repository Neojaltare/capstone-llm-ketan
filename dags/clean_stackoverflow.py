"""Run the capstone cleaning job, one Airflow task per StackOverflow tag.

The job itself lives in the `capstone-llm:latest` image, built from this repo's
Dockerfile. Airflow's only responsibility here is scheduling it and handing it
credentials -- it never imports the project code.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator

IMAGE = "capstone-llm:latest"

TAGS = [
    "airflow",
    "apache-spark",
    "dbt",
    "docker",
    "pyspark",
    "python-polars",
    "sql",
]

# Read from the worker's environment, which docker-compose populates from .env.
# Putting literal keys in this file would commit them to git -- the thing the
# assignment calls out as a security review finding.
AWS_ENVIRONMENT = {
    "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION", "eu-west-1"),
}

default_args = {
    "owner": "ketan",
    "retries": 2,
    # The failures seen so far were transient S3/DNS errors, so a retry has a
    # real chance of succeeding where the first attempt did not.
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="clean_stackoverflow",
    description="Clean StackOverflow questions and answers, one task per tag",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule=None,          # trigger manually; no backfill wanted
    catchup=False,
    max_active_tasks=2,     # two tags at a time: S3 writes are the bottleneck
    tags=["capstone"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # One task per tag, generated rather than copy-pasted. A failure in "dbt"
    # retries and reports on its own -- it does not block or hide the others.
    clean_tasks = []
    for tag in TAGS:
        clean_tasks.append(
            DockerOperator(
                task_id=f"clean_{tag.replace('-', '_')}",
                image=IMAGE,
                command=f"--tag {tag}",
                environment=AWS_ENVIRONMENT,
                # Talks to the host Docker daemon through the socket that
                # docker-compose mounts into the Airflow containers, so the
                # job runs as a sibling container rather than nested.
                docker_url="unix://var/run/docker.sock",
                network_mode="bridge",
                auto_remove="force",
                # DockerOperator otherwise mounts a temp dir from the worker
                # into the new container. That path exists in the worker but
                # not on the host daemon, so the mount fails.
                mount_tmp_dir=False,
                # Stream the job's stdout into the Airflow task log, so the
                # progress logging in clean.py is visible in the UI.
                tty=True,
            )
        )

    start >> clean_tasks >> end
