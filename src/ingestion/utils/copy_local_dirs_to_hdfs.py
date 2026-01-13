# import os
from .run_cmd import run_cmd
from .date import DATE
from .log import log

ALLOWED_TYPES = {"orders", "stock"}


def copy_docker_mounted_dir_into_hdfs(data_type: str):
    """Copy files from mounted directory to HDFS"""

    if data_type not in ALLOWED_TYPES:
        raise ValueError(
            f"Invalid data_type '{data_type}'. Must be one of {ALLOWED_TYPES}"
        )

    hdfs_path = f"/raw/{data_type}/{data_type}_date={DATE}"

    # Extension selon le type
    if data_type == "orders":
        pattern = "*.jsonl"
    else:  # stock
        pattern = "*.csv"

    log(f"Starting ingestion of {data_type} for day: {DATE}", "INFO")

    run_cmd(
        f'docker exec namenode bash -c "hdfs dfs -put -f /data/{data_type}/{data_type}_date={DATE}/{pattern} {hdfs_path}/"'
    )

    log(f"Finished ingestion of {data_type} for {DATE}", "SUCCESS")
