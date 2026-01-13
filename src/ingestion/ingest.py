from .utils.create_dir_hdfs import create_hdfs_dirs
from .utils.copy_local_dirs_to_hdfs import copy_docker_mounted_dir_into_hdfs
from .utils.verify_ingestion import verify_ingestion
from .utils.date import DATE
from .utils.log import log


def ingest():
    log(f"Creating HDFS directories for DATE: {DATE}...", "INFO")
    print()
    create_hdfs_dirs()
    print()

    log("Starting the ingestion of data to HDFS...", "INFO")
    print()

    log("Ingesting Orders...", "INFO")
    copy_docker_mounted_dir_into_hdfs("orders")
    print()  
    
    log("Ingesting Stock...", "INFO")
    copy_docker_mounted_dir_into_hdfs("stock")
    print()

    log("Verifying ingestion...", "INFO")
    verify_ingestion()
