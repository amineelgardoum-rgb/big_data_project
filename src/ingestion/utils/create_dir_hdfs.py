from .date import DATE
from .run_cmd import run_cmd
from .log import log
from .give_permission_to_trino import give_permission_trino


def hdfs_dir_exists(path: str) -> bool:
    """
    Check if an HDFS directory exists by listing it.
    """
    output = run_cmd(f"docker exec namenode hdfs dfs -ls {path}")
    return bool(output.strip())



def create_hdfs_dirs(
    hdfs_orders_path=f"/raw/orders/orders_date={DATE}",
    hdfs_stock_path=f"/raw/stock/stock_date={DATE}",
):
    log("Checking HDFS directories...", "INFO")
    give_permission_trino()

    for path in [hdfs_orders_path, hdfs_stock_path]:
        if hdfs_dir_exists(path):
            log(f"HDFS directory already exists: {path}", "SKIP")
        else:
            # Print CREATE log BEFORE running mkdir
            log(f"Creating HDFS directory: {path}", "CREATE")
            run_cmd(f"docker exec namenode hdfs dfs -mkdir -p {path}", check=True)
            print()

    log("HDFS directory check completed.", "INFO")
    
