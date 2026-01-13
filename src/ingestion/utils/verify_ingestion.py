from .run_cmd import run_cmd
from .log import log

def verify_ingestion(directory="/raw"):
    output_ls = run_cmd(f"docker exec namenode hdfs dfs -ls -R {directory}")

    if not output_ls.strip():
        log(f"No files found in {directory}", "WARN")
        return

    lines = output_ls.splitlines()

    orders = [l for l in lines if l.startswith("-") and "/orders/" in l]
    stock  = [l for l in lines if l.startswith("-") and "/stock/" in l]

    log("=== Orders Files ===", "INFO")
    if orders:
        for line in orders:
            log(line.strip(), "INFO")
    else:
        log("No orders found", "WARN")

    print()

    log("=== Stock Files ===", "INFO")
    if stock:
        for line in stock:
            log(line.strip(), "INFO")
    else:
        log("No stock found", "WARN")

    print()
