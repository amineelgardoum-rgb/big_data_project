from .run_cmd import run_cmd
def give_permission_trino():
    run_cmd("docker exec  namenode hdfs dfs -chown -R trino:supergroup /raw/orders")
    run_cmd("docker exec namenode hdfs dfs -chmod -R 777 /raw/orders")

