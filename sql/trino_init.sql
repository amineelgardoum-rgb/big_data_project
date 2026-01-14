DROP TABLE IF EXISTS hive.default.orders;

CREATE TABLE IF NOT EXISTS hive.default.orders (
    order_id VARCHAR,
    "sku-id" VARCHAR,
    quantity_ordered INT,
    orders_date VARCHAR  -- Changed to orders_date (with 's')
)
WITH (
    external_location = 'hdfs://namenode:8020/raw/orders/',
    format = 'JSON',
    partitioned_by = ARRAY['orders_date']  -- Changed to orders_date
);
CREATE TABLE IF NOT EXISTS hive.default.warehouse_inventory (
    warehouse_id VARCHAR,
    sku_id VARCHAR,
    available_quantity VARCHAR ,
    reserved_quantity VARCHAR,
    stock_date VARCHAR
)
WITH (
    external_location = 'hdfs://namenode:8020/raw/stock/',
    format = 'CSV',
    csv_separator = ',',
    skip_header_line_count = 1,
    partitioned_by = ARRAY['stock_date']
);

CALL hive.system.sync_partition_metadata('default', 'warehouse_inventory', 'FULL');
-- Sync partitions
CALL hive.system.sync_partition_metadata('default', 'orders', 'FULL');

