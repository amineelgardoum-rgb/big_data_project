
WITH aggregated_orders AS (
    SELECT 
        "sku-id" as sku_id,
        SUM(quantity_ordered) as total_ordered
    FROM hive.default.orders
    WHERE orders_date = '2026-01-13'
    GROUP BY "sku-id"
),

latest_inventory AS (
    SELECT 
        sku_id,
        SUM(CAST(available_quantity AS INTEGER)) as total_available,
        SUM(CAST(reserved_quantity AS INTEGER)) as total_reserved
    FROM hive.default.warehouse_inventory
    WHERE stock_date = '2026-01-13'
    GROUP BY sku_id
),

net_demand_raw AS (
    SELECT 
        p.supplier_id,
        s.supplier_name,
        p.sku_id,
        p.product_name,
        p.pack_size,
        p.min_order_qty,
        p.unit_price,
        COALESCE(o.total_ordered, 0) as daily_demand,
        COALESCE(i.total_available, 0) as available_stock,
        COALESCE(i.total_reserved, 0) as reserved_stock,
        GREATEST(0, 
            COALESCE(o.total_ordered, 0) + p.safety_stock - 
            (COALESCE(i.total_available, 0) - COALESCE(i.total_reserved, 0))
        ) as raw_demand
    FROM postgresql.public.products p
    INNER JOIN postgresql.public.suppliers s ON p.supplier_id = s.supplier_id
    LEFT JOIN aggregated_orders o ON p.sku_id = o.sku_id
    LEFT JOIN latest_inventory i ON p.sku_id = i.sku_id
)

SELECT 
    supplier_id,
    supplier_name,
    '2026-01-13' as order_date,
    sku_id,
    product_name,
    pack_size,
    min_order_qty,
    CAST(unit_price AS DECIMAL(10,2)) as unit_price,
    daily_demand,
    available_stock,
    reserved_stock,
    raw_demand,
    CASE 
        WHEN raw_demand = 0 THEN 0
        WHEN raw_demand < min_order_qty THEN min_order_qty
        ELSE CAST(CEIL(CAST(raw_demand AS DOUBLE) / pack_size) AS INTEGER) * pack_size
    END as order_quantity
FROM net_demand_raw
WHERE CASE 
        WHEN raw_demand = 0 THEN 0
        WHEN raw_demand < min_order_qty THEN min_order_qty
        ELSE CAST(CEIL(CAST(raw_demand AS DOUBLE) / pack_size) AS INTEGER) * pack_size
    END > 0
ORDER BY supplier_id, sku_id;