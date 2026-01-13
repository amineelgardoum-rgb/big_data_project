-- Exception Checks (Dynamic Date)
-- Usage: Replace ${DATE} with actual date

-- Exception 1: Missing Supplier Mappings
SELECT 
    'missing_supplier_mapping' as exception_type,
    'high' as severity,
    o."sku-id" as sku_id,
    COUNT(*) as order_count,
    'SKU found in orders but not mapped to any supplier' as message
FROM hive.default.orders o
LEFT JOIN postgresql.public.products p ON o."sku-id" = p.sku_id
WHERE p.sku_id IS NULL AND o.orders_date = '${DATE}'
GROUP BY o."sku-id"

UNION ALL

-- Exception 2: Missing Inventory Data
SELECT 
    'missing_inventory_data' as exception_type,
    'medium' as severity,
    o."sku-id" as sku_id,
    NULL as order_count,
    'No inventory snapshot available for this SKU' as message
FROM hive.default.orders o
LEFT JOIN hive.default.warehouse_inventory i 
    ON o."sku-id" = i.sku_id AND i.stock_date = '${DATE}'
WHERE i.sku_id IS NULL AND o.orders_date = '${DATE}'

UNION ALL

-- Exception 3: Abnormal Demand Spikes
SELECT 
    'abnormal_demand_spike' as exception_type,
    'medium' as severity,
    o."sku-id" as sku_id,
    SUM(o.quantity_ordered) as order_count,
    CONCAT('Demand (', CAST(SUM(o.quantity_ordered) AS VARCHAR), 
           ') exceeds 5x safety stock (', CAST(p.safety_stock AS VARCHAR), ')') as message
FROM hive.default.orders o
INNER JOIN postgresql.public.products p ON o."sku-id" = p.sku_id
WHERE o.orders_date = '${DATE}'
GROUP BY o."sku-id", p.product_name, p.safety_stock
HAVING SUM(o.quantity_ordered) > p.safety_stock * 5;