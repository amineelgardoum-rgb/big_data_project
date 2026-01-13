from src.data_generation.utils.generate_master_data import load_to_postgres,generate_suppliers, generate_products, generate_warehouses
from src.data_generation.utils.generate_orders import generate_daily_orders
from src.data_generation.utils.generate_stock import generate_stock_snapshot
from src.data_generation.utils.log import log
import os
import psycopg2
from datetime import datetime  # standard library, absolute import is fine

from datetime import datetime 
def generate_data():
    # Generate master data (postgres injection)
    today=datetime.now().strftime('%Y-%m-%d')
    suppliers_df=generate_suppliers(10)
    products_df=generate_products(100,suppliers_df)
    warehouses_df=generate_warehouses(3)
    
    os.makedirs('data/master', exist_ok=True)
    
    suppliers_df.to_csv('data/master/suppliers.csv',index=False)
    products_df.to_csv('data/master/products.csv',index=False)
    warehouses_df.to_csv('data/master/warehouses.csv',index=False)
    log("loading to Postgres Database Master Data....","INFO")
    tables_to_load = {
    "suppliers": suppliers_df,
    "products": products_df,
    "warehouses": warehouses_df
        }

        
    with psycopg2.connect(
    host='127.0.0.1',
    port=5432,
    database='procurement_db',   # ✅ correct database
    user='procurement_user',     # ✅ correct user
    password='procurement_pass'  # ✅ correct password
    ) as conn:
        load_to_postgres(tables_to_load, conn)

    
    
    log("Master data is generated successfully!","SUCCESS")
    # Generate daily orders,snapshot of warehouse (the mount to hdfs)
    generate_daily_orders(today,num_pos=50,orders_per_pos=50)
    log("daily orders data generated successfully!","SUCCESS")
    generate_stock_snapshot(today)
    log("warehouse snapshot generated successfully!","SUCCESS")
    print()
    

    