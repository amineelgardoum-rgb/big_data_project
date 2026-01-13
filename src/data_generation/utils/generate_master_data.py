from faker import Faker
import pandas as pd
from .log import log 
# import psycopg2
import random 

fake=Faker()

def generate_suppliers(n=10):
    """Generate supplier data"""
    suppliers=[]
    for i in range(1,n+1):
        suppliers.append({
            'supplier_id':f'SUP_{i:03d}',
            'supplier_name':fake.company(),
            'contact_email':fake.company_email(),
            'lead_time_days':random.randint(1,7)
        })
        
    return pd.DataFrame(suppliers)

def generate_products(n=100,suppliers_df=None):
    """Generate product catalog"""
    categories=['Fruits','Vegetables','Dairy','Meat','Bakery','Beverages','Snacks','Frozen','Canned','Personal Care']
    products=[]
    if suppliers_df is None:
        raise ValueError("suppliers_df must be provided.")
    for i in range(1,n+1):
        products.append({
            'sku_id':f'SKU_{i:04d}',
            'product_name':f"{fake.word().capitalize()} {fake.word()}",
            'category':random.choice(categories),
            'supplier_id':random.choice(suppliers_df['supplier_id'].tolist()),
            'pack_size':random.choice([1,6,12,24]),
            'min_order_qty':random.choice([12,24,48]),
            'unit_price':round(random.uniform(0.5,50.0),2),
            'safety_stock':random.randint(10,100)
        })
    return pd.DataFrame(products)

def generate_warehouses(n=3):
    """Generate warehouse data"""
    warehouses=[]
    for i in range(1,n+1):
        warehouses.append({
            'warehouse_id':f'WH_{i:02d}',
            'warehouse_name':f'Warehouse {fake.city()}',
            'location':f'{fake.city()},{fake.country()}'
        })
    return pd.DataFrame(warehouses)

def check_table_data(conn, table_name):
    """Check if table has data"""
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    cursor.close()
    return count > 0

def clear_table(conn, table_name):
    """Clear all data from table"""
    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE")
    conn.commit()
    cursor.close()

def load_to_postgres(tables_dict, conn):
    """Load dataframe to PostgreSQL with ON CONFLICT handling"""
    cursor = conn.cursor()
    
    try:
        for table_name, df in tables_dict.items():
            if df.empty:
                log(f"Skipping empty table: {table_name}", "WARNING")
                continue
            
            # Check if data exists
            if check_table_data(conn, table_name):
                log(f"Table '{table_name}' already has data. Clearing and reloading...", "INFO")
                clear_table(conn, table_name)
            
            log(f"Loading {len(df)} rows into {table_name}...", "INFO")
            
            for _, row in df.iterrows():
                columns = ','.join(row.index)
                placeholders = ','.join(['%s'] * len(row))
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                cursor.execute(sql, tuple(row.values))
            
            log(f" Loaded {len(df)} rows into {table_name}", "SUCCESS")
        
        conn.commit()
        log("All data committed to PostgreSQL", "SUCCESS")
        
    except Exception as e:
        conn.rollback()
        log(f"Error loading to PostgreSQL: {e}", "ERROR")
        raise
    finally:
        cursor.close()

