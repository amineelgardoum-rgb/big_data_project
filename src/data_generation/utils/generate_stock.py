import pandas as pd
import random
from datetime import datetime
import os
from .log import log

def generate_stock_snapshot(date_str):
    """Generate end-of-day stock snapshot for all warehouses
    Args:
        date_str:Date in format 'YYYY-MM-DD'
    """
    products_df=pd.read_csv('./data/master/products.csv')
    warehouses_df=pd.read_csv('./data/master/warehouses.csv')
     
    stock_records=[]
    for _,warehouse in warehouses_df.iterrows():
        for _,product in products_df.iterrows():
            stock_records.append({
                'warehouse_id':warehouse['warehouse_id'],
                'sku_id':product['sku_id'],
                'available_quantity':random.randint(0,500),
                'reserved_quantity':random.randint(0,50),
                'stock_date':date_str                
            })
    stock_df=pd.DataFrame(stock_records)
    
    output_dir=f'./data/raw/stock/stock_date={date_str}'
    os.makedirs(output_dir,exist_ok=True)
    
    output_file=f'{output_dir}/warehouse_snapshot.csv'
    stock_df.to_csv(output_file,index=False)
    
    log(f"Stock snapshot for {date_str} generated:{len(stock_df)} records","SUCCESS")
