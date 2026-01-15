from faker import Faker
import pandas as pd
import json
import random
import os
from .log import log
from .date import DATE
seed=int(DATE)

fake=Faker()
Faker.seed(seed)



def generate_daily_orders(date_str, num_pos, orders_per_pos=100):
    """
    Generates orders for multiple POS systems for a given date
    Args:
        date_str:Date in Format 'YY-MM-DD'
        num_pos:Number of POS systems (stores)
        orders_per_pos:Average orders per POS
    """
    products_df = pd.read_csv("data/master/products.csv")
    sku_list = products_df["sku_id"].tolist()

    output_dir = f"./data/raw/orders/orders_date={date_str}"
    os.makedirs(output_dir, exist_ok=True)

    for pos_num in range(1, num_pos + 1):
        orders = []
        output_file = os.path.join(output_dir, f"pos_{pos_num:03d}.jsonl")
        with open(output_file, "w") as f:
            num_orders = random.randint(orders_per_pos - 10, orders_per_pos + 10)
            for order_num in range(num_orders):
                num_items = random.randint(1, 5)
                for _ in range(num_items):
                    order = {
                        "order_id": f"ORD_{date_str}_{pos_num:03d}_{order_num:04d}",
                        "sku-id": random.choice(sku_list),
                        "quantity_ordered": random.randint(1, 10),
                        "order_date":date_str
                    }
                    orders.append(order)
                    f.write(json.dumps(order) + "\n")
                    
        log(f"Generated {len(orders)} order items for POS_{pos_num:03d}", "SUCCESS")

    log(f"All orders for {date_str} generated successfully!", "SUCCESS")
