
"""
Synthetic e-commerce traffic generator.
Simulates continuous inserts/updates so Debezium (Phase 2) has real
change events to capture.
"""
import argparse
import random
import time
from datetime import datetime

import psycopg2
from faker import Faker

fake = Faker()

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "ecommerce",
    "user": "ecom_user",
    "password": "ecom_pass",
}

CATEGORIES = ["Electronics", "Home & Kitchen", "Clothing", "Books", "Sports", "Beauty"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "wallet"]
ORDER_STATUSES = ["PENDING", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"]


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def seed_products(cur, n=200):
    for _ in range(n):
        cur.execute(
            """INSERT INTO products (product_name, category, price, stock_quantity)
               VALUES (%s, %s, %s, %s)""",
            (
                fake.unique.catch_phrase(),
                random.choice(CATEGORIES),
                round(random.uniform(5, 500), 2),
                random.randint(0, 1000),
            ),
        )


def seed_customers(cur, n=500):
    for _ in range(n):
        cur.execute(
            """INSERT INTO customers (full_name, email, country)
               VALUES (%s, %s, %s)""",
            (fake.name(), fake.unique.email(), fake.country()),
        )


def create_order(cur):
    cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1;")
    customer_id = cur.fetchone()[0]

    cur.execute(
        """INSERT INTO orders (customer_id, order_status)
           VALUES (%s, 'PENDING') RETURNING order_id""",
        (customer_id,),
    )
    order_id = cur.fetchone()[0]

    cur.execute("SELECT product_id, price FROM products ORDER BY random() LIMIT %s;",
                (random.randint(1, 4),))
    items = cur.fetchall()

    total = 0
    for product_id, price in items:
        qty = random.randint(1, 3)
        total += float(price) * qty
        cur.execute(
            """INSERT INTO order_items (order_id, product_id, quantity, unit_price)
               VALUES (%s, %s, %s, %s)""",
            (order_id, product_id, qty, price),
        )

    cur.execute(
        """INSERT INTO payments (order_id, payment_status, payment_method, amount)
           VALUES (%s, 'INITIATED', %s, %s)""",
        (order_id, random.choice(PAYMENT_METHODS), round(total, 2)),
    )
    return order_id


def progress_random_order(cur):
    cur.execute(
        "SELECT order_id, order_status FROM orders ORDER BY random() LIMIT 1;"
    )
    row = cur.fetchone()
    if not row:
        return
    order_id, status = row

    idx = ORDER_STATUSES.index(status)
    if idx < len(ORDER_STATUSES) - 1 and random.random() < 0.7:
        new_status = ORDER_STATUSES[idx + 1]
        cur.execute(
            "UPDATE orders SET order_status=%s, updated_at=now() WHERE order_id=%s",
            (new_status, order_id),
        )
        if new_status == "PAID":
            cur.execute(
                """UPDATE payments SET payment_status='SUCCESS', paid_at=now(), updated_at=now()
                   WHERE order_id=%s""",
                (order_id,),
            )


def seed(n_customers=500, n_products=200, n_orders=1000):
    conn = get_conn()
    conn.autocommit = False
    with conn:
        with conn.cursor() as cur:
            print(f"[{datetime.now()}] Seeding {n_customers} customers...")
            seed_customers(cur, n_customers)
            print(f"[{datetime.now()}] Seeding {n_products} products...")
            seed_products(cur, n_products)
            print(f"[{datetime.now()}] Creating {n_orders} orders...")
            for _ in range(n_orders):
                create_order(cur)
    conn.close()
    print("Seed complete.")


def stream(interval_seconds=2):
    conn = get_conn()
    conn.autocommit = True
    print("Starting live traffic simulation. Ctrl+C to stop.")
    try:
        while True:
            with conn.cursor() as cur:
                action = random.choices(
                    ["new_order", "progress_order"], weights=[0.4, 0.6]
                )[0]
                if action == "new_order":
                    oid = create_order(cur)
                    print(f"[{datetime.now()}] New order #{oid}")
                else:
                    progress_random_order(cur)
                    print(f"[{datetime.now()}] Progressed a random order")
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["seed", "stream"], required=True)
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args()

    if args.mode == "seed":
        seed()
    else:
        stream(args.interval)
