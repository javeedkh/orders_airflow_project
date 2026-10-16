
import sqlite3
from pathlib import Path

DB_FILE = Path(__file__).resolve().parent / "data" / "warehouse.db"


def show_processed_orders():
    with sqlite3.connect(DB_FILE) as conn:
        rows = conn.execute("""
            SELECT order_id, customer_name, amount,
                   order_date, order_month
            FROM fact_orders
            ORDER BY order_id DESC
            LIMIT 10
        """).fetchall()

    print("LATEST PROCESSED WAREHOUSE ROWS:", flush=True)

    for row in reversed(rows):
        print(row, flush=True)
