import csv
import logging
import sqlite3
import uuid

from datetime import datetime, timezone, date
from pathlib import Path

# Project paths
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
SOURCE_FILE = DATA_DIR / "orders.csv"
DB_FILE = DATA_DIR / "warehouse.db"
LOG_FILE = DATA_DIR / "pipeline.log"

DATA_DIR.mkdir(parents=True, exist_ok=True)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ],
)

logger = logging.getLogger("orders_pipeline")


def connect_db():
    """Connect to the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def create_tables(conn):
    """Create tables required by the pipeline."""

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS raw_orders (
            order_id INTEGER PRIMARY KEY,
            customer_name TEXT NOT NULL,
            amount REAL NOT NULL,
            order_date TEXT NOT NULL,
            ingested_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS fact_orders (
            order_id INTEGER PRIMARY KEY,
            customer_name TEXT NOT NULL,
            amount REAL NOT NULL,
            order_date TEXT NOT NULL,
            order_month TEXT NOT NULL,
            loaded_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS rejected_orders (
            reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            order_id TEXT,
            reason TEXT NOT NULL,
            rejected_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pipeline_audit (
            run_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            rows_read INTEGER DEFAULT 0,
            rows_loaded INTEGER DEFAULT 0,
            rows_rejected INTEGER DEFAULT 0,
            error_message TEXT
        );

        CREATE TABLE IF NOT EXISTS pipeline_state (
            state_key TEXT PRIMARY KEY,
            state_value TEXT NOT NULL
        );
    """)

    conn.execute("""
        INSERT OR IGNORE INTO pipeline_state
        (state_key, state_value)
        VALUES ('last_order_id', '0')
    """)

    conn.commit()


def validate_row(row):
    """Validate one source record."""

    try:
        order_id = int(row["order_id"])

        if order_id <= 0:
            return None, "Order ID must be positive"

    except (ValueError, TypeError):
        return None, "Invalid order ID"

    customer_name = row.get("customer_name", "").strip()

    if not customer_name:
        return None, "Customer name is missing"

    try:
        amount = float(row["amount"])

        if amount <= 0:
            return None, "Amount must be greater than zero"

    except (ValueError, TypeError):
        return None, "Invalid amount"

    order_date = row.get("order_date", "").strip()

    try:
        date.fromisoformat(order_date)
    except ValueError:
        return None, "Invalid order date"

    clean_row = {
        "order_id": order_id,
        "customer_name": customer_name,
        "amount": amount,
        "order_date": order_date
    }

    return clean_row, None


def run_pipeline():
    """Execute one incremental ETL/ELT pipeline run."""

    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()

    rows_read = 0
    rows_loaded = 0
    rows_rejected = 0

    conn = connect_db()
    create_tables(conn)

    conn.execute("""
        INSERT INTO pipeline_audit
        (run_id, started_at, status)
        VALUES (?, ?, 'RUNNING')
    """, (run_id, started_at))

    conn.commit()

    try:
        last_id = int(conn.execute("""
            SELECT state_value
            FROM pipeline_state
            WHERE state_key = 'last_order_id'
        """).fetchone()["state_value"])

        max_seen_id = last_id
        now = datetime.now(timezone.utc).isoformat()

        # ETL: Extract source records.
        with open(
            SOURCE_FILE,
            "r",
            newline="",
            encoding="utf-8-sig"
        ) as file:

            reader = csv.DictReader(file)

            required = {
                "order_id",
                "customer_name",
                "amount",
                "order_date"
            }

            if not reader.fieldnames or not required.issubset(
                set(reader.fieldnames)
            ):
                raise ValueError("Required CSV columns are missing")

            for row in reader:

                # Incremental loading: skip IDs already processed.
                raw_id = row.get("order_id", "").strip()

                try:
                    candidate_id = int(raw_id)
                except ValueError:
                    candidate_id = None

                if candidate_id is not None and candidate_id <= last_id:
                    continue

                rows_read += 1

                if candidate_id is not None:
                    max_seen_id = max(max_seen_id, candidate_id)

                # ETL: Validate and clean.
                clean, error = validate_row(row)

                if error:
                    rows_rejected += 1

                    conn.execute("""
                        INSERT INTO rejected_orders
                        (run_id, order_id, reason, rejected_at)
                        VALUES (?, ?, ?, ?)
                    """, (
                        run_id,
                        raw_id or None,
                        error,
                        now
                    ))

                    logger.warning(
                        "Rejected order %s: %s",
                        raw_id or "UNKNOWN",
                        error
                    )
                    continue

                # Load valid records into the raw table.
                conn.execute("""
                    INSERT OR IGNORE INTO raw_orders
                    (order_id, customer_name, amount,
                     order_date, ingested_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    clean["order_id"],
                    clean["customer_name"],
                    clean["amount"],
                    clean["order_date"],
                    now
                ))

        # ELT: Transform loaded data using SQL.
        cursor = conn.execute("""
            INSERT OR IGNORE INTO fact_orders
            (order_id, customer_name, amount,
             order_date, order_month, loaded_at)

            SELECT
                order_id,
                customer_name,
                amount,
                order_date,
                substr(order_date, 1, 7),
                ?
            FROM raw_orders
            WHERE order_id NOT IN (
                SELECT order_id FROM fact_orders
            )
        """, (datetime.now(timezone.utc).isoformat(),))

        rows_loaded = max(cursor.rowcount, 0)

        # Save the high-water mark.
        conn.execute("""
            UPDATE pipeline_state
            SET state_value = ?
            WHERE state_key = 'last_order_id'
        """, (str(max_seen_id),))

        finished_at = datetime.now(timezone.utc).isoformat()

        # Audit successful execution.
        conn.execute("""
            UPDATE pipeline_audit
            SET finished_at = ?,
                status = 'SUCCESS',
                rows_read = ?,
                rows_loaded = ?,
                rows_rejected = ?
            WHERE run_id = ?
        """, (
            finished_at,
            rows_read,
            rows_loaded,
            rows_rejected,
            run_id
        ))

        conn.commit()

        logger.info("Pipeline completed successfully")

        return {
            "run_id": run_id,
            "status": "SUCCESS",
            "rows_read": rows_read,
            "rows_loaded": rows_loaded,
            "rows_rejected": rows_rejected
        }

    except Exception as error:
        conn.rollback()

        logger.exception("Pipeline failed")

        conn.execute("""
            UPDATE pipeline_audit
            SET finished_at = ?,
                status = 'FAILED',
                rows_read = ?,
                rows_loaded = ?,
                rows_rejected = ?,
                error_message = ?
            WHERE run_id = ?
        """, (
            datetime.now(timezone.utc).isoformat(),
            rows_read,
            rows_loaded,
            rows_rejected,
            str(error),
            run_id
        ))

        conn.commit()
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    print(run_pipeline())