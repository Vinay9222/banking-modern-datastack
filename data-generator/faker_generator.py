import argparse
import logging
import os
import random
import sys
import time
from decimal import ROUND_DOWN, Decimal
from pathlib import Path

from dotenv import load_dotenv

# ---------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("data_generator")

# ---------------------------------------------------------------------
# Environment Configuration
# ---------------------------------------------------------------------
local_env = Path(__file__).parent / ".env"
root_env = Path(__file__).parent.parent / ".env"

if local_env.exists():
    load_dotenv(dotenv_path=local_env)
elif root_env.exists():
    load_dotenv(dotenv_path=root_env)
else:
    load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "banking")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# ---------------------------------------------------------------------
# Generator Parameters
# ---------------------------------------------------------------------
NUM_CUSTOMERS = 10
ACCOUNTS_PER_CUSTOMER = 2
NUM_TRANSACTIONS = 50
MAX_TXN_AMOUNT = 1000.00
CURRENCY = "USD"
INITIAL_BALANCE_MIN = Decimal("10.00")
INITIAL_BALANCE_MAX = Decimal("1000.00")
SLEEP_SECONDS = 2


def random_money(min_val: Decimal, max_val: Decimal) -> Decimal:
    """Generate a rounded monetary Decimal value."""
    val = Decimal(str(random.uniform(float(min_val), float(max_val))))
    return val.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def run_iteration(cur, fake) -> None:
    """Generate a single batch of customers, accounts, and transactions."""
    customers = []
    # 1. Generate customers
    for _ in range(NUM_CUSTOMERS):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.unique.email()

        cur.execute(
            "INSERT INTO customers (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id",
            (first_name, last_name, email),
        )
        customer_id = cur.fetchone()[0]
        customers.append(customer_id)

    # 2. Generate accounts
    accounts = []
    for customer_id in customers:
        for _ in range(ACCOUNTS_PER_CUSTOMER):
            account_type = random.choice(["SAVINGS", "CHECKING"])
            initial_balance = random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX)
            cur.execute(
                "INSERT INTO accounts (customer_id, account_type, balance, currency) "
                "VALUES (%s, %s, %s, %s) RETURNING id",
                (customer_id, account_type, initial_balance, CURRENCY),
            )
            account_id = cur.fetchone()[0]
            accounts.append(account_id)

    # 3. Generate transactions
    txn_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER"]
    for _ in range(NUM_TRANSACTIONS):
        account_id = random.choice(accounts)
        txn_type = random.choice(txn_types)
        amount = round(random.uniform(1, MAX_TXN_AMOUNT), 2)
        related_account = None
        if txn_type == "TRANSFER" and len(accounts) > 1:
            related_account = random.choice([a for a in accounts if a != account_id])

        cur.execute(
            "INSERT INTO transactions (account_id, txn_type, amount, related_account_id, status) "
            "VALUES (%s, %s, %s, %s, 'COMPLETED')",
            (account_id, txn_type, amount, related_account),
        )

    logger.info(
        "Generated %d customers, %d accounts, and %d transactions.",
        len(customers),
        len(accounts),
        NUM_TRANSACTIONS,
    )


def main():
    try:
        from faker import Faker
        import psycopg2
    except ImportError as e:
        logger.error("Missing dependency: %s. Run: pip install -r requirements.txt", e)
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Banking Mock Data Generator for PostgreSQL")
    parser.add_argument("--once", action="store_true", help="Run a single iteration and exit")
    parser.add_argument("--interval", type=int, default=SLEEP_SECONDS, help="Sleep interval in seconds between batches")
    args = parser.parse_args()

    fake = Faker()

    logger.info("Connecting to PostgreSQL at %s:%d/%s...", POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB)
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        conn.autocommit = True
        cur = conn.cursor()
    except Exception as err:
        logger.error("Failed to connect to PostgreSQL: %s", err)
        sys.exit(1)

    logger.info("Connected successfully to PostgreSQL database: %s", POSTGRES_DB)

    try:
        iteration = 0
        while True:
            iteration += 1
            logger.info("--- Batch Iteration %d started ---", iteration)
            run_iteration(cur, fake)
            logger.info("--- Batch Iteration %d finished ---", iteration)

            if args.once:
                break
            time.sleep(args.interval)

    except KeyboardInterrupt:
        logger.info("Interrupted by user. Exiting gracefully...")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()