from datetime import datetime
from datetime import timedelta
import os
import sqlite3
import logging

from config import DB_PATH
from config import CACHE_DURATION_DAYS


logger = logging.getLogger(__name__)
def database_file_exists():
    db_exists = os.path.exists(DB_PATH)
    logging.info(f"Database file {'exists' if db_exists else 'does not exist'} at {DB_PATH}")
    return db_exists

def setup_database():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS size_data (size TEXT PRIMARY KEY, last_fetched TIMESTAMP, data TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS product_details (url TEXT PRIMARY KEY, last_fetched TIMESTAMP, data TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS url_segments (segment TEXT PRIMARY KEY, last_fetched TIMESTAMP)''')
            # Adding indexes
            c.execute('''CREATE INDEX IF NOT EXISTS idx_size_data ON size_data (last_fetched)''')
            c.execute('''CREATE INDEX IF NOT EXISTS idx_product_details ON product_details (last_fetched)''')
            conn.commit()
        logging.info("Database setup completed successfully.")
    except Exception as e:
        logging.error(f"Error setting up database: {e}")

def is_json_up_to_date(identifier, table_name):
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        if table_name == 'size_data':
            c.execute("SELECT last_fetched FROM size_data WHERE size = ?", (identifier,))
        elif table_name == 'product_details':
            c.execute("SELECT last_fetched FROM product_details WHERE url = ?", (identifier,))
        else:
            logging.error("Invalid table name provided to is_json_up_to_date function.")
            return False
        result = c.fetchone()
    if result:
        last_fetched = datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S')
        return datetime.now() - last_fetched < timedelta(days=CACHE_DURATION_DAYS)
    return False

def update_cache(filename, table_name):
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        c.execute(f"REPLACE INTO {table_name} (filename, last_fetched) VALUES (?, ?)", (filename, current_time))
        conn.commit()

