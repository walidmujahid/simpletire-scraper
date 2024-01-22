from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import os
import threading
import logging

from config import DATA_DIR
from config import DB_PATH
from config import LOG_FILE
from database import database_file_exists
from database import setup_database
from scraper import setup_driver
from scraper import get_or_update_url_segment
from scraper import fetch_and_save_size_data
from scraper import scrape_and_save_json
from scraper import fetch_and_save_product_details
from scraper import prepare_product_details_api_request_urls
from scraper import process_downloaded_files
from utils import ensure_dir
import logger_config


logger_config.setup_logging(LOG_FILE)

def main():
    if not database_file_exists():
        setup_database()

    ensure_dir(DATA_DIR)

    driver = setup_driver()
    dynamic_url_segment = get_or_update_url_segment(driver)
    driver.quit()

    if dynamic_url_segment:
        fetch_and_save_size_data(driver, dynamic_url_segment)

        current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        json_directory = os.path.join(DATA_DIR, f"product_details_{current_datetime}")
        csv_file_path = f"product_data_{current_datetime}.csv"
        downloaded_files = []
        scraping_completed_flag = [False]

        product_details = prepare_product_details_api_request_urls()

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(fetch_and_save_product_details, url, json_directory) for url in product_details]
            for future in as_completed(futures):
                future.result()  # Blocks until the future is done

        threading.Thread(target=scrape_and_save_json, args=(product_details, json_directory, downloaded_files, scraping_completed_flag)).start()
        threading.Thread(target=process_downloaded_files, args=(downloaded_files, csv_file_path, scraping_completed_flag)).start()
    else:
        logging.error("Failed to extract dynamic URL segment.")

    logging.info("Main thread completed.")

if __name__ == "__main__":
    main()

