from datetime import datetime, timedelta
import os
import re
import json
import time
import csv
import threading
import logging
import sqlite3
import requests
import hashlib

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
import undetected_chromedriver as uc

from config import DATA_DIR
from config import DB_PATH
from config import CACHE_DURATION_DAYS
from config import RATE_LIMIT
from config import LOG_FILE
from config import SCRAPE_ATTEMPTS
from config import SIZES


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(LOG_FILE, mode='a'),
                              logging.StreamHandler()])

# Directory and Database Setup Functions
def ensure_dir(directory):
    os.makedirs(directory, exist_ok=True)

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

# Database Utility Functions
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

# Update update_cache to work with new tables
def update_cache(filename, table_name):
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        c.execute(f"REPLACE INTO {table_name} (filename, last_fetched) VALUES (?, ?)", (filename, current_time))
        conn.commit()

# Network Monitoring and Dynamic URL Segment Extraction
def setup_driver():
    caps = DesiredCapabilities.CHROME
    caps['goog:loggingPrefs'] = {'performance': 'ALL'}
    options = uc.ChromeOptions()
    options.headless = True
    return uc.Chrome(desired_capabilities=caps, options=options)

def enable_network_monitoring(driver):
    driver.execute_cdp_cmd("Network.enable", {})
    driver.request_interceptor = lambda request: _request_interceptor(driver, request)

def _request_interceptor(driver, request):
    url = request['request']['url']
    if '_next/data/' in url:
        match = re.search(r'/_next/data/([^/]+)/', url)
        if match:
            driver.execute_script("window.dynamicUrlSegment = arguments[0]", match.group(1))

def extract_dynamic_url_segment(driver):
    driver.get("https://simpletire.com/")
    time.sleep(RATE_LIMIT)
    return _parse_dynamic_url_segment_from_logs(driver)

def _parse_dynamic_url_segment_from_logs(driver):
    logs = driver.get_log("performance")
    for entry in logs:
        log = json.loads(entry["message"])["message"]
        if log["method"] == "Network.responseReceived" and "_next/data/" in log["params"]["response"]["url"]:
            match = re.search(r'/_next/data/([^/]+)/index\.json', log["params"]["response"]["url"])
            if match:
                return match.group(1)
    return None

# Product Link and Detail Extraction
def extract_product_links(json_data):
    product_details = []
    
    top_picks_list = json_data.get('pageProps', {}) \
                              .get('serverData', {}) \
                              .get('siteCatalogSummary', {}) \
                              .get('siteCatalogSummaryTopPicksList', [])

    for item in top_picks_list:
        product = item.get('product', {})
        link_info = product.get('link', {})
        href = link_info.get('href', '')
        brand_label = product.get('brand', {}).get('label', '').lower()

        if href:
            hash_index = href.find('#')
            if hash_index != -1:
                link_fragment = href[hash_index+1:]

                # Using regex to extract the product line
                match = re.search(r'/([^/]+)#', href)
                if match:
                    product_line = match.group(1)
                    product_details.append((link_fragment, brand_label, product_line))

    return product_details

def build_product_details_api_request_url(link_fragment, brand_label, product_line):
    api_url = f"https://simpletire.com/api/product-detail?brand={brand_label}&productLine={product_line}&{link_fragment}"
    return api_url

def get_or_update_url_segment(driver):
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT segment, last_fetched FROM url_segments ORDER BY last_fetched DESC LIMIT 1")
        result = c.fetchone()
        if result and datetime.now() - datetime.strptime(result[1], '%Y-%m-%d %H:%M:%S') < timedelta(days=1):
            return result[0]
        else:
            segment = extract_dynamic_url_segment(driver)
            if segment:
                c.execute("INSERT OR REPLACE INTO url_segments (segment, last_fetched) VALUES (?, ?)", 
                          (segment, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            return segment

def fetch_and_save_size_data(driver, dynamic_url_segment):
    for size in SIZES:
        file_path = os.path.join(DATA_DIR, f"size_data_{size}.json")
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("SELECT data FROM size_data WHERE size = ?", (size,))
            result = c.fetchone()
            if result and is_json_up_to_date(size, 'size_data'):
                logging.info(f"Using cached size data for size {size}")
                continue

            size_url = f"https://simpletire.com/_next/data/{dynamic_url_segment}/tire-sizes/{size}.json"
            try:
                response = requests.get(size_url)
                if response.status_code == 200:
                    json_data = response.json()
                    with open(file_path, 'w') as file:
                        json.dump(json_data, file)
                    c.execute("REPLACE INTO size_data (size, last_fetched, data) VALUES (?, ?, ?)",
                              (size, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), json.dumps(json_data)))
                    logging.info(f"Saved size data for size {size}")
                else:
                    logging.error(f"Failed to fetch size data for size {size}: {response.status_code}")
            except requests.RequestException as e:
                logging.error(f"Request error while fetching size data for size {size}: {e}")

def prepare_product_details_api_request_urls():
    product_details_api_request_urls = []
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        for size in SIZES:
            c.execute("SELECT data FROM size_data WHERE size = ?", (size,))
            result = c.fetchone()
            if result:
                json_data = json.loads(result[0])
                product_details = extract_product_links(json_data)

                for details in product_details:
                    link_fragment, brand_label, product_line = details
                    api_request_url = build_product_details_api_request_url(link_fragment, brand_label, product_line)
                    product_details_api_request_urls.append(api_request_url)
            else:
                logging.error(f"Size data not found in database for size {size}")

    return product_details_api_request_urls

def safe_filename(url):
    """Create a safe and shorter filename from a URL."""
    url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
    return url_hash + '.json'


def fetch_and_save_product_details(url, directory_name):
    ensure_dir(directory_name)
    file_name = safe_filename(url)
    file_path = os.path.join(directory_name, file_name)

    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT data FROM product_details WHERE url = ?", (url,))
        result = c.fetchone()
        if result and is_json_up_to_date(url, 'product_details'):
            logging.info(f"Using cached product details for URL {url}")
            return

        try:
            response = requests.get(url)
            if response.status_code == 200:
                json_data = response.json()
                with open(file_path, 'w') as file:
                    json.dump(json_data, file)
                c.execute("REPLACE INTO product_details (url, last_fetched, data) VALUES (?, ?, ?)",
                          (url, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), json.dumps(json_data)))
                logging.info(f"Saved product details for URL {url}")
            else:
                logging.error(f"Failed to fetch product details for URL {url}: {response.status_code}")
        except requests.RequestException as e:
            logging.error(f"Request error while fetching product details for URL {url}: {e}")

# JSON Processing and CSV Writing

def extract_product_details_data_and_write_to_csv(json_file, csv_file_path):
    logging.info(f"Processing JSON file: {json_file}")
    try:
        with open(json_file, 'r') as file:
            data = json.load(file)
            logging.debug(f"Data loaded from JSON file: {json_file}")
    except Exception as e:
        logging.error(f"Error reading JSON file {json_file}: {e}")
        return

    available_sizes = data.get('siteProductLineAvailableSizeList', [])
    product_line = data.get('siteProductLine', {})
    product_brand = product_line.get('brand', {}).get('label', '')

    write_header = not os.path.exists(csv_file_path) or os.stat(csv_file_path).st_size == 0
    
    try:
        with open(csv_file_path, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            if write_header:
                headers = ['searched_tire_size', 'tire_size', 'brand', 'product_name', 'price', 'model', 'spec_width', 'spec_ratio', 'spec_inflatable_pressure', 'spec_tread_depth', 'spec_width_range', 'spec_sidewall', 'spec_tread_width', 'side_tread_image_url', 'product_link']
                writer.writerow(headers)
                logging.info("CSV headers written.")
            
            for size in available_sizes:
                searched_tire_size = size.get('size', '')
                tire_size = size.get('siteQueryParams', {}).get('tireSize', '')
                brand = product_brand
                product_name = product_line.get('name', '')
                model = size.get('partNumber', '')
                price = float(size.get('priceInCents', 0)) / 100  # Convert cents to dollars
                side_tread_image_url = None
                
                for asset in product_line.get('assetList', []):
                    if asset.get('productImageType') == 'sidetread':
                        side_tread_image_url = asset['image']['src']
                        break
                
                spec_dict = {spec.get('label', ''): spec.get('value', '') for spec in size.get('specList', [])}
                spec_width = spec_dict.get('Width', '')
                spec_ratio = spec_dict.get('Ratio', '')
                spec_inflatable_pressure = spec_dict.get('Inflation Pressure', '')
                spec_tread_depth = spec_dict.get('Tread Depth', '')
                spec_width_range = spec_dict.get('Width Range', '')
                spec_sidewall = spec_dict.get('Sidewall', '')
                spec_tread_width = spec_dict.get('Tread Width', '')
                spec_tread_width = spec_dict.get('Tread Width', '')


                query_params = size.get('siteQueryParams', {})
                mpn = query_params.get('mpn', '')

                product_link_base = f"https://simpletire.com/brands/{brand.lower().replace(' ', '-')}-tires/{product_name.lower().replace(' ', '-')}"
                product_link_params = f"curationPos={query_params.get('curationPos', '')}&curationSeq={query_params.get('curationSeq', '')}&curationSource={query_params.get('curationSource', '')}&mpn={mpn}&pageSource={query_params.get('pageSource', '')}&productPos={query_params.get('productPos', '')}&region={query_params.get('region', '')}&tireSize={tire_size.replace(' ', '-').lower()}"
                product_link = f"{product_link_base}#{product_link_params}"

                row = [searched_tire_size, tire_size, brand, product_name, price, model, spec_width, spec_ratio, spec_inflatable_pressure, spec_tread_depth, spec_width_range, spec_sidewall, spec_tread_width, side_tread_image_url, product_link]
                logging.debug(f"Preparing to write row: {row}")
                writer.writerow(row)
                logging.debug(f"Row written to CSV: {row}")
            logging.info(f"Data from {json_file} written to CSV.")
    except Exception as e:
        logging.error(f"Error writing to CSV file {csv_file_path}: {e}")

# Main Scraping Function
def scrape_and_save_json(links, directory_name, downloaded_files, scraping_completed_flag):
    ensure_dir(directory_name)
    options = uc.ChromeOptions()
    options.headless = True
    dynamic_url_segment = None

    with uc.Chrome(options=options) as driver:
        enable_network_monitoring(driver)
        driver.set_page_load_timeout(10)
        driver.get("https://simpletire.com/")
        time.sleep(RATE_LIMIT)

        counter = 1

        for link in links:
            success = False
            attempts = 0

            while not success and attempts < SCRAPE_ATTEMPTS:
                try:
                    # Check if the link is for the _next/data endpoint
                    if '_next/data/' in link:
                        if dynamic_url_segment is None:
                            dynamic_url_segment = extract_dynamic_url_segment(driver)

                        if dynamic_url_segment is None:
                            logging.error("Failed to extract dynamic URL segment. Retrying...")
                            attempts += 1
                            continue

                        modified_link = link.replace("DYNAMIC_SEGMENT", dynamic_url_segment)
                        response = requests.get(modified_link)
                        if response.status_code == 200:
                            parsed_json = response.json()
                        else:
                            logging.error(f"Error fetching data: {response.status_code}")
                            dynamic_url_segment = None  # Reset segment to trigger re-fetch
                            continue

                    else:
                        driver.get(link)
                        json_response = driver.find_element('tag name', 'pre').text
                        parsed_json = json.loads(json_response)

                    # Save the JSON response
                    file_path = os.path.join(directory_name, f"{counter}.json")
                    with open(file_path, 'w') as output:
                        json.dump(parsed_json, output)
                    downloaded_files.append(file_path)
                    success = True

                except TimeoutException:
                    logging.error(f"Timeout occurred for {link}. Retrying... (Attempt {attempts + 1})")
                    attempts += 1
                
                except (NoSuchElementException, requests.RequestException) as e:
                    logging.error(f"Error occurred for {link}: {e}. Retrying... (Attempt {attempts + 1})")
                    attempts += 1

            counter += 1

    scraping_completed_flag[0] = True
    logging.info("Scraping completed.")

# Processing Downloaded Files
def process_downloaded_files(downloaded_files, csv_file_path, scraping_completed_flag):
    logging.info("Started processing downloaded files.")
    while not scraping_completed_flag[0] or downloaded_files:
        if downloaded_files:
            json_file = downloaded_files.pop(0)
            extract_product_details_data_and_write_to_csv(json_file, csv_file_path)
        else:
            time.sleep(1)
    logging.info("Finished processing all downloaded files.")

# Test Function for Dynamic URL Segment
def test_fetch_dynamic_url_segment():
    driver = setup_driver()
    dynamic_url_segment = extract_dynamic_url_segment(driver)
    
    if dynamic_url_segment:
        logging.info(f"Test Passed: Dynamic URL Segment fetched: {dynamic_url_segment}")
    else:
        logging.error("Test Failed: Dynamic URL Segment not fetched.")
    driver.quit()

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

