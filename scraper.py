from datetime import datetime
from datetime import timedelta
import os
import re
import requests
import json
import time
import sqlite3
import logging

from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
import undetected_chromedriver as uc

from config import DB_PATH
from config import DATA_DIR
from config import RATE_LIMIT
from config import SCRAPE_ATTEMPTS
from config import SIZES
from database import is_json_up_to_date
from csv_handler import extract_product_details_data_and_write_to_csv
from utils import ensure_dir
from utils import safe_filename


logger = logging.getLogger(__name__)

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

