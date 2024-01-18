from datetime import datetime
import os
import re
import json
import time
import csv
import threading
import logging

from selenium.common.exceptions import TimeoutException
import undetected_chromedriver as uc

from constants import SIZES


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def ensure_dir(directory):
    os.makedirs(directory, exist_ok=True)


def extract_site_search_result_action_link(json_data):
    links = []
    
    # Loop through each group in the JSON data
    for group in json_data.get('siteSearchResultGroupList', []):
        # Loop through each item in the site search result list
        for item in group.get('siteSearchResultList', []):
            action = item.get('action', {})
            # Check if the action type is 'SiteSearchResultActionLink'
            if action.get('type') == 'SiteSearchResultActionLink':
                link = action.get('link', {}).get('href')
                if link:
                    links.append(link)
    
    return links


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


def prepare_product_details_api_request_urls():
    path = "all_next_data_tire_sizes/"
    product_details_api_request_urls = []

    for file_name in os.listdir(path):
        with open(f"{path}{file_name}", 'r') as data:
            json_data = json.load(data)

        product_details = extract_product_links(json_data)

        # Build API request URLs
        for details in product_details:
            link_fragment, brand_label, product_line = details
            api_request_url = build_product_details_api_request_url(link_fragment, brand_label, product_line)
            
            product_details_api_request_urls.append(api_request_url)
    
    return product_details_api_request_urls


def extract_data_and_write_to_csv(json_file, csv_file_path):
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
                headers = ['tire_size', 'brand', 'product_name', 'price', 'spec_width', 'spec_ratio', 'spec_inflatable_pressure', 'spec_tread_depth', 'spec_width_range', 'spec_sidewall', 'spec_tread_width', 'side_tread_image_url', 'product_link']
                writer.writerow(headers)
                logging.info("CSV headers written.")
            
            for size in available_sizes:
                tire_size = size.get('size', '')
                brand = product_brand
                product_name = product_line.get('name', '')
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

                # Building the product link is a TODO task
                product_link = None #TODO
                
                row = [tire_size, brand, product_name, price, spec_width, spec_ratio, 
                       spec_inflatable_pressure, spec_tread_depth, spec_width_range, 
                       spec_sidewall, spec_tread_width, side_tread_image_url, product_link]
                logging.debug(f"Preparing to write row: {row}")
                writer.writerow(row)
                logging.debug(f"Row written to CSV: {row}")
            logging.info(f"Data from {json_file} written to CSV.")
    except Exception as e:
        logging.error(f"Error writing to CSV file {csv_file_path}: {e}")

def scrape_and_save_json(links, directory_name, downloaded_files, scraping_completed_flag):
    ensure_dir(directory_name)
    options = uc.ChromeOptions()
    options.headless = False
    
    logging.info(f"Starting scraping. Total links: {len(links)}")

    with uc.Chrome(use_subprocess=True, options=options) as driver:
        driver.set_page_load_timeout(10)
        driver.get("https://simpletire.com/")
        time.sleep(2)

        counter = 1
        
        for link in links:
            attempt = 0
            success = False

            while attempt < 3 and not success:
                try:
                    driver.get(link)
                    json_response = driver.find_element('tag name', 'pre').text
                    parsed_json = json.loads(json_response)
                    file_path = os.path.join(directory_name, f"{counter}.json")
                    
                    with open(file_path, 'w') as output:
                        json.dump(parsed_json, output)

                    downloaded_files.append(file_path)
                    success = True

                except TimeoutException:
                    print(f"Timeout occurred for {link}. Retrying... (Attempt {attempt + 1})")
                    attempt += 1

            counter += 1

    scraping_completed_flag[0] = True
    logging.info("Scraping completed.")


def process_downloaded_files(downloaded_files, csv_file_path, scraping_completed_flag):
    logging.info("Started processing downloaded files.")
    while not scraping_completed_flag[0] or downloaded_files:
        if downloaded_files:
            json_file = downloaded_files.pop(0)
            extract_data_and_write_to_csv(json_file, csv_file_path)
        else:
            time.sleep(1)
    logging.info("Finished processing all downloaded files.")

if __name__ == "__main__":
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    json_directory = f"product_details/{current_datetime}"
    csv_file_path = f"product_data_{current_datetime}.csv"
    downloaded_files = []
    scraping_completed_flag = [False]

    product_details = prepare_product_details_api_request_urls()
    
    threading.Thread(target=scrape_and_save_json, args=(product_details, json_directory, downloaded_files, scraping_completed_flag)).start()
    threading.Thread(target=process_downloaded_files, args=(downloaded_files, csv_file_path, scraping_completed_flag)).start()
    
    logging.info("Main thread completed.")

    '''
    while True:
        if downloaded_files:
            json_file = downloaded_files.pop(0)
            extract_data_and_write_to_csv(json_file, csv_file_path)
        elif scraping_completed_flag[0]:
            while downloaded_files:
                json_file = downloaded_files.pop(0)
                extract_data_and_write_to_csv(json_file, csv_file_path)
            break
        else:
            time.sleep(1)

    print("Scraping and processing completed.") 
    '''
       
    #current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    #tire_sizes_dir = f"tire-sizes/{current_datetime}"
    #get_width_ratios_dir = f"get_width_ratios/{current_datetime}"
    #width_ratios_dir = f"width_ratios/{current_datetime}"
    #products_tire_size_classic_dir = f"products_tire_size_classic/{current_datetime}"
    #product_details_dir = f"product_details/{current_datetime}"
    
    #ensure_dir(tire_sizes_dir)
    #scrape_and_save_json(TIRE_SIZES, tire_sizes_dir)

    #ensure_dir(width_ratios_dir)
    #scrape_and_save_json(WIDTH_RATIOS, width_ratios_dir)
    
    #product_details = prepare_product_details_api_request_urls()
    
    #ensure_dir(product_details_dir)
    #scrape_and_save_json(product_details, product_details_dir)

    """
    links = []
    for size in SIZES:
        links.append(f"https://simpletire.com/api/products-tire-size-classic?size={size}")
    
    ensure_dir(products_tire_size_classic_dir)
    scrape_and_save_json(links[789:], products_tire_size_classic_dir)
    
    ----------
    
    path = "get_width_ratios/2024-01-17_09-03-18/"
    link_list = []
    for file_name in os.listdir(path):
        with open(f"{path}{file_name}", 'r') as data:
            json_data = json.load(data)
            links = extract_site_search_result_action_link(json_data)

            for link in links:
                link_list.append(f"https://simpletire.com/_next/data/RTEDKvXVj3OdPLYNmqGii{link}.json")
    
    print(len(link_list))
    """

