import os
import json
import csv
import logging

from config import SIZES


logger = logging.getLogger(__name__)

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

