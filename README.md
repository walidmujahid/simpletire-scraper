# Scraping Tires from SimpleTire.com

## Overview
This Python script is designed to scrape tire size data and product details from a https://simpletire.com/. It utilizes web scraping techniques to extract data dynamically, handling pagination, and network traffic monitoring to retrieve necessary data segments. The data is then saved locally in JSON format and can be processed further into a CSV file.

## Features
- **Dynamic Data Extraction**: Retrieves tire size data and product details dynamically.
- **Caching Mechanism**: Utilizes SQLite database to cache data, reducing unnecessary network calls.
- **Cache Duration Configuration**: Ability to specify cache duration for data freshness.
- **Concurrent Processing**: Uses threading and concurrent futures for efficient data fetching and processing.
- **Error Handling and Logging**: Implements robust error handling and logs important events and errors for troubleshooting.

## Dependencies
To run this script, you will need Python installed on your machine, along with the following dependencies:

```
pip install requests selenium undetected-chromedriver
```

## Configuration
Before running the script, ensure to configure the following:
- **DATA_DIR**: Directory where JSON data will be stored.
- **DB_PATH**: Path to the SQLite database file for caching.
- **CACHE_DURATION_DAYS**: Duration in days to determine when to refresh the cache.
- **SCRAPE_ATTEMPTS**: Number of attempts scraper will try to scrape a URL. Default: 3

## Usage

```
python main.py
```
The script will scrape data, handle pagination, and store the results in the specified data directory. Cached data will be used when available and not outdated.

## Logging
The script logs its progress and any errors encountered. This information can be useful for debugging purposes and understanding the script's flow.

## Project Background
This script was originally developed as a custom solution for a client, Syed Faiq Yazdani, on Upwork. It was tailored to meet specific requirements for scraping generating a csv from the product data. Following the successful completion of the project, it has been added to my portfolio to showcase my skills in web scraping, data processing, and automation.

This inclusion serves as a demonstration of my ability to handle complex scraping tasks, implement caching mechanisms, determine the best way to collect relevant data, and efficiently process and store large amounts of data. It highlights my expertise in Python programming, especially in web scraping and data manipulation.

Please note that while this script is a part of my professional portfolio, it should be used responsibly and ethically, adhering to the legal considerations and terms of service of the target website.

## Updates & Contributions
I may come back from time to time to make this better by fixing an issue or adding a new feature. Feel free to open an issue if you come accross any. Anyone is welcome to make pull requests.
