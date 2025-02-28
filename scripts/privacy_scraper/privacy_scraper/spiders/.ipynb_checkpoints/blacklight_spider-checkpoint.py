import scrapy
import json
import pandas as pd
import os
import requests
from urllib.parse import urlparse
from datetime import datetime
from scrapy.loader import ItemLoader
from privacy_scraper.items import PrivacyScraperItem

class BlacklightSpider(scrapy.Spider):
    name = "blacklight_spider"
    output_folder = "../../../data/in_blacklight_json/"
    log_file = "./blacklight_errors.log"
    blacklight_endpoint = 'https://blacklight-us-ca.api.themarkup.org'

    def __init__(self, *args, **kwargs):
        super(BlacklightSpider, self).__init__(*args, **kwargs)
        # Create output directory and ensure log file exists
        os.makedirs(self.output_folder, exist_ok=True)
        with open(self.log_file, 'a') as f:
            f.write(f"\n--- New Spider Run: {datetime.now()} ---\n")

    def normalize_url(self, url):
        """Normalize URL to ensure proper format."""
        url = url.strip().lower()
        if not url:
            return None
        
        # Add scheme if missing
        if not url.startswith(('http://', 'https://')):
            url = f'https://{url}'
        
        # Remove trailing slash
        return url.rstrip('/')

    def log_error(self, message):
        """Log error with timestamp."""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(self.log_file, "a") as f:
            f.write(f"[{timestamp}] {message}\n")

    def start_requests(self):
        try:
            # Load and prepare websites
            df = pd.read_csv("../../data/in_gov_domain_list.csv", usecols=["url"])
            websites = df["url"].dropna().drop_duplicates().astype(str).tolist()
            
            self.logger.info(f"Loaded {len(websites)} unique websites")
            
            for index, website in enumerate(websites, 1):
                # Normalize URL
                url = self.normalize_url(website)
                if not url:
                    self.log_error(f"Invalid URL format: {website}")
                    continue

                # Check if already processed
                sanitized_name = url.replace("http://", "").replace("https://", "").replace("/", "_")
                output_file = os.path.join(self.output_folder, f"{sanitized_name}.json")
                
                if os.path.exists(output_file):
                    self.logger.info(f"Skipping {url} - already processed")
                    continue

                # First try with HTTPS
                yield scrapy.Request(
                    url=url,
                    method='HEAD',
                    callback=self.check_site_availability,
                    errback=self.try_http_fallback,
                    meta={"original_url": url},
                    dont_filter=True
                )

        except Exception as e:
            self.logger.error(f"Failed to load or process websites: {str(e)}")
            self.log_error(f"Failed to load or process websites: {str(e)}")

    def check_site_availability(self, response):
        """Called when site check succeeds"""
        if 200 <= response.status < 400:
            # Site is available, make Blacklight API request
            data = {"inUrl": response.meta["original_url"]}
            yield scrapy.Request(
                url=self.blacklight_endpoint,
                method="POST",
                body=json.dumps(data),
                headers={'Content-Type': 'application/json'},
                meta={"website": response.meta["original_url"], "retry_count": 0},
                callback=self.parse,
                errback=self.handle_error,
                dont_filter=True
            )
        else:
            self.log_error(f"Website returned status {response.status}: {response.meta['original_url']}")

    def try_http_fallback(self, failure):
        """Try HTTP if HTTPS fails"""
        url = failure.request.meta["original_url"]
        if url.startswith('https://'):
            http_url = 'http://' + url[8:]
            yield scrapy.Request(
                url=http_url,
                method='HEAD',
                callback=self.check_site_availability,
                errback=lambda f: self.log_error(f"Website unreachable: {url}"),
                meta={"original_url": url},
                dont_filter=True
            )
        else:
            self.log_error(f"Website unreachable: {url}")

    def parse(self, response):
        website = response.meta["website"]
        retry_count = response.meta["retry_count"]

        try:
            json_data = json.loads(response.body)
        except json.JSONDecodeError:
            self.logger.error(f"Failed to decode JSON for {website}")
            self.log_error(f"Failed to decode JSON for {website}")
            return

        sanitized_name = website.replace("http://", "").replace("https://", "").replace("/", "_")
        output_file = os.path.join(self.output_folder, f"{sanitized_name}.json")

        # Check for valid data
        groups = json_data.get("groups", [])
        if not groups and retry_count < 3:
            self.logger.warning(f"Empty response for {website}. Retrying ({retry_count + 1}/3)")
            yield scrapy.Request(
                url=self.blacklight_endpoint,
                method="POST",
                body=response.request.body,
                headers=response.request.headers,
                meta={"website": website, "retry_count": retry_count + 1},
                callback=self.parse,
                errback=self.handle_error,
                dont_filter=True
            )
            return

        try:
            with open(output_file, "w") as f:
                json.dump(json_data, f, indent=4)
            self.logger.info(f"Successfully saved data for {website}")
        except Exception as e:
            self.logger.error(f"Failed to save data for {website}: {str(e)}")
            self.log_error(f"Failed to save data for {website}: {str(e)}")

    def handle_error(self, failure):
        website = failure.request.meta["website"]
        error_message = f"Request failed for {website}: {str(failure.value)}"
        self.logger.error(error_message)
        self.log_error(error_message)