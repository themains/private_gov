import scrapy
import json
import pandas as pd
import os
import requests
from urllib.parse import quote
from datetime import datetime

class BlacklightSpider(scrapy.Spider):
    name = "blacklight_spider"
    output_folder = "../../data/cc_json/"
    log_file = "./blacklight_errors.log"
    blacklight_endpoint = 'https://blacklight-us-ca.api.themarkup.org'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Create output directory and ensure log file exists.
        os.makedirs(self.output_folder, exist_ok=True)
        with open(self.log_file, 'a') as f:
            f.write(f"\n--- New Spider Run: {datetime.now()} ---\n")
    
    def normalize_url(self, url: str) -> str:
        """Normalize URL to ensure proper format."""
        url = url.strip().lower()
        if not url:
            return ""
        # Add scheme if missing.
        if not url.startswith(('http://', 'https://')):
            url = f'https://{url}'
        return url.rstrip('/')
    
    def log_error(self, message: str) -> None:
        """Log error message with timestamp to a log file."""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(self.log_file, "a") as f:
            f.write(f"[{timestamp}] {message}\n")
    
    def start_requests(self):
        try:
            df = pd.read_csv("../../data/common_crawl_sample.csv", usecols=["url"])
            websites = df["url"].dropna().drop_duplicates().astype(str).tolist()
            self.logger.info(f"Loaded {len(websites)} unique websites")
            
            for website in websites:
                url = self.normalize_url(website)
                if not url:
                    self.log_error(f"Invalid URL format: {website}")
                    continue
                
                sanitized_name = url.replace("http://", "").replace("https://", "").replace("/", "_")
                output_file = os.path.join(self.output_folder, f"{sanitized_name}.json")
                if os.path.exists(output_file):
                    self.logger.info(f"Skipping {url} - already processed")
                    continue

                # Use GET instead of HEAD to improve reliability.
                yield scrapy.Request(
                    url=url,
                    method='GET',
                    callback=self.check_site_availability,
                    errback=self.try_http_fallback,
                    meta={"original_url": url},
                    dont_filter=True,
                    headers={'User-Agent': 'Mozilla/5.0'}
                )
        except Exception as e:
            self.logger.error(f"Failed to load or process websites: {e}")
            self.log_error(f"Failed to load or process websites: {e}")
    
    def check_site_availability(self, response):
        """Check if the site is available and, if so, request data from the Blacklight API."""
        if 200 <= response.status < 400:
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
        """Attempt HTTP fallback if the initial HTTPS request fails."""
        url = failure.request.meta["original_url"]
        if url.startswith("https://"):
            http_url = "http://" + url[8:]
            yield scrapy.Request(
                url=http_url,
                method='GET',
                callback=self.check_site_availability,
                errback=lambda f: self.log_error(f"Website unreachable: {url}"),
                meta={"original_url": url, "dont_retry": True},
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
            self.logger.error(f"Failed to save data for {website}: {e}")
            self.log_error(f"Failed to save data for {website}: {e}")
    
    def handle_error(self, failure):
        website = failure.request.meta.get("website", "Unknown")
        error_message = f"Request failed for {website}: {failure.value}"
        self.logger.error(error_message)
        self.log_error(error_message)