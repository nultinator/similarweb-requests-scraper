import os
import csv
import requests
import json
import logging
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import concurrent.futures
from dataclasses import dataclass, field, fields, asdict

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]

## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def scrape_search_results(keyword, retries=3):
    url = f"https://www.similarweb.com/top-websites/{keyword['category']}/{keyword['subcategory']}/"
    tries = 0
    success = False
    
    while tries <= retries and not success:
        try:
            response = requests.get(url)
            logger.info(f"Recieved [{response.status_code}] from: {url}")
            if response.status_code != 200:
                raise Exception(f"Failed request, Status Code {response.status_code}")
                
            soup = BeautifulSoup(response.text, "html.parser")            
            rows = soup.find_all("tr", class_="top-table__row")
            
            rank = 1
            for row in rows:
                link_holder = row.find("a", class_="tw-table__compare")
                site_name = link_holder.text
                link = f"https://www.similarweb.com/website/{site_name}/"

                rank_change_holder = row.find("td", class_="top-table__column top-table__column--rank-change")
                rank_change = 0
                up_or_down = rank_change_holder.find("span").get("class")[1]
                if "change--up" in up_or_down:
                    rank_change += int(rank_change_holder.text)
                elif "change--down" in up_or_down:
                    rank_change -= int(rank_change_holder.text)
                
                average_visit = row.find("span", class_="tw-table__avg-visit-duration").text
                pages_per_visit = float(row.find("span", class_="tw-table__pages-per-visit").text)
                bounce_rate = row.find("span", class_="tw-table__bounce-rate").text
                                
                search_data = {
                    "name": site_name,
                    "url": link,
                    "rank": rank,
                    "rank_change": rank_change,
                    "average_visit": average_visit,
                    "pages_per_visit": pages_per_visit,
                    "bounce_rate": bounce_rate
                }
                
                rank+=1                

                print(search_data)
            logger.info(f"Successfully parsed data from: {url}")
            success = True
        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}, retries left {retries-tries}")
            tries+=1
    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")


def start_scrape(keywords, retries=3):
    for keyword in keywords:
        scrape_search_results(keyword, retries=retries)


if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 5

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = [{"category": "arts-and-entertainment", "subcategory": "humor"}, {"category": "arts-and-entertainment", "subcategory": "animation-and-comics"}]
    aggregate_files = []

    ## Job Processes
    filename = "arts-and-entertainment"

    start_scrape(keyword_list, retries=MAX_RETRIES)
    logger.info(f"Crawl complete.")
