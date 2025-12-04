#!/usr/bin/env python3
"""
Scraper for DDTC 'Statutorily Debarred Parties' dynamic table.
Outputs CSV: data/raw_ddtc.csv
"""
import argparse
import os
import time
from datetime import date, datetime

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, f"{date.today()}_scraper.log")

def log(msg, level="INFO"):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{level}] {ts} - {msg}"
    print(line)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def scrape(url: str, headless: bool = True, max_pages: int = 200):
    options = Options()
    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")

    # Используем Service для ChromeDriverManager
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.get(url)
    time.sleep(4)

    all_rows = []
    page = 1

    while page <= max_pages:
        log(f"Scraping page {page}")
        try:
            rows = driver.find_elements(By.XPATH, "//table//tr")[1:]  # skip header
        except Exception as e:
            log(f"Cannot find rows: {e}", level="ERROR")
            break

        if not rows:
            log("No rows found on page, breaking.")
            break

        for r in rows:
            try:
                cells = r.find_elements(By.TAG_NAME, "td")
                if not cells:
                    continue
                name = cells[0].text.strip()
                date_of_birth = cells[1].text.strip()
                federal_register_notice = cells[2].text.strip()
                notice_date = cells[3].text.strip()
                corrected_notice = cells[4].text.strip() if len(cells) > 4 else None
                corrected_notice_date = cells[5].text.strip() if len(cells) > 5 else None
                all_rows.append({
                    "name": name,
                    "date_of_birth": date_of_birth,
                    "federal_register_notice": federal_register_notice,
                    "notice_date": notice_date,
                    "corrected_notice": corrected_notice,
                    "corrected_notice_date": corrected_notice_date
                })
            except Exception as e:
                log(f"Row parsing error: {e}", level="ERROR")
                continue

        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, "a[aria-label='Go to next page']")
            if next_btn.get_attribute("aria-disabled") == "true":
                log("Next button disabled -> last page")
                break
            next_btn.click()
            time.sleep(2.5)
            page += 1
        except Exception as e:
            log(f"Failed to click next: {e}. Stopping.", level="ERROR")
            break

    driver.quit()
    df = pd.DataFrame(all_rows)
    os.makedirs("data", exist_ok=True)
    raw_csv = os.path.join("data", "raw_ddtc.csv")
    df.to_csv(raw_csv, index=False)
    log(f"Scraping finished. Rows collected: {len(df)}. Saved to {raw_csv}")
    return raw_csv

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="https://deccspmddtc.servicenowservices.com/ddtc_public?id=ddtc_kb_article_page&sys_id=7188dac6db3cd30044f9ff621f961914")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--max-pages", type=int, default=200)
    args = parser.parse_args()
    scrape(args.url, headless=args.headless, max_pages=args.max_pages)
