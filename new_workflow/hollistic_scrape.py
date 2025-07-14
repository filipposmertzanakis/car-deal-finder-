import time
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from supabase import Client, create_client
import sys
import re

from dotenv import load_dotenv
import os
load_dotenv()

# --- Supabase setup ---
***REMOVED*** = os.getenv("***REMOVED***")
***REMOVED*** = os.getenv("***REMOVED***")
supabase = create_client(***REMOVED***, ***REMOVED***)
supabase: Client = create_client(***REMOVED***, ***REMOVED***)

# Car models configuration for mapping URLs to model names
CAR_MODELS = {
    "https://www.car.gr/used-cars/toyota/yaris.html": {"supabase_model": "Yaris"},
    "https://www.car.gr/used-cars/opel/corsa.html": {"supabase_model": "Corsa"},
    "https://www.car.gr/used-cars/suzuki/swift.html": {"supabase_model": "Swift"}
}

# Selenium config
def get_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/115.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=options)

# Fetch existing source_ids to avoid duplicates
def get_existing_source_ids():
    response = supabase.table("listings").select("source_id").execute()
    return set(item["source_id"] for item in response.data)

# Get total pages from pagination
def get_total_pages(driver, url):
    print("[INFO] Determining total number of pages...")
    driver.get(url)
    time.sleep(1)  # Rate limiting
    try:
        LAST_PAGE_LOCATOR = (By.XPATH, "//button[span[text()='…']]/following-sibling::a")
        last_page_element = WebDriverWait(driver, 10).until(EC.visibility_of_element_located(LAST_PAGE_LOCATOR))
        return int(last_page_element.text.strip())
    except TimeoutException:
        print("[INFO] '...' button not found. Trying fallback method.")
    try:
        PAGINATION_NAV_LOCATOR = (By.TAG_NAME, "nav")
        pagination_container = WebDriverWait(driver, 5).until(EC.visibility_of_element_located(PAGINATION_NAV_LOCATOR))
        all_links = pagination_container.find_elements(By.TAG_NAME, "a")
        page_numbers = [int(link.text.strip()) for link in all_links if link.text.strip().isdigit()]
        return max(page_numbers) if page_numbers else 1
    except TimeoutException:
        print("[WARN] No pagination found at all. Assuming 1 page.")
        return 1

# Parse listings from car.gr page
def get_listings_from_page(driver, url, supabase_model):
    driver.get(url)
    time.sleep(3)  # Rate limiting
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    listings = []

    listing_items = soup.select('div[index]')
    for listing in listing_items:
        try:
            title_tag = listing.select_one('h3')
            if not title_tag:
                continue

            title_text = title_tag.get_text(separator=' ', strip=True)
            parts = title_text.split()
            make = parts[0] if len(parts) > 0 else None
            model = parts[1] if len(parts) > 1 else supabase_model  # Fallback to supabase_model
            year = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None

            link_tag = listing.select_one('a.row-anchor')
            if not link_tag or not link_tag.has_attr('href'):
                continue

            relative_url = link_tag['href']
            full_url = f"https://www.car.gr{relative_url}"
            source_id = relative_url.split('/')[-1].split('-')[0]

            price_tag = listing.select_one('span.lg\\:tw-text-3xl span')
            price_raw = price_tag.get_text(strip=True).replace('.', '').replace('€', '') if price_tag else '0'
            price = float(price_raw) if price_raw.replace(',', '').isdigit() else None

            subtitle_tag = listing.select_one('h3 + p')
            description = subtitle_tag.get_text(strip=True) if subtitle_tag else ""

            def extract_spec(attr):
                tag = listing.select_one(f'div[title="{attr}"] p')
                return tag.get_text(strip=True) if tag else None

            mileage_raw = extract_spec("Χιλιόμετρα")
            mileage = int(mileage_raw.replace('.', '').replace('Km', '').strip()) if mileage_raw else None

            img_tag = listing.select_one('img')
            image_url = img_tag['src'] if img_tag and img_tag.has_attr('src') else None

            if not all([source_id, make, model, year, mileage, price]):
                print(f"[WARN] Skipping listing with missing core data: {full_url}")
                continue

            listings.append({
                'source_id': source_id,
                'make': make,
                'model': model,
                'year': year,
                'mileage': mileage,
                'price': price,
                'url': full_url,
                'image_url': image_url,
                'description': description,
                'timestamp': datetime.utcnow().isoformat()
            })

        except Exception as e:
            print(f"[!] Error parsing listing: {e}")
            continue

    return listings

# Save to Supabase with duplicate check
def save_to_supabase(listings, existing_ids):
    new_listings = 0
    for entry in listings:
        if entry['source_id'] in existing_ids:
            print(f"[INFO] Skipping duplicate listing: {entry['source_id']}")
            continue
        try:
            supabase.table("listings").insert(entry).execute()
            print(f"[✓] Saved: {entry['source_id']}")
            existing_ids.add(entry['source_id'])
            new_listings += 1
        except Exception as e:
            print(f"[x] Failed to save {entry['source_id']}: {e}")
    return new_listings

# Main script
if __name__ == "__main__":
    # Get URL from command-line argument or use default
    default_url = "https://www.car.gr/used-cars/toyota/yaris.html?category=15001&crashed=f&location2=3&make=13301&model=14938&offer_type=sale&pg={}&sort=cr"
    base_url = sys.argv[1] if len(sys.argv) > 1 else default_url

    # Extract supabase_model from URL
    supabase_model = None
    for url_prefix, config in CAR_MODELS.items():
        if base_url.startswith(url_prefix):
            supabase_model = config['supabase_model']
            break
    if not supabase_model:
        # Fallback: Extract model from URL path
        match = re.search(r'used-cars/[^/]+/([^.]+)\.html', base_url)
        supabase_model = match.group(1).capitalize() if match else "Unknown"

    print(f"[INFO] Scraping listings for model: {supabase_model}")

    driver = get_driver()
    existing_ids = get_existing_source_ids()

    total_pages = get_total_pages(driver, base_url.format(1))
    print(f"[INFO] Total pages to scrape: {total_pages}")
    print("[INFO] Starting scrape...")

    total_new_listings = 0
    for page in range(1, total_pages):  # Limit to 5 pages for speed
        print(f"\n[INFO] Scraping page {page}...")
        page_url = base_url.format(page)
        listings = get_listings_from_page(driver, page_url, supabase_model)
        print(f"[INFO] Found {len(listings)} listings.")
        new_listings = save_to_supabase(listings, existing_ids)
        total_new_listings += new_listings

    driver.quit()
    print(f"[INFO] Scrape completed. Saved {total_new_listings} new listings.")