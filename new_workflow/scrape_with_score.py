import time
from datetime import datetime
from io import BytesIO

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from supabase import Client, create_client

from dotenv import load_dotenv
import os
load_dotenv()

# --- Supabase setup ---
***REMOVED*** = os.getenv("***REMOVED***").strip()
***REMOVED*** = os.getenv("***REMOVED***").strip()
supabase: Client = create_client(***REMOVED***, ***REMOVED***)

# Car models configuration
CAR_MODELS = [
    {
        "make": "Toyota",
        "model": "Yaris",
        "url_template": "https://www.car.gr/used-cars/toyota/yaris.html?category=15001&crashed=f&location2=3&make=13301&model=14938&offer_type=sale&pg={}&sort=cr",
        "supabase_model": "Yaris",
        "stats_table": "yaris_price_stats_by_mileage"
    },
    {
        "make": "Opel",
        "model": "Corsa",
        "url_template": "https://www.car.gr/used-cars/opel/corsa.html?category=15001&crashed=f&location2=3&make=13334&model=14744&offer_type=sale&pg={}&sort=cr",
        "supabase_model": "Corsa",
        "stats_table": "corsa_price_stats_by_mileage"
    },
    {
        "make": "Suzuki",
        "model": "Swift",
        "url_template": "https://www.car.gr/used-cars/suzuki/swift.html?category=15001&crashed=f&location2=3&make=12858&model=14903&offer_type=sale&pg={}&sort=cr",
        "supabase_model": "Swift",
        "stats_table": "swift_price_stats_by_mileage"
    },
    {
        "make": "Renault",
        "model": "Clio",
        "url_template": "https://www.car.gr/used-cars/renault/clio.html?activeq=Renault+Clio&category=15001&crashed=f&from_suggester=1&location2=3&make=13151&model=14844&offer_type=sale&pg={}",
        "supabase_model": "Clio",
        "stats_table": "clio_price_stats_by_mileage"
    },
    {
        "make": "Peugeot",
        "model": "208",
        "url_template": "https://www.car.gr/used-cars/peugeot/208.html?activeq=peugot%20208&category=15001&crashed=f&from_suggester=1&location2=3&make=13198&model=15403&offer_type=sale&pg={}",
        "supabase_model": "208",
        "stats_table": "208_price_stats_by_mileage"
    }
    , {
        "make": "Hyundai",
        "model": "i10",
        "url_template": "https://www.car.gr/used-cars/hyundai/i10.html?activeq=Hyundai+i10&category=15001&crashed=f&from_suggester=1&location2=3&make=13205&model=14904&offer_type=sale&pg={}",
        "supabase_model": "i10",
        "stats_table": "i10_price_stats_by_mileage"
        },
    {
        "make": "Hyundai",
        "model": "i20",
        "url_template": "https://www.car.gr/used-cars/hyundai/i_20.html?activeq=Hyundai+i20&category=15001&crashed=f&from_suggester=1&location2=3&make=12522&model=15412&offer_type=sale&pg={}",
        "supabase_model": "i20",
        "stats_table": "i20_price_stats_by_mileage"
    }
    , {
        "make": "Citroen",
        "model": "C3",
        "url_template": "https://www.car.gr/used-cars/citroen/c3.html?activeq=Citroen+C3&category=15001&crashed=f&from_suggester=1&location2=3&make=13199&model=15404&offer_type=sale&pg={}",
        "supabase_model": "C3",
        "stats_table": "c3_price_stats_by_mileage"
    }
    , {
        "make": "Ford",
        "model": "Fiesta",
        "url_template": "https://www.car.gr/used-cars/ford/fiesta.html?activeq=Ford+Fiesta&category=15001&crashed=f&from_suggester=1&location2=3&make=13150&model=14843&offer_type=sale&pg={}",
        "supabase_model": "Fiesta",
        "stats_table": "fiesta_price_stats_by_mileage"
    }

        ]

# Selenium config
def get_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/115.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=options)

# Fetch existing listings to avoid duplicates
def get_existing_source_ids(model):
    response = supabase.table("listings").select("source_id").eq("model", model).execute()
    return set([item["source_id"] for item in response.data])

# Fetch price stats from Supabase
def get_stats(stats_table):
    response = supabase.table(stats_table).select("*").execute()
    stats = {}
    for row in response.data:
        key = (int(row['year']), row['mileage_bin'])
        stats[key] = row
    return stats

# Assign deal score using the Granular Quartile Model
def assign_deal_score(listing, stats):
    try:
        year = int(listing['year'])
        mileage = int(listing['mileage'])
        price = float(listing['price'])
    except (ValueError, TypeError, KeyError):
        return None  # Return None if data is missing or invalid

    bin_size = 25000
    mileage_bin = f"{(mileage // bin_size) * bin_size}-{((mileage // bin_size) + 1) * bin_size}"
    
    stat = stats.get((year, mileage_bin))
    if not stat:
        return None  # No statistical data for this car's group

    p25 = float(stat['p25_price'])
    median = float(stat['median_price'])
    p75 = float(stat['p75_price'])

    if price < p25:
        return 1  # Excellent Deal
    elif price < median:
        return 2  # Good Deal
    elif price < p75:
        return 3  # Fair Price
    else:
        return 4  # Expensive

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

# Scrape and process listings
def scrape_new_listings():
    driver = get_driver()
    
    for car in CAR_MODELS:
        try:
            print(f"\n[INFO] Scraping listings for {car['make']} {car['model']}...")
            base_url = car['url_template']
            print(f"DEBUG raw URL: {repr(base_url)}")
            model = car['supabase_model']
            stats_table = car['stats_table']
            
            print("[INFO] Fetching market data...")
            existing_ids = get_existing_source_ids(model)
            stats = get_stats(stats_table)
            
            all_processed_listings = []
            formated_url = base_url.format(1)
            print(f"[INFO] Formatted URL: {formated_url}")
            total_pages = get_total_pages(driver, base_url.format(1))
            if total_pages > 2:
                print(f"[WARN] Found {total_pages} pages, limiting check to first 5 pages for speed.")
                total_pages = 2
                
            print(f"[INFO] Total pages to scrape: {total_pages}")
            print("[INFO] Starting scrape...")
            
            for page in range(1, total_pages + 1):
                print(f"\n[INFO] Scraping page {page} for {car['make']} {car['model']}...")
                driver.get(base_url.format(page))
                time.sleep(1)  # Rate limiting
                
                try:
                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[index]")))
                except TimeoutException:
                    print(f"[WARN] No listings found on page {page}, or page timed out.")
                    continue
                    
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                listing_items = soup.select("div[index]")
                
                for listing in listing_items:
                    try:
                        title_tag = listing.select_one('h3')
                        if not title_tag:
                            continue

                        title_text = title_tag.get_text(separator=' ', strip=True)
                        parts = title_text.split()
                        make = parts[0] if len(parts) > 0 else None
                        model_name = parts[1] if len(parts) > 1 else None
                        year = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None

                        link_tag = listing.select_one('a.row-anchor')
                        if not link_tag:
                            continue
                        relative_url = link_tag['href']
                        source_id = relative_url.split('/')[-1].split('-')[0]

                        full_url = f"https://www.car.gr{relative_url}"
                        price_tag = listing.select_one('span.lg\\:tw-text-3xl span')
                        price_raw = price_tag.get_text(strip=True).replace('.', '').replace('€', '') if price_tag else '0'
                        price = float(price_raw) if price_raw.replace(',', '').isdigit() else None

                        mileage_tag = listing.select_one('div[title="Χιλιόμετρα"] p')
                        mileage_raw = mileage_tag.get_text(strip=True) if mileage_tag else None
                        mileage = int(mileage_raw.replace('.', '').replace('Km', '').strip()) if mileage_raw else None
                        
                        img_tag = listing.select_one('img')
                        image_url = img_tag['src'] if img_tag and img_tag.has_attr('src') else None

                        description_tag = listing.select_one('h3 + p')
                        description = description_tag.get_text(strip=True) if description_tag else ""

                        if not all([source_id, make, model_name, year, mileage, price]):
                            print(f"[WARN] Skipping listing with missing core data: {full_url}")
                            continue

                        new_listing = {
                            "source_id": source_id,
                            "make": make,
                            "model": model,  # Use supabase_model for consistency
                            "year": year,
                            "mileage": mileage,
                            "price": price,
                            "url": full_url,
                            "image_url": image_url,
                            "description": description,
                            "timestamp": datetime.utcnow().isoformat()
                        }

                        score = assign_deal_score(new_listing, stats)
                        if score:
                            new_listing["deal_score"] = score

                        if source_id in existing_ids:
                            print(f"[INFO] Updating deal score for existing listing: {source_id} -> Score: {score}")
                            supabase.table("listings").update({"deal_score": score}).eq("source_id", source_id).execute()
                        else:
                            print(f"[✓] Saved new listing: {source_id} -> Score: {score}")
                            supabase.table("listings").insert(new_listing).execute()
                            existing_ids.add(source_id)

                        all_processed_listings.append(new_listing)

                    except Exception as e:
                        print(f"[!] Error parsing a listing: {e}")

                # Process good deals for the current model
                good_deals_for_report = []
                for listing in all_processed_listings:
                    try:
                        year = int(listing['year'])
                        mileage = int(listing['mileage'])
                        price = float(listing['price'])

                        bin_size = 25000
                        mileage_bin = f"{(mileage // bin_size) * bin_size}-{((mileage // bin_size) + 1) * bin_size}"
                        stat = stats.get((year, mileage_bin))
                        if not stat:
                            continue

                        p25 = float(stat['p25_price'])

                        if price >= p25:
                            continue

                        discount_ratio = (p25 - price) / p25

                        if discount_ratio >= 0.2:    score = 10
                        elif discount_ratio >= 0.15: score = 9
                        elif discount_ratio >= 0.1:  score = 8
                        elif discount_ratio >= 0.07: score = 7
                        elif discount_ratio >= 0.05: score = 6
                        elif discount_ratio >= 0.03: score = 4
                        else:                        score = 3
                            
                        good_deals_for_report.append({
                            "listing": listing,
                            "stat": stat,
                            "discount_vs_p25": p25 - price,
                            "deal_score_user": score
                        })

                    except (ValueError, TypeError, KeyError):
                        continue

                # Clear previous highlights for this model
                supabase.table("listings").update({"highlighted": False}).eq("model", model).execute()

                # Flag top 10 as highlighted for this model
                good_deals_for_report = sorted(
                    good_deals_for_report,
                    key=lambda x: (x['deal_score_user'], x['discount_vs_p25']),
                    reverse=True
                )[:10]

                for item in good_deals_for_report:
                    listing_id = item["listing"]["source_id"]
                    supabase.table("listings").update({"highlighted": True}).eq("source_id", listing_id).execute()

        except Exception as e:
            print(f"[ERROR] Failed to scrape {car['make']} {car['model']}: {e}")
            continue

    driver.quit()
    print("[INFO] Scrape completed successfully for all models.")

# Entry point
if __name__ == "__main__":
    while True:
        try:
            print("\n[INFO] Starting new listings scrape...")
            scrape_new_listings()
            print("[INFO] Scrape completed successfully.")
            break  # Exit loop if successful (remove or adjust for continuous scraping)
        except Exception as e:
            print(f"[ERROR] An error occurred during scraping: {e}")
            time.sleep(60)  # Wait before retrying 