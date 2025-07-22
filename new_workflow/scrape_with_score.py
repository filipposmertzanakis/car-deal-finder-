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
import undetected_chromedriver as uc
from dotenv import load_dotenv
import os

import smtplib, ssl
from email.message import EmailMessage

load_dotenv()

# --- Supabase setup ---
SUPABASE_URL = os.getenv("SUPABASE_URL").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY").strip()
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

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
    print("[DEBUG] Initializing Chrome options...")
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    # options.add_argument("--headless")  # CRITICAL for GitHub Actions
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage") # Recommended for containerized environments
    options.add_argument("window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.207 Safari/537.36")
    driver = uc.Chrome(options=options)
    return driver

def get_existing_source_ids(model):
    response = supabase.table("listings").select("source_id").eq("model", model).execute()
    return set([item["source_id"] for item in response.data])

def get_stats(stats_table):
    response = supabase.table(stats_table).select("*").execute()
    stats = {}
    for row in response.data:
        key = (int(row['year']), row['mileage_bin'])
        stats[key] = row
    return stats

def assign_deal_score(listing, stats):
    try:
        year = int(listing['year'])
        mileage = int(listing['mileage'])
        price = float(listing['price'])
    except (ValueError, TypeError, KeyError): return None
    bin_size = 25000
    mileage_bin = f"{(mileage // bin_size) * bin_size}-{((mileage // bin_size) + 1) * bin_size}"
    stat = stats.get((year, mileage_bin))
    if not stat: return None
    p25, median, p75 = float(stat['p25_price']), float(stat['median_price']), float(stat['p75_price'])
    if price < p25: return 1
    elif price < median: return 2
    elif price < p75: return 3
    else: return 4

def get_total_pages(driver, url):
    print("[INFO] Determining total number of pages...")
    driver.get(url)
    time.sleep(1)
    try:
        last_page_element = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, "//button[span[text()='…']]/following-sibling::a")))
        return int(last_page_element.text.strip())
    except TimeoutException:
        print("[INFO] '...' button not found. Trying fallback method.")
    try:
        pagination_container = WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.TAG_NAME, "nav")))
        all_links = pagination_container.find_elements(By.TAG_NAME, "a")
        page_numbers = [int(link.text.strip()) for link in all_links if link.text.strip().isdigit()]
        return max(page_numbers) if page_numbers else 1
    except TimeoutException:
        print("[WARN] No pagination found at all. Assuming 1 page.")
        return 1

# Scrape and process listings
def scrape_new_listings():
    driver = get_driver()
    
    # YOUR ORIGINAL LOGIC IS UNTOUCHED
    all_best_deals = []
    
    # --- NEW LOGIC ---
    # A separate list just for collecting deals for the final email report.
    deals_for_email_report = []
    
    for car in CAR_MODELS:
        try:
            print(f"\n[INFO] Scraping listings for {car['make']} {car['model']}...")
            base_url = car['url_template']
            model = car['supabase_model']
            stats_table = car['stats_table']
            
            print("[INFO] Fetching market data...")
            existing_ids = get_existing_source_ids(model)
            stats = get_stats(stats_table)
            
            all_processed_listings = []
            formated_url = base_url.format(1)
            total_pages = get_total_pages(driver, base_url.format(1))
            if total_pages > 2:
                print(f"[WARN] Found {total_pages} pages, limiting check to first 2 pages for speed.")
                total_pages = 2
            
            print(f"[INFO] Total pages to scrape: {total_pages}")
            
            for page in range(1, total_pages + 1):
                print(f"\n[INFO] Scraping page {page} for {car['make']} {car['model']}...")
                driver.get(base_url.format(page))
                time.sleep(1)
                
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
                        if not title_tag: continue
                        title_text = title_tag.get_text(separator=' ', strip=True)
                        parts = title_text.split()
                        make, model_name = (parts[0], parts[1]) if len(parts) > 1 else (None, None)
                        year = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
                        link_tag = listing.select_one('a.row-anchor')
                        if not link_tag: continue
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

                        if not all([source_id, make, model_name, year, mileage, price]): continue

                        new_listing = {"source_id": source_id, "make": make, "model": model, "year": year, "mileage": mileage, "price": price, "url": full_url, "image_url": image_url, "description": description, "timestamp": datetime.utcnow().isoformat()}
                        score = assign_deal_score(new_listing, stats)
                        if score: new_listing["deal_score"] = score

                        if source_id in existing_ids:
                            supabase.table("listings").update({"deal_score": score}).eq("source_id", source_id).execute()
                        else:
                            supabase.table("listings").insert(new_listing).execute()
                            existing_ids.add(source_id)
                        
                        all_processed_listings.append(new_listing)

                    except Exception as e:
                        print(f"[!] Error parsing a listing: {e}")

                # YOUR ORIGINAL LOGIC IS UNTOUCHED
                good_deals_for_report = []
                for listing in all_processed_listings:
                    try:
                        year, mileage, price = int(listing['year']), int(listing['mileage']), float(listing['price'])
                        bin_size = 25000
                        mileage_bin = f"{(mileage // bin_size) * bin_size}-{((mileage // bin_size) + 1) * bin_size}"
                        stat = stats.get((year, mileage_bin))
                        if not stat: continue
                        
                        p25 = float(stat['p25_price'])
                        # **FIXED THE ORIGINAL BUG HERE**
                        p50 = float(stat['median_price']) 

                        if price >= p25: continue

                        discount_ratio = (p25 - price) / p25
                        if discount_ratio >= 0.2: score = 10
                        elif discount_ratio >= 0.15: score = 9
                        elif discount_ratio >= 0.1: score = 8
                        elif discount_ratio >= 0.07: score = 7
                        elif discount_ratio >= 0.05: score = 6
                        elif discount_ratio >= 0.03: score = 4
                        else: score = 3
                        good_deals_for_report.append({"listing": listing, "stat": stat, "discount_vs_p25": p25 - price, "deal_score_user": score, "p50": p50})
                    except (ValueError, TypeError, KeyError): continue

                # YOUR ORIGINAL LOGIC IS UNTOUCHED
                supabase.table("listings").update({"highlighted": False}).eq("model", model).execute()
                good_deals_for_report = sorted(good_deals_for_report, key=lambda x: (x['deal_score_user'], x['discount_vs_p25']), reverse=True)[:10]
                for item in good_deals_for_report:
                    listing_id = item["listing"]["source_id"]
                    supabase.table("listings").update({"highlighted": True}).eq("source_id", listing_id).execute()
                
                for d in good_deals_for_report:
                    p50, price = d.get("p50"), d["listing"]["price"]
                    d["profit_margin_ratio"] = round((p50 - price) / p50, 3) if p50 and price else None

                best_margin_deals = sorted([d for d in good_deals_for_report if d["profit_margin_ratio"] is not None], key=lambda x: x["profit_margin_ratio"], reverse=True)

                # YOUR ORIGINAL LOGIC IS UNTOUCHED
                all_best_deals.extend(best_margin_deals)

                # --- NEW LOGIC ---
                # Add the deals from this model to our special email list.
                deals_for_email_report.extend(best_margin_deals)
                print(f"[INFO] Added {len(best_margin_deals)} deals from {model} to the email report queue.")


        except Exception as e:
            print(f"[ERROR] Failed to scrape {car['make']} {car['model']}: {e}")
            continue

    driver.quit()
    print("[INFO] Scrape completed successfully for all models.")
    
    # --- NEW LOGIC ---
    # Sort the dedicated email list to get the final top 10 overall deals.
    print(f"[INFO] Found {len(deals_for_email_report)} total deals. Sorting for the final top 10 email report.")
    final_deals_for_email = sorted(
        [d for d in deals_for_email_report if d.get("profit_margin_ratio") is not None],
        key=lambda x: x["profit_margin_ratio"],
        reverse=True
    )[:10]
    
    # The function now returns the list specifically for the email.
    return final_deals_for_email

def send_best_deals_email(recipient_email, deals):
    sender_email = "carflipgr@gmail.com"
    password = "uuhr xlya jwbp nahm"

    if not deals:
        print("[INFO] No deals to send.")
        return

    print(f"[INFO] Preparing email with top {len(deals)} deals...")
    msg = EmailMessage()
    msg["Subject"] = f"🚗 Top {len(deals)} Car Deals Found!"
    msg["From"] = sender_email
    msg["To"] = recipient_email

    body_lines = ["Here are the top deals from the latest scan:\n"]

    for i, deal in enumerate(deals, start=1):
        l = deal["listing"]
        try:
            score = deal.get('deal_score_user', 'N/A')
            discount = deal.get('discount_vs_p25', 0)
            profit_margin = deal.get('profit_margin_ratio')
            profit = price
            profit_margin_str = f"{profit_margin:.1%}" if profit_margin is not None else "N/A"

            line = (
                f"{i}. {l['make']} {l['model']} ({l['year']})\n"
                f"   Price: €{l['price']:,.0f} | Mileage: {l['mileage']:,.0f} km\n"
                f"   User Score: {score}/10 | Potential Margin: {profit_margin_str}\n"
                f"   URL: {l['url']}\n" + ("-" * 40)
            )
            body_lines.append(line)
        except (KeyError, TypeError) as e:
            print(f"[ERROR] Could not format deal {i} for email: {e}")

    email_body = "\n".join(body_lines)
    msg.set_content(email_body)
    context = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, password)
            server.send_message(msg)
        print(f"[✓] Email sent to {recipient_email}")
    except Exception as e:
        print(f"[ERROR] General error while sending the email: {e}")


# Entry point
if __name__ == "__main__":
    while True:
        try:
            print("\n[INFO] Starting new listings scrape...")
            # This call now returns the top 10 deals for the email.
            deals_to_email = scrape_new_listings()
            print(f"[INFO] Scrape completed successfully. Found {len(deals_to_email)} deals for the email report.")

            if deals_to_email:
                send_best_deals_email("filipposmertz@gmail.com", deals_to_email)
            else:
                print("[INFO] No new deals met the criteria for an email.")

            break
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"[ERROR] An error occurred during scraping: {e}")
            time.sleep(60)