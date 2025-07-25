import time
from datetime import datetime
from io import BytesIO
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

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
load_dotenv()

# --- Supabase setup ---
SUPABASE_URL = os.getenv("SUPABASE_URL").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY").strip()
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Email setup ---
EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")  # e.g., smtp.gmail.com
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USER = os.getenv("EMAIL_USER")  # Your email
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")  # Your email password or app password
# EMAIL_RECIPIENT = os.getenv("EMAIL_RECIPIENT")  # Recipient email
EMAIL_RECIPIENT = "filipposmertz@gmail.com" 

# Complete Car models configuration
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
    },
    {
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
    },
    {
        "make": "Citroen",
        "model": "C3",
        "url_template": "https://www.car.gr/used-cars/citroen/c3.html?activeq=Citroen+C3&category=15001&crashed=f&from_suggester=1&location2=3&make=13199&model=15404&offer_type=sale&pg={}",
        "supabase_model": "C3",
        "stats_table": "c3_price_stats_by_mileage"
    },
    {
        "make": "Ford",
        "model": "Fiesta",
        "url_template": "https://www.car.gr/used-cars/ford/fiesta.html?activeq=Ford+Fiesta&category=15001&crashed=f&from_suggester=1&location2=3&make=13150&model=14843&offer_type=sale&pg={}",
        "supabase_model": "Fiesta",
        "stats_table": "fiesta_price_stats_by_mileage"
    }
]

def get_driver():
    print("[DEBUG] Initializing Chrome options...")
    options = uc.ChromeOptions()
    
    # Essential options for CI environments
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-logging")
    options.add_argument("--log-level=3")
    options.add_argument("--silent")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
    
# CI-specific paths
    if os.getenv('GITHUB_ACTIONS') == 'true':
        print("[DEBUG] Running in CI environment, enabling headless mode")
        options.add_argument("--headless=new")
        # ... other CI arguments ...

        # Use user-installed binaries
        driver_executable_path = os.path.expanduser('~/chromedriver')
        browser_executable_path = '/usr/bin/google-chrome'
    else:
        driver_executable_path = None
        browser_executable_path = None

    print("[DEBUG] Starting browser...")
    
    try:
        print("[DEBUG] Trying undetected-chromedriver...")
        driver = uc.Chrome(
            options=options,
            driver_executable_path=driver_executable_path,
            browser_executable_path=browser_executable_path,
            version_main=138
        )
        driver.set_page_load_timeout(60)
        print("[DEBUG] Undetected ChromeDriver initialized successfully")
        return driver
        
    except Exception as e:
        print(f"[WARN] Undetected-chromedriver failed: {e}")
        print("[DEBUG] Falling back to regular Selenium...")
    
    # Fallback to regular Selenium
    try:
        from selenium.webdriver.chrome.service import Service
        
        # Try different chromedriver paths
        chromedriver_paths = [
            os.path.expanduser('~/chromedriver'),  # New CI path
            '/usr/local/bin/chromedriver',
            '/usr/bin/chromedriver',
            'chromedriver'
        ]
        
        for path in chromedriver_paths:
            try:
                if path != 'chromedriver':
                    # Check if file exists and is executable
                    if not os.path.exists(path):
                        print(f"[DEBUG] ChromeDriver not found at {path}")
                        continue
                    if not os.access(path, os.X_OK):
                        print(f"[DEBUG] ChromeDriver at {path} is not executable")
                        continue
                
                print(f"[DEBUG] Trying ChromeDriver at: {path}")
                service = Service(path) if path != 'chromedriver' else Service()
                
                # For headless mode in CI
                if os.getenv('GITHUB_ACTIONS') == 'true':
                    options.add_argument("--headless=new")
                
                driver = webdriver.Chrome(service=service, options=options)
                driver.set_page_load_timeout(60)
                print("[DEBUG] Regular Selenium WebDriver initialized successfully")
                return driver
                
            except Exception as e:
                print(f"[DEBUG] Failed with ChromeDriver at {path}: {e}")
                continue
        
        print("[ERROR] All ChromeDriver paths failed")
        
    except Exception as e:
        print(f"[ERROR] Regular Selenium setup failed: {e}")
    
    raise Exception("Could not initialize any WebDriver. Please check Chrome and ChromeDriver installation.")

def get_existing_source_ids(model):
    """Get all existing source_ids for a model, ensuring consistent string type"""
    response = supabase.table("listings").select("source_id").eq("model", model).execute()
    return set(str(item["source_id"]).strip() for item in response.data)

def get_emailed_listings(model):
    """Get all listings that have been emailed, ensuring consistent string type"""
    response = supabase.table("listings").select("source_id").eq("model", model).eq("email_sent", True).execute()
    return set(str(item["source_id"]).strip() for item in response.data)

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
    except (ValueError, TypeError, KeyError):
        return None

    bin_size = 25000
    mileage_bin = f"{(mileage // bin_size) * bin_size}-{((mileage // bin_size) + 1) * bin_size}"
    
    stat = stats.get((year, mileage_bin))
    if not stat:
        return None

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

def send_email_notification(high_profit_deals, all_stats_by_model):
    """Send email notification for cars with high profit margins to multiple recipients"""
    if not high_profit_deals:
        print("[INFO] No high profit deals found. No email sent.")
        return False
    
    # Hardcoded list of recipients
    recipients = [
        "filipposmertz@gmail.com",
        "pakoissick@gmail.com",
        "carflipgr@gmail.com",
        "fourth@example.com"
    ]
    
    if not all([EMAIL_USER, EMAIL_PASSWORD]):
        if not EMAIL_USER:
            print("[WARN] EMAIL_USER is not set. Skipping email notification.")
        if not EMAIL_PASSWORD:
            print("[WARN] EMAIL_PASSWORD is not set. Skipping email notification.")
        
        print("[WARN] Email configuration missing. Skipping email notification.")
        return False
    
    try:
        # Build email content once (reused for all recipients)
        subject = f"🚗 Ειδοποίηση Επικερδών Αυτοκινήτων - {len(high_profit_deals)} Ευκαιρίες!"
        
        # Complete HTML email body with styling
        html_body = f"""
        <html>
        <head>
            <style>
                /* Your existing CSS styles */
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🚗 Ειδοποίηση Επικερδών Αυτοκινήτων</h1>
                    <p>Βρέθηκαν {len(high_profit_deals)} εξαιρετικές ευκαιρίες με περιθώρια κέρδους 20%+ και δυνατότητα κέρδους €2,000+</p>
                </div>
                <div class="content">
        """
        
        # Calculate total potential profit
        total_profit = sum(deal['discount_vs_p25'] for deal in high_profit_deals)
        avg_margin = sum(deal['profit_margin_percent'] for deal in high_profit_deals) / len(high_profit_deals)
        
        html_body += f"""
                    <div class="stats">
                        <h3>📊 Στατιστικά Περίληψης</h3>
                        <p><strong>Συνολικό Δυνητικό Κέρδος:</strong> €{total_profit:,.0f} | <strong>Μέσο Περιθώριο:</strong> {avg_margin:.1f}%</p>
                    </div>
        """
        
        for i, deal in enumerate(high_profit_deals, 1):
            listing = deal['listing']
            profit_margin = deal['profit_margin_percent']
            discount_amount = deal['discount_vs_p25']
            market_price_p25 = deal['market_price_p25']
            
            # Calculate difference from median (P50)
            year = int(listing['year'])
            mileage = int(listing['mileage'])
            model = listing['model']
            bin_size = 25000
            mileage_bin = f"{(mileage // bin_size) * bin_size}-{((mileage // bin_size) + 1) * bin_size}"
            
            # Get median price from stats for this model
            median_price = market_price_p25  # fallback
            if model in all_stats_by_model:
                model_stats = all_stats_by_model[model]
                stat = model_stats.get((year, mileage_bin))
                if stat:
                    median_price = float(stat['median_price'])
            
            median_discount = median_price - listing['price']
            median_margin = (median_discount / median_price) * 100 if median_price > 0 else 0
            
            html_body += f"""
            <div class="deal">
                <div class="deal-title">#{i} - {listing['make']} {listing['model']} {listing['year']}</div>
                <div class="details">
                    <strong>Τιμή Αγγελίας:</strong> €{listing['price']:,.0f} | 
                    <strong>Μέση Αξία Αγοράς (P50):</strong> €{median_price:,.0f} | 
                    <strong>Φθηνή Τιμή (P25):</strong> €{market_price_p25:,.0f}
                </div>
                <div class="profit">
                    💰 Κέρδος από μέση αξία: €{median_discount:,.0f} ({median_margin:.1f}% έκπτωση!)
                </div>
                <div class="details">
                    <strong>Χιλιόμετρα:</strong> {listing['mileage']:,} km | 
                    <strong>Έτος:</strong> {listing['year']}
                </div>
                <div class="details">
                    <strong>Περιγραφή:</strong> {listing.get('description', 'Δεν υπάρχει περιγραφή')[:150]}{'...' if len(listing.get('description', '')) > 150 else ''}
                </div>
                <div class="details">
                    <a href="{listing['url']}" class="link" target="_blank">Δες την Αγγελία →</a>
                </div>
            </div>
            """
        
        html_body += f"""
                </div>
                <div class="footer">
                    <p><strong>Ειδοποίηση δημιουργήθηκε:</strong> {datetime.now().strftime('%d %B %Y στις %H:%M:%S')}</p>
                    <p>Αυτή είναι μια αυτοματοποιημένη ειδοποίηση από το σύστημα παρακολούθησης αυτοκινήτων σας.</p>
                    <p>💡 <em>Συμβουλή: Ενεργήστε γρήγορα σε αυτές τις ευκαιρίες καθώς μπορεί να μην διαρκέσουν πολύ!</em></p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Send individual emails
        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            
            success_count = 0
            for recipient in recipients:
                try:
                    # Create new message for each recipient
                    msg = MIMEMultipart()
                    msg['From'] = EMAIL_USER
                    msg['To'] = recipient
                    msg['Subject'] = subject
                    msg.attach(MIMEText(html_body, 'html'))
                    
                    # Send email
                    server.sendmail(EMAIL_USER, recipient, msg.as_string())
                    success_count += 1
                    print(f"[✓] Email sent to: {recipient}")
                    
                except Exception as e:
                    print(f"[!] Failed to send to {recipient}: {e}")
            
            if success_count > 0:
                print(f"[✓] Successfully sent {success_count}/{len(recipients)} email notifications")
                return True
            else:
                print("[✗] Failed to send to all recipients")
                return False
                
    except Exception as e:
        print(f"[ERROR] Email system failure: {e}")
        return False


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

def scrape_new_listings():
    driver = get_driver()
    all_high_profit_deals = []  # Collect high profit deals across all models
    all_stats_by_model = {}  # Store stats for email calculations
    problem_words = ["προβλημα", "βλάβη", "ζημιά", "ατυχημα"]  # Add more as needed
    
    for car in CAR_MODELS:
        try:
            print(f"\n[INFO] Scraping listings for {car['make']} {car['model']}...")
            base_url = car['url_template']
            model = car['supabase_model']
            stats_table = car['stats_table']
            
            print("[INFO] Fetching market data...")
            existing_ids = get_existing_source_ids(model)
            emailed_ids = get_emailed_listings(model)
            print(f"[INFO] Found {len(emailed_ids)} listings already emailed for {model}.")
            
            stats = get_stats(stats_table)
            all_stats_by_model[model] = stats
            
            all_processed_listings = []
            formatted_url = base_url.format(1)
            total_pages = get_total_pages(driver, formatted_url)
            if total_pages > 2:
                print(f"[WARN] Found {total_pages} pages, limiting check to first 2 pages for speed.")
                total_pages = 2
                
            print(f"[INFO] Total pages to scrape: {total_pages}")
            
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
                        source_id = str(relative_url.split('/')[-1].split('-')[0]).strip()

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

                        if any(word in description.lower() for word in problem_words):
                            print(f"[INFO] Skipping listing with problematic description: {full_url}")
                            continue

                        if not all([source_id, make, model_name, year, mileage, price]):
                            print(f"[WARN] Skipping listing with missing core data: {full_url}")
                            continue

                        new_listing = {
                            "source_id": source_id,
                            "make": make,
                            "model": model,
                            "year": year,
                            "mileage": mileage,
                            "price": price,
                            "url": full_url,
                            "image_url": image_url,
                            "description": description,
                            "timestamp": datetime.utcnow().isoformat(),
                            "email_sent": False
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
            high_profit_deals_for_model = []
            
            for listing in all_processed_listings:
                try:
                    source_id = str(listing['source_id']).strip()
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

                    discount_amount = p25 - price
                    profit_margin_percent = (discount_amount / p25) * 100

                    # Debug check
                    print(f"[DEBUG] Checking email eligibility for {source_id}")
                    print(f"[DEBUG] Emailed IDs contains {source_id}: {source_id in emailed_ids}")
                    
                    # Only consider if not already emailed and meets profit criteria
                    if (profit_margin_percent >= 20 and 
                        discount_amount >= 2000 and 
                        source_id not in emailed_ids):
                        
                        high_profit_deal = {
                            "listing": listing,
                            "market_price_p25": p25,
                            "discount_vs_p25": discount_amount,
                            "profit_margin_percent": profit_margin_percent
                        }
                        high_profit_deals_for_model.append(high_profit_deal)
                        all_high_profit_deals.append(high_profit_deal)
                        print(f"[💰] NEW HIGH PROFIT DEAL: {listing['make']} {listing['model']} {listing['year']} - {profit_margin_percent:.1f}% profit margin (€{discount_amount:,.0f} profit)!")

                except (ValueError, TypeError, KeyError) as e:
                    print(f"[ERROR] Error processing deal score: {e}")
                    continue

            print(f"[INFO] Found {len(high_profit_deals_for_model)} high profit deals for {car['make']} {car['model']}")

        except Exception as e:
            print(f"[ERROR] Failed to scrape {car['make']} {car['model']}: {e}")
            continue

    driver.quit()
    
    # Final check before sending emails
    if all_high_profit_deals:
        print(f"\n[INFO] Verifying {len(all_high_profit_deals)} potential deals before emailing...")
        
        # Get fresh list of emailed IDs from database
        all_emailed_ids = set()
        for car in CAR_MODELS:
            all_emailed_ids.update(get_emailed_listings(car['supabase_model']))
        
        # Filter out any deals that were marked as emailed since we checked
        final_deals_to_email = [
            deal for deal in all_high_profit_deals 
            if str(deal['listing']['source_id']).strip() not in all_emailed_ids
        ]
        
        if final_deals_to_email:
            print(f"[INFO] Sending email notification for {len(final_deals_to_email)} verified new high profit deals...")
            final_deals_to_email.sort(key=lambda x: x['profit_margin_percent'], reverse=True)
            
            email_sent_successfully = send_email_notification(final_deals_to_email, all_stats_by_model)
            
            if email_sent_successfully:
                # Mark all sent deals in a single transaction
                source_ids = [str(deal['listing']['source_id']).strip() for deal in final_deals_to_email]
                response = supabase.table("listings")\
                    .update({"email_sent": True})\
                    .in_("source_id", source_ids)\
                    .execute()
                print(f"[✓] Marked all source_ids as emailed, response: {response}")
        else:
            print("[INFO] All potential deals were already emailed (race condition avoided)")
    else:
        print("[INFO] No new high profit deals found - no email needed.")
    
    print("[INFO] Scrape completed successfully for all models.")

if __name__ == "__main__":
    while True:
        try:
            print("\n[INFO] Starting new listings scrape...")
            scrape_new_listings()
            print("[INFO] Scrape completed successfully.")
            break
        except Exception as e:
            print(f"[ERROR] An error occurred during scraping: {e}")
            time.sleep(60)