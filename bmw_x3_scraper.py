"""
bmw_x3_scraper.py
-------------------

This script scrapes used BMW X3 listings from AutoTrader.ca for model years
2021‑2023 located within 500 km of the postal code “H1X 3J1” (Montreal,
Quebec).  It navigates the AutoTrader search results page with Selenium,
collects all listing URLs and then visits each detail page.  Most of the
structured vehicle data resides in a JavaScript object called
``window.ngVdpModel`` which AutoTrader injects into each vehicle page.  Once
available, that object contains the ad’s make, model, year, mileage, price,
seller information and more【298376264705576†L224-L233】.  By pulling data
from this object the scraper can reliably extract the fields listed in
AutoTrader’s own documentation【298376264705576†L246-L255】.

Because the site is built with client‑side JavaScript it cannot be scraped
reliably using ``requests`` alone.  The script therefore uses Selenium
alongside ``webdriver_manager`` to automatically download and manage a
compatible version of ChromeDriver.  It scrolls through the results page
(AutoTrader loads more vehicles as you scroll) and grabs every anchor tag
whose URL path contains ``/a/``, which points to a specific vehicle listing.
Each listing is visited and the contents of ``window.ngVdpModel`` are read
via ``driver.execute_script``; if this object does not materialise the script
falls back to parsing the page’s HTML with BeautifulSoup.

Duplicate listings are removed by keying on the seller name, year and
mileage.  Finally, the collected data are written to ``subaru_forester_used_2014_plus.csv``.

To run this script you will need the following Python packages:

    pip install selenium webdriver-manager beautifulsoup4 pandas

Note: AutoTrader may update their site or deploy anti‑bot measures.  If you
encounter issues, consider adding random delays, rotating user agents or
using a proxy.  Always consult the website’s terms of service before
scraping.
"""

# ── Scraper settings ─────────────────────────────────────────────────────
MAX_LISTINGS = None       # Max listings per vehicle (None = all)
LISTING_PAUSE_SECS = 0   # Pause between each listing scrape

import json
import re
import time
from datetime import datetime as _dt

def log(msg: str) -> None:
    print(f"[{_dt.now().strftime('%H:%M:%S')}] {msg}", flush=True)
from dataclasses import dataclass, asdict
from datetime import datetime
import os
import subprocess
from typing import List, Dict, Any, Optional, Set, Tuple

from bs4 import BeautifulSoup  # type: ignore
import pandas as pd  # type: ignore

from selenium import webdriver  # type: ignore
from selenium.webdriver.chrome.options import Options  # type: ignore
from selenium.webdriver.common.by import By  # type: ignore
from selenium.webdriver.support.ui import WebDriverWait  # type: ignore
from selenium.webdriver.support import expected_conditions as EC  # type: ignore
from webdriver_manager.chrome import ChromeDriverManager  # type: ignore
from selenium.webdriver.chrome.service import Service  # type: ignore


@dataclass
class VehicleListing:
    """Container for a single vehicle listing extracted from AutoTrader.ca."""

    scrape_number: Optional[int] = None
    scrape_timestamp: Optional[str] = None
    make: Optional[str] = None
    title: Optional[str] = None
    url: Optional[str] = None
    seller_name: Optional[str] = None
    model: Optional[str] = None
    year: Optional[int] = None
    mileage_km: Optional[int] = None
    price_cad: Optional[int] = None
    status: Optional[str] = None
    trim: Optional[str] = None
    exterior_colour: Optional[str] = None
    fuel_type: Optional[str] = None
    city: Optional[str] = None
    province: Optional[str] = None
    is_private_seller: Optional[bool] = None
    # Fields from `subaru_forester_used_2014_plus.csv`
    has_driver_assistance: Optional[bool] = None
    has_carplay: Optional[bool] = None
    has_cruise: Optional[bool] = None
    price_analysis_description: Optional[str] = None
    average_market_price: Optional[int] = None
    price_vs_market: Optional[int] = None
    ad_id: Optional[str] = None
    dealer_co_id: Optional[str] = None
    vin: Optional[str] = None
    odometer_condition: Optional[str] = None
    price_position: Optional[str] = None
    price_evaluation: Optional[str] = None
    google_map_url: Optional[str] = None
    description: Optional[str] = None
    last_scrape_timestamp: Optional[str] = None
    is_deleted: Optional[str] = None


def create_driver(headless: bool = True) -> webdriver.Chrome:
    """Instantiate a headless Chrome WebDriver using webdriver‑manager.

    The ``webdriver_manager`` library automatically downloads an appropriate
    version of ChromeDriver and ensures that Selenium can locate it.  The
    ``--headless=new`` flag is used with modern versions of Chrome to
    suppress the GUI; remove it if you need to see the browser window.

    Returns
    -------
    webdriver.Chrome
        A configured Selenium Chrome driver.
    """
    options = Options()
    # Explicitly set the path to the Chrome binary. This is necessary when
    # Selenium cannot automatically find the Chrome installation.
    options.binary_location = "/Applications/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing"
    if headless:
        # Use the new headless mode if available.  This avoids some rendering
        # quirks of the old headless implementation.
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    # Anti-detection: hide headless indicators from Incapsula/Imperva bot protection
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    # Setting a realistic user‑agent matching the actual Chrome version
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    )
    # Explicitly specify the driver version to match the browser version from the error log.
    # This is a workaround for when webdriver-manager fails to auto-detect the correct version.
    service = Service(ChromeDriverManager(driver_version="130.0.6723.69").install())
    driver = webdriver.Chrome(service=service, options=options)
    # Hide navigator.webdriver flag that bot detectors check
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    })
    # Prevent page loads from hanging indefinitely
    driver.set_page_load_timeout(30)
    return driver


def scroll_to_load_all(driver: webdriver.Chrome, pause: float = 2.0) -> None:
    """Scroll the page until no new content loads.

    AutoTrader’s search results page appends additional vehicles when the user
    scrolls toward the bottom.  This function repeatedly scrolls to the
    bottom of the page and waits until the page height stops increasing.

    Parameters
    ----------
    driver : webdriver.Chrome
        The Selenium driver controlling the browser.
    pause : float, optional
        How many seconds to wait between scrolls; increase this if your
        connection is slow.  The default is 2 seconds.
    """
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height


def _collect_page_links(driver: webdriver.Chrome) -> Set[str]:
    """Collect listing links from the current search results page."""
    # Wait for listing links to appear (up to 15s), retry once if blocked
    for attempt in range(3):
        try:
            WebDriverWait(driver, 15).until(
                lambda d: d.find_elements(By.CSS_SELECTOR, "a[href*='/a/']")
            )
            break
        except Exception:
            if attempt < 2:
                print(f"  Listings not loaded (attempt {attempt + 1}), retrying...")
                driver.refresh()
                time.sleep(3)
            else:
                print(f"  WARNING: no listing links found (page title: {driver.title})")
                return set()
    # Attempt to accept cookie consent if present.
    try:
        consent_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Accept')]")
        consent_button.click()
        time.sleep(1)
    except Exception:
        pass
    # Scroll to load all results
    scroll_to_load_all(driver)
    # Collect listing links
    links: Set[str] = set()
    anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='/a/']")
    for anchor in anchors:
        href = anchor.get_attribute("href")
        if href and "/a/" in href:
            clean = href.split("?")[0]
            links.add(clean)
    return links


def get_listing_urls(driver: webdriver.Chrome, search_url: str) -> List[str]:
    """Return all vehicle listing URLs, paginating through results.

    Iterates through pages using the ``rcs`` offset parameter (increments
    of 100) until a page returns no new listings.
    """
    all_links: Set[str] = set()
    page = 0
    while True:
        page_url = f"{search_url}&rcs={page * 100}"
        driver.get(page_url)
        page_links = _collect_page_links(driver)
        new_links = page_links - all_links
        if not new_links:
            break
        all_links.update(new_links)
        log(f"  Page {page + 1}: {len(new_links)} new URLs (total: {len(all_links)})")
        page += 1
    return list(all_links)


def parse_ngvdp_model(data: Dict[str, Any]) -> VehicleListing:
    """Extract fields from the ngVdpModel JSON into a VehicleListing.

    The keys used here mirror those documented in the Apify Autotrader
    scraper【298376264705576†L246-L255】.  Not every listing will have all fields
    populated; missing values remain ``None``. This function includes
    fallbacks to other potential keys and objects within the JSON data to
    handle site updates gracefully.

    Parameters
    ----------
    data : dict
        The JavaScript object retrieved from ``window.ngVdpModel``.

    Returns
    -------
    VehicleListing
        A populated VehicleListing dataclass instance.
    """
    listing = VehicleListing()
    ad_basic = data.get("adBasicInfo", {})
    hero = data.get("hero", {})
    seller = data.get("seller", {})
    price_analysis = data.get("priceAnalysis", {})
    condition_analysis = data.get("conditionAnalysis", {})
    dealer_trust = data.get("dealerTrust", {})
    vehicle = data.get("vehicle", {})  # Added for more robust extraction

    # Title: prefer hero.title, fallback to vehicle.title or build from parts
    title = hero.get("title") or vehicle.get("title")
    if not title:
        year = hero.get("year") or vehicle.get("year")
        make = hero.get("make") or vehicle.get("make")
        model = hero.get("model") or vehicle.get("model")
        title = " ".join(str(part) for part in (year, make, model) if part)
    listing.title = title

    listing.make = hero.get("make") or ad_basic.get("make")
    listing.trim = hero.get("trim") or ad_basic.get("trim")
    year_val = hero.get("year") or vehicle.get("year")
    if year_val is not None:
        try:
            listing.year = int(year_val)
        except (ValueError, TypeError):
            listing.year = None
    mileage_km = (
        hero.get("mileage")
        or ad_basic.get("odometer")
        or vehicle.get("mileage")
    )
    if mileage_km is not None:
        try:
            listing.mileage_km = int(mileage_km)
        except (ValueError, TypeError):
            listing.mileage_km = int(re.sub(r"[^0-9]", "", str(mileage_km)))

    price_val = (
        hero.get("price")
        or ad_basic.get("price")
        or vehicle.get("price")
    )
    if price_val:
        try:
            listing.price_cad = int(price_val)
        except (ValueError, TypeError):
            # Clean string by removing currency symbols and commas
            listing.price_cad = int(re.sub(r"[^0-9]", "", str(price_val)))
    else:
        listing.price_cad = None
    listing.status = (
        data.get("status")
        or hero.get("status")
        or vehicle.get("condition")
        or ad_basic.get("adType")
    )
    # Corrected seller name extraction based on gu.json
    listing.seller_name = (
        ad_basic.get("dealerCoName")
        or dealer_trust.get("dealerCompanyName")
        or seller.get("name")
        or seller.get("seller_name")
    )
    listing.is_private_seller = ad_basic.get(
        "isPrivate",
        seller.get("is_private_seller", seller.get("isPrivateSeller")),
    )
    listing.city = hero.get("location")
    city_province_name = dealer_trust.get("cityProvinceName")
    if city_province_name and "," in city_province_name:
        listing.province = city_province_name.split(",")[1].strip()
    else:
        listing.province = None
    specs = {
        spec.get("key"): spec.get("value")
        for spec in data.get("specifications", {}).get("specs", [])
    }
    listing.exterior_colour = (
        hero.get("exterior_colour")
        or hero.get("exteriorColour")
        or vehicle.get("exteriorColour")
        or specs.get("Exterior Colour")
    )
    listing.fuel_type = (
        hero.get("fuel_type")
        or hero.get("fuelType")
        or vehicle.get("fuelType")
        or specs.get("Fuel Type")
    )

    # The description can be a complex object, so we try to extract the text
    desc_obj = data.get("description", {})
    if isinstance(desc_obj, dict):
        desc_list = desc_obj.get("description")
        if isinstance(desc_list, list) and desc_list and isinstance(desc_list[0], dict):
            listing.description = desc_list[0].get("description")
        else:
            listing.description = str(desc_obj)
    else:
        listing.description = desc_obj

    # Sanitize the description to remove newlines that break the CSV format.
    if listing.description:
        listing.description = " ".join(listing.description.split())

    # New fields from CSV based on gu.json
    listing.ad_id = ad_basic.get("adId")
    listing.dealer_co_id = ad_basic.get("dealerCoId")
    car_insurance = data.get("carInsurance", {})
    listing.vin = (
        hero.get("vin")
        or ad_basic.get("vin")
        or car_insurance.get("vin")
        or vehicle.get("vin")
    )
    listing.price_analysis_description = (
        hero.get("priceAnalysisDescription")
        or price_analysis.get("priceAnalysisDescription")
    )
    avg_market_price_str = price_analysis.get("averageMarketPrice")
    if avg_market_price_str:
        try:
            cleaned_price = int(re.sub(r"[^0-9]", "", str(avg_market_price_str)))
            listing.average_market_price = cleaned_price
            if listing.price_cad is not None:
                listing.price_vs_market = listing.price_cad - cleaned_price
        except (ValueError, TypeError):
            listing.average_market_price = None
            listing.price_vs_market = None
    else:
        listing.average_market_price = None
        listing.price_vs_market = None

    listing.odometer_condition = condition_analysis.get("odometerCondition")
    listing.google_map_url = dealer_trust.get("googleMapUrl")
    listing.price_position = price_analysis.get("currentAskingPricePosition")
    listing.price_evaluation = price_analysis.get("priceEvaluation")

    # Infer has_driver_assistance from description (e.g. EyeSight for Subaru)
    desc_text = (listing.description or "").lower()
    listing.has_driver_assistance = (
        "eyesight" in desc_text or "driving assistant" in desc_text
        or "assistant de conduite" in desc_text
    )

    # Use structured feature highlights if available
    highlights = [
        h.lower() for h in
        data.get("featureHighlights", {}).get("highlights", [])
    ]

    # Infer has_carplay from highlights, title, or description
    title_text = (listing.title or "").lower()
    listing.has_carplay = (
        "carplay" in desc_text or "carplay" in title_text
        or any("carplay" in h for h in highlights)
    )

    # Infer has_cruise from highlights, title, or description
    listing.has_cruise = (
        "cruise control" in desc_text or "cruise" in title_text
        or any("cruise" in h for h in highlights)
    )

    # Set model from trim or title
    listing.model = listing.trim or listing.title

    return listing


def extract_listing_details(driver: webdriver.Chrome, url: str) -> VehicleListing:
    """Visit a vehicle page and extract details into a VehicleListing.

    This function first attempts to read the ``window.ngVdpModel`` object via
    JavaScript.  If that fails (e.g., due to script loading errors), it
    falls back to parsing the page HTML with BeautifulSoup.  The fallback
    extracts fewer fields but still returns a usable result.

    Parameters
    ----------
    driver : webdriver.Chrome
        The Selenium driver.
    url : str
        The URL of the vehicle listing page.

    Returns
    -------
    VehicleListing
        The extracted vehicle listing.
    """
    listing = VehicleListing(url=url)
    try:
        driver.get(url)
    except Exception:
        return listing
    # Wait for the ngVdpModel object to be available
    try:
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script("return typeof window.ngVdpModel !== 'undefined'")
        )
        data = driver.execute_script("return window.ngVdpModel")
        # Add the URL into the data for completeness
        listing = parse_ngvdp_model(data)
        listing.url = url
        log("  → extracted via ngVdpModel")
        return listing
    except Exception:
        pass
    log("  → ngVdpModel unavailable, falling back to BeautifulSoup")
    # Fallback: parse the page source via BeautifulSoup.  This will only
    # capture a subset of fields but ensures the scraper still returns a
    # record.
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    # Attempt to extract title
    h1 = soup.find("h1")
    if h1:
        listing.title = h1.get_text(strip=True)
    # Find mileage and location line (e.g., "26,000 km | Brossard |")
    km_text = None
    for text in soup.stripped_strings:
        if "km |" in text:
            km_text = text
            break
    if km_text:
        # Extract mileage as digits
        km_match = re.search(r"([0-9][0-9,. ]*)\s*km", km_text, re.IGNORECASE)
        if km_match:
            try:
                listing.mileage_km = int(re.sub(r"[^0-9]", "", km_match.group(1)))
            except Exception:
                listing.mileage_km = None
        # Attempt to infer year if it appears at the beginning of the title
        if listing.title:
            year_match = re.match(r"(20\d{2})", listing.title)
            if year_match:
                listing.year = int(year_match.group(1))
    # Price extraction
    price_tag = soup.find(string=re.compile(r"\$[\d,]+"))
    if price_tag:
        price_str = price_tag.strip()
        # Only use it if it looks like a plain price string, not JSON
        if len(price_str) < 30:
            listing.price_cad = int(re.sub(r"[^0-9]", "", price_str))
    # Seller name from page title (format: "YEAR MAKE MODEL | $PRICE | KM | ... by DEALER | CITY, PROV")
    page_title = soup.find("title")
    if page_title:
        title_text = page_title.get_text()
        by_match = re.search(r"for sale by (.+?) \|", title_text)
        if by_match:
            listing.seller_name = by_match.group(1).strip()
    return listing


def deduplicate_listings(listings: List[VehicleListing]) -> List[VehicleListing]:
    """Remove duplicate listings based on ad ID and dealer ID.

    This is a more reliable way to deduplicate than using seller, year and
    mileage, as IDs are guaranteed to be unique.

    Parameters
    ----------
    listings : List[VehicleListing]
        The list of VehicleListing objects to deduplicate.

    Returns
    -------
    List[VehicleListing]
        A new list with duplicates removed.
    """
    seen: Set[Tuple[Optional[str], Optional[str]]] = set()
    unique_listings: List[VehicleListing] = []
    for listing in listings:
        # Use ad_id and dealer_co_id for robust deduplication
        key = (listing.ad_id, listing.dealer_co_id)
        if not all(key) or key not in seen:
            if all(key):
                seen.add(key)
            unique_listings.append(listing)
    return unique_listings


# ── Search configurations ────────────────────────────────────────────────
# Each entry: (make/model URL path, output CSV filename)
OUTPUT_FILE = "used_suv_listings.csv"

VEHICLES = [
    "subaru/forester",
    "toyota/rav4",
    "honda/hr-v",
    "honda/cr-v",
    "hyundai/kona",
]

COMMON_PARAMS = (
    "?rcp=100"          # results per page (max 100)
    "&srt=39"           # sort order
    "&yRng=2016%2C"     # year 2016+
    "&pRng=15000"       # $20,000 max
    "&prx=200"          # radius in km
    "&prv=Quebec"       # province
    "&loc=H1X%203J1"    # postal code

    "&body=SUV"         # body type
    "&hprc=True"        # has price
    "&sts=Used"         # used vehicles only
    "&inMarket=advancedSearch"
)



def scrape_vehicle(driver: webdriver.Chrome, search_url: str,
                   make_model: str, output_file: str,
                   scrape_num: int, scrape_time: str) -> None:
    """Scrape listings for a single vehicle search and save to CSV.

    Updates ``last_scrape_timestamp`` for listings still present and sets
    ``is_deleted`` for listings that have disappeared from search results.
    """
    # Load existing data
    existing_df = None
    if os.path.exists(output_file):
        try:
            existing_df = pd.read_csv(output_file)
        except pd.errors.EmptyDataError:
            existing_df = None

    existing_urls: Set[str] = set()
    if existing_df is not None and "url" in existing_df.columns:
        existing_urls = set(existing_df["url"].dropna())

    log("Collecting listing URLs...")
    urls = get_listing_urls(driver, search_url)
    # Skip Ontario listings
    urls = [u for u in urls if "/ontario/" not in u.lower()]
    log(f"Found {len(urls)} vehicle URLs (after excluding Ontario)")

    scraped_url_set = set(urls)

    # Only scrape details for URLs not already in CSV
    new_urls = [u for u in urls if u not in existing_urls]
    if len(new_urls) < len(urls):
        print(f"Skipping {len(urls) - len(new_urls)} already scraped, {len(new_urls)} new")
    urls_to_process = new_urls[:MAX_LISTINGS] if MAX_LISTINGS else new_urls
    log(f"Processing {len(urls_to_process)} new listings...")
    listings: List[VehicleListing] = []
    for idx, url in enumerate(urls_to_process, start=1):
        log(f"  {idx}/{len(urls_to_process)}: {url}")
        try:
            listing = extract_listing_details(driver, url)
            listing.scrape_number = scrape_num
            listing.scrape_timestamp = scrape_time
            listing.last_scrape_timestamp = scrape_time
            listings.append(listing)
        except Exception as exc:
            print(f"  Failed: {exc}")
            continue
        if LISTING_PAUSE_SECS and idx < len(urls_to_process):
            time.sleep(LISTING_PAUSE_SECS)

    unique_listings = deduplicate_listings(listings)
    log(f"Keeping {len(unique_listings)} unique new listings after deduplication")
    new_df = pd.DataFrame([asdict(l) for l in unique_listings])

    # Update existing rows for this vehicle type
    if existing_df is not None and not existing_df.empty:
        url_pattern = f"/a/{make_model}/"
        vehicle_mask = existing_df["url"].str.contains(url_pattern, na=False)

        # Update last_scrape_timestamp for listings still present
        still_present = vehicle_mask & existing_df["url"].isin(scraped_url_set)
        existing_df.loc[still_present, "last_scrape_timestamp"] = scrape_time

        # Mark disappeared listings as deleted (only if not already deleted)
        # Skip if scrape returned 0 URLs — that's a scrape failure, not real deletions
        if scraped_url_set:
            disappeared = (vehicle_mask
                           & ~existing_df["url"].isin(scraped_url_set)
                           & existing_df["is_deleted"].isna())
            if disappeared.any():
                log(f"Marking {disappeared.sum()} listings as deleted")
                existing_df.loc[disappeared, "is_deleted"] = scrape_time
        else:
            log("Skipping deletion marking — no URLs scraped (possible scrape failure)")

        # Combine existing + new
        if not new_df.empty:
            combined = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            combined = existing_df
    else:
        combined = new_df if not new_df.empty else pd.DataFrame()

    if not combined.empty:
        cols = combined.columns.tolist()
        if "scrape_timestamp" in cols:
            cols.insert(0, cols.pop(cols.index("scrape_timestamp")))
        if "scrape_number" in cols:
            cols.insert(0, cols.pop(cols.index("scrape_number")))
        combined = combined[cols]

    combined.to_csv(output_file, index=False)
    log(f"Data saved to {output_file}")


SCATTER_HTML = "suv_scatter.html"
GIST_ID = "d47eb2219498fa62ddf729c9c96ccdb3"


def generate_scatter_html(csv_file: str, output_file: str,
                          max_price: int = 15000,
                          max_km: int = 200000) -> None:
    """Read the CSV and generate an interactive scatter plot HTML file."""
    df = pd.read_csv(csv_file)
    # Filter: active listings, within thresholds, known makes
    df = df[df["is_deleted"].isna()]
    df = df[(df["price_cad"] <= max_price) & (df["mileage_km"] <= max_km)]
    df = df[df["make"].isin(["Subaru", "Toyota", "Honda", "Hyundai"])]

    # Freshness tiers: "latest" (last scrape), "recent" (today/yesterday), "old"
    scrape_nums = df["scrape_number"].dropna().unique()
    max_scrape = df["scrape_number"].max() if len(scrape_nums) else 0
    has_multiple = len(scrape_nums) > 1
    today = datetime.now().date()
    yesterday = today - __import__("datetime").timedelta(days=1)

    records = []
    for _, row in df.iterrows():
        # Determine freshness tier
        is_latest = has_multiple and int(row["scrape_number"]) == int(max_scrape)
        scrape_date = None
        if pd.notna(row.get("scrape_timestamp")):
            try:
                scrape_date = datetime.fromisoformat(str(row["scrape_timestamp"])).date()
            except (ValueError, TypeError):
                pass
        is_recent = scrape_date in (today, yesterday) if scrape_date else False

        if is_latest:
            freshness = "latest"
        elif is_recent:
            freshness = "recent"
        else:
            freshness = "old"

        records.append({
            "make": row["make"],
            "year": int(row["year"]) if pd.notna(row["year"]) else 0,
            "mileage_km": int(row["mileage_km"]) if pd.notna(row["mileage_km"]) else 0,
            "price_cad": int(row["price_cad"]) if pd.notna(row["price_cad"]) else 0,
            "url": row["url"] or "",
            "title": row["title"] if pd.notna(row.get("title")) else "",
            "city": row["city"] if pd.notna(row.get("city")) else "",
            "has_cruise": bool(row["has_cruise"]) if pd.notna(row.get("has_cruise")) else False,
            "seller_name": row["seller_name"] if pd.notna(row.get("seller_name")) else "",
            "price_analysis": row["price_analysis_description"] if pd.notna(row.get("price_analysis_description")) else "",
            "freshness": freshness,
        })

    json_data = json.dumps(records, ensure_ascii=False)
    y_min = int(df["price_cad"].min() - 500) if not df.empty else 0

    html = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Used SUVs — Price vs Kilometres</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2"></script>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  html, body { height: 100%; height: 100dvh; }
  body { font-family: -apple-system, system-ui, sans-serif; background: #1a1a2e; color: #eee; padding: 12px; display: flex; flex-direction: column; }
  .controls { display: flex; justify-content: center; gap: 20px; margin-bottom: 8px; font-size: 0.9rem; }
  .controls label { display: inline-flex; align-items: center; gap: 5px; cursor: pointer; }
  .controls svg { vertical-align: middle; }
  .chart-wrap { flex: 1; min-height: 300px; position: relative; background: #16213e; border-radius: 12px; padding: 12px; }
  canvas { cursor: pointer; }
  #tooltip { position: fixed; background: #0f3460; border: 1px solid #555; border-radius: 8px; padding: 10px 14px; font-size: 0.82rem; pointer-events: none; display: none; z-index: 10; line-height: 1.5; }
  #tooltip .tt-title { font-weight: bold; }
  #tooltip .tt-detail { color: #ccc; }
  #tooltip .tt-city { color: #999; }
  #tooltip .tt-extra { color: #aaa; font-size: 0.78rem; margin-top: 4px; white-space: pre-line; }
  #tooltip .tt-open { display: inline-block; margin-top: 6px; background: #e74c3c; color: #fff; padding: 4px 10px; border-radius: 4px; font-size: 0.78rem; text-decoration: none; pointer-events: auto; }
</style>
</head>
<body>

<div class="controls">
  <label><input type="checkbox" checked data-make="Subaru"> <svg width="14" height="14"><circle cx="7" cy="7" r="5" fill="#7f8c8d" stroke="#fff" stroke-width="1"/></svg> Subaru</label>
  <label><input type="checkbox" checked data-make="Toyota"> <svg width="14" height="14"><polygon points="7,2 13,12 1,12" fill="#7f8c8d" stroke="#fff" stroke-width="1"/></svg> Toyota</label>
  <label><input type="checkbox" checked data-make="Honda"> <svg width="14" height="14"><rect x="2" y="2" width="10" height="10" fill="#7f8c8d" stroke="#fff" stroke-width="1"/></svg> Honda</label>
  <label><input type="checkbox" checked data-make="Hyundai"> <svg width="14" height="14"><polygon points="2,7 7,2 12,7 7,12" fill="#7f8c8d" stroke="#fff" stroke-width="1"/></svg> Hyundai</label>
</div>

<div class="chart-wrap">
  <canvas id="chart"></canvas>
</div>

<div id="tooltip">
  <div class="tt-title"></div>
  <div class="tt-detail"></div>
  <div class="tt-city"></div>
  <div class="tt-extra"></div>
  <a class="tt-open" href="#" target="_blank">Open listing</a>
</div>

<script>
const SHAPES = { Subaru: 'circle', Toyota: 'triangle', Honda: 'rect', Hyundai: 'rectRot' };
const COLORS = { latest: '#ffd700', recent: '#2ecc71', old: '#7f8c8d' };
const SIZES  = { latest: { r: 7, hr: 9, bw: 0 }, recent: { r: 7, hr: 9, bw: 0 }, old: { r: 7, hr: 9, bw: 0 } };

const data = ''' + json_data + ''';

const datasets = {};
Object.keys(SHAPES).forEach(make => {
  datasets[make] = { label: make, data: [], meta: [], backgroundColor: [], borderColor: [], pointRadius: [], pointHoverRadius: [], pointBorderColor: [], pointBorderWidth: [], pointStyle: SHAPES[make], showLine: false };
});

data.forEach(d => {
  const ds = datasets[d.make];
  ds.data.push({ x: d.mileage_km, y: d.price_cad });
  ds.meta.push(d);
  const f = d.freshness || 'old';
  const c = COLORS[f], s = SIZES[f];
  ds.backgroundColor.push(c);
  ds.borderColor.push(c);
  ds.pointRadius.push(s.r);
  ds.pointHoverRadius.push(s.hr);
  ds.pointBorderColor.push(c);
  ds.pointBorderWidth.push(0);
});

Chart.register(ChartDataLabels);

let selectedKey = null;
function showTooltip(item, cx, cy) {
  const tt = document.getElementById('tooltip');
  tt.querySelector('.tt-title').textContent = item.title;
  tt.querySelector('.tt-detail').textContent = item.year + ' \\u00b7 ' + item.mileage_km.toLocaleString() + ' km \\u00b7 $' + item.price_cad.toLocaleString();
  tt.querySelector('.tt-city').textContent = item.city;
  const cruise = item.has_cruise ? 'Yes' : 'No';
  const extra = 'Cruise: ' + cruise + '\\n' + item.seller_name + (item.price_analysis ? '\\n' + item.price_analysis : '');
  tt.querySelector('.tt-extra').textContent = extra;
  tt.querySelector('.tt-open').href = item.url;
  tt.style.display = 'block';
  const rect = tt.getBoundingClientRect();
  const left = Math.min(cx + 14, window.innerWidth - rect.width - 10);
  const top = Math.min(cy - 10, window.innerHeight - rect.height - 10);
  tt.style.left = left + 'px';
  tt.style.top = Math.max(10, top) + 'px';
}
document.addEventListener('click', function(e) {
  if (e.target.closest('#tooltip') || e.target.tagName === 'CANVAS') return;
  document.getElementById('tooltip').style.display = 'none';
  selectedKey = null;
});

const ctx = document.getElementById('chart').getContext('2d');
const chart = new Chart(ctx, {
  type: 'scatter',
  data: { datasets: Object.values(datasets) },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 600 },
    scales: {
      x: {
        min: 59000,
        title: { display: true, text: 'Kilometres', color: '#aaa', font: { size: 14 } },
        ticks: { color: '#888', callback: v => (v/1000).toFixed(0) + 'k' },
        grid: { color: '#2a2a4a' }
      },
      y: {
        min: ''' + str(y_min) + ''',
        title: { display: true, text: 'Price (CAD)', color: '#aaa', font: { size: 14 } },
        ticks: { color: '#888', callback: v => '$' + v.toLocaleString() },
        grid: { color: '#2a2a4a' }
      }
    },
    plugins: {
      legend: { display: false },
      tooltip: { enabled: false },
      datalabels: {
        align: 'bottom',
        offset: 4,
        color: '#999',
        font: { size: 9 },
        formatter(value, ctx) {
          const item = ctx.dataset.meta[ctx.dataIndex];
          return item.make + '\\n' + item.year;
        }
      }
    },
    onClick(e, elements) {
      const tt = document.getElementById('tooltip');
      if (!elements.length) { tt.style.display = 'none'; selectedKey = null; return; }
      const el = elements[0];
      const ds = chart.data.datasets[el.datasetIndex];
      const item = ds.meta[el.index];
      const key = el.datasetIndex + '-' + el.index;
      if (selectedKey === key) {
        window.open(item.url, '_blank');
        return;
      }
      selectedKey = key;
      showTooltip(item, e.native.clientX, e.native.clientY);
    },
    onHover(e, elements) {
      const tt = document.getElementById('tooltip');
      if (!elements.length) { if (!('ontouchstart' in window)) { tt.style.display = 'none'; selectedKey = null; } return; }
      if ('ontouchstart' in window) return;
      const el = elements[0];
      const ds = chart.data.datasets[el.datasetIndex];
      const item = ds.meta[el.index];
      selectedKey = el.datasetIndex + '-' + el.index;
      showTooltip(item, e.native.clientX, e.native.clientY);
    }
  }
});

document.querySelectorAll('.controls input').forEach(cb => {
  cb.addEventListener('change', () => {
    const idx = chart.data.datasets.findIndex(ds => ds.label === cb.dataset.make);
    if (idx >= 0) { chart.setDatasetVisibility(idx, cb.checked); chart.update(); }
  });
});
</script>

</body>
</html>'''

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)
    log(f"Scatter plot written to {output_file}")


def main() -> None:
    """Scrape all configured vehicles, one at a time.

    The search URL is assembled with filters for make (BMW), model (X3), year
    range (2021–2023), vehicle condition (Used), radius (500 km) and postal
    code (H1X 3J1).  The ``rcp`` parameter is set to 100 to request the
    maximum number of results per page, as suggested by the AutoTrader
    scraping documentation【298376264705576†L224-L233】.  Additional pages
    (``rcs`` offsets) could be processed in a loop if required.
    """
    # Compute scrape number and timestamp once for the entire run
    scrape_time = datetime.now().isoformat()
    scrape_num = 1
    if os.path.exists(OUTPUT_FILE):
        try:
            existing_df = pd.read_csv(OUTPUT_FILE)
            if "scrape_number" in existing_df.columns:
                scrape_num = int(existing_df["scrape_number"].max()) + 1
        except (pd.errors.EmptyDataError, KeyError, ValueError):
            pass

    driver = create_driver(headless=False)
    try:
        # Warm up the session by visiting the homepage first so the first
        # search request is not treated as a cold bot hit by Incapsula.
        log("Warming up session...")
        driver.get("https://www.autotrader.ca/")
        time.sleep(5)
        for make_model in VEHICLES:
            search_url = (
                f"https://www.autotrader.ca/cars/{make_model}/qc/montr%c3%a9al/"
                + COMMON_PARAMS
            )
            print(f"\n{'='*60}")
            print(f"Scraping {make_model}")
            print(f"{'='*60}")
            scrape_vehicle(driver, search_url, make_model, OUTPUT_FILE,
                           scrape_num, scrape_time)
    finally:
        driver.quit()

    generate_scatter_html(OUTPUT_FILE, SCATTER_HTML)
    log("Scatter plot updated")

    # Upload to GitHub Gist for iMessage-friendly sharing
    try:
        subprocess.run(
            ["gh", "gist", "edit", GIST_ID, "-f", SCATTER_HTML, SCATTER_HTML],
            check=True, capture_output=True, text=True,
        )
        log(f"Gist updated: https://gist.githack.com/winzee/{GIST_ID}/raw/{SCATTER_HTML}")
    except Exception as e:
        log(f"Gist upload failed (non-fatal): {e}")


if __name__ == "__main__":
    main()
