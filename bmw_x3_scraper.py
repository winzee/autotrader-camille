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
mileage.  Finally, the collected data are written to ``bmw_x3_used_2021_2023.csv``.

To run this script you will need the following Python packages:

    pip install selenium webdriver-manager beautifulsoup4 pandas

Note: AutoTrader may update their site or deploy anti‑bot measures.  If you
encounter issues, consider adding random delays, rotating user agents or
using a proxy.  Always consult the website’s terms of service before
scraping.
"""

import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime
import os
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
    # Fields from `bmw_x3_used_2021_2023.csv`
    has_driver_assistance: Optional[bool] = None
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
    # Setting a realistic user‑agent helps to avoid being blocked by the
    # site’s anti‑bot defences.  Feel free to customise this value.
    options.add_argument(
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/114.0 Safari/537.36"
    )
    # Explicitly specify the driver version to match the browser version from the error log.
    # This is a workaround for when webdriver-manager fails to auto-detect the correct version.
    service = Service(ChromeDriverManager(driver_version="130.0.6723.69").install())
    driver = webdriver.Chrome(service=service, options=options)
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


def get_listing_urls(driver: webdriver.Chrome, search_url: str) -> List[str]:
    """Return a list of unique vehicle listing URLs from the search page.

    The function navigates to the provided search URL, accepts cookies if
    prompted and then scrolls through the page to trigger lazy loading of
    additional results.  It extracts all anchor tags whose ``href``
    attribute contains ``'/a/'``, which is the path segment used by
    AutoTrader.ca for individual vehicle listings.

    Parameters
    ----------
    driver : webdriver.Chrome
        The Selenium driver.
    search_url : str
        The fully formed AutoTrader search URL.

    Returns
    -------
    List[str]
        A list of distinct vehicle listing URLs (without query parameters).
    """
    driver.get(search_url)
    # Allow the page to load initial content.
    time.sleep(3)
    # Attempt to accept cookie consent if present.
    try:
        consent_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Accept')]" )
        consent_button.click()
        time.sleep(1)
    except Exception:
        # No consent pop‑up detected
        pass
    # Scroll to load all results
    scroll_to_load_all(driver)
    # Collect listing links
    links: Set[str] = set()
    anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='/a/']")
    for anchor in anchors:
        href = anchor.get_attribute("href")
        if href and "/a/" in href:
            # Strip any query parameters for de‑duplication
            clean = href.split("?")[0]
            links.add(clean)
    return list(links)


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

    listing.trim = hero.get("trim") or ad_basic.get("trim")
    listing.year = hero.get("year") or vehicle.get("year")
    mileage_km = (
        hero.get("mileage_km")
        or hero.get("mileageKm")
        or hero.get("mileage")
        or vehicle.get("mileage_km")
        or vehicle.get("mileageKm")
        or vehicle.get("mileage")
    )
    if mileage_km is not None:
        try:
            listing.mileage_km = int(mileage_km)
        except (ValueError, TypeError):
            listing.mileage_km = int(re.sub(r"[^0-9]", "", str(mileage_km)))

    price_val = (
        hero.get("price_cad")
        or vehicle.get("price_cad")
        or ad_basic.get("price")
        or hero.get("price_str")
        or hero.get("priceStr")
        or vehicle.get("price_str")
        or vehicle.get("priceStr")
        or ad_basic.get("priceString")
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

    # Infer has_driver_assistance from description
    desc_text = (listing.description or "").lower()
    listing.has_driver_assistance = (
        "driving assistant" in desc_text or "assistant de conduite" in desc_text
    )

    # Set model based on trim and description
    trim_text = (listing.trim or "").lower()
    desc_text = (listing.description or "").lower()
    if "30i" in trim_text:
        listing.model = "30i"
    elif "40i" in trim_text:
        listing.model = "40i"
    elif "30i" in desc_text:
        listing.model = "30i"
    elif "40i" in desc_text:
        listing.model = "40i"
    elif "30" in trim_text:
        listing.model = "30i"
    elif "40" in trim_text:
        listing.model = "40i"
    else:
        listing.model = None

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
        return listing
    except Exception:
        pass
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
    price_tag = soup.find(text=re.compile(r"\$[\d,]+"))
    if price_tag:
        price_str = price_tag.strip()
        listing.price_cad = int(re.sub(r"[^0-9]", "", price_str))
    # Seller name (e.g., appears repeatedly on the page)
    seller_match = soup.find(text=re.compile(r"[A-Za-z].*BMW"))
    if seller_match:
        listing.seller_name = seller_match.strip()
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

def main() -> None:
    """Run the scraper and write results to CSV.

    The search URL is assembled with filters for make (BMW), model (X3), year
    range (2021–2023), vehicle condition (Used), radius (500 km) and postal
    code (H1X 3J1).  The ``rcp`` parameter is set to 100 to request the
    maximum number of results per page, as suggested by the AutoTrader
    scraping documentation【298376264705576†L224-L233】.  Additional pages
    (``rcs`` offsets) could be processed in a loop if required.
    """
    output_file = "bmw_x3_used_2021_2023.csv"
    scrape_start_time = datetime.now().isoformat()
    scrape_num = 1
    if os.path.exists(output_file):
        try:
            existing_df = pd.read_csv(output_file)
            if "scrape_number" in existing_df.columns:
                scrape_num = int(existing_df["scrape_number"].max()) + 1
        except (pd.errors.EmptyDataError, KeyError, ValueError):
            # File is empty or doesn't have the column, start with 1
            pass
    # Build the search URL.  If you wish to narrow the search further you can
    # modify these parameters; see AutoTrader.ca’s URL structure for
    # additional options.
    search_url = (
        "https://www.autotrader.ca/cars/bmw/x3/"
        "?fr=2021&to=2023"  # year range
        "&prx=500"          # radius in km
        "&loc=H1X%203J1"    # postal code
        "&prv=Quebec"       # province
        "&sts=Used"         # used vehicles only
        "&rcp=100"          # results per page (max 100)
        "&rcs=0"            # result offset (0 for first page)
    )
    driver = create_driver(headless=True)
    try:
        print("Collecting listing URLs…")
        urls = get_listing_urls(driver, search_url)
        print(f"Found {len(urls)} vehicle URLs")
        # Limit to 5 for debugging
        urls_to_process = urls
        print(f"Processing the first {len(urls_to_process)} listings for debugging...")
        listings: List[VehicleListing] = []
        for idx, url in enumerate(urls_to_process, start=1):
            print(f"Processing {idx}/{len(urls_to_process)}: {url}")
            try:
                listing = extract_listing_details(driver, url)
                listing.scrape_number = scrape_num
                listing.scrape_timestamp = scrape_start_time
                listings.append(listing)
            except Exception as exc:
                print(f"Failed to process {url}: {exc}")
                continue
    finally:
        driver.quit()
    # Deduplicate based on seller, year and mileage
    unique_listings = deduplicate_listings(listings)
    print(f"Keeping {len(unique_listings)} unique listings after deduplication")
    # Convert to DataFrame and save to CSV
    df = pd.DataFrame([asdict(l) for l in unique_listings])

    # Reorder columns to have scrape_number and scrape_timestamp first
    if not df.empty:
        cols = df.columns.tolist()
        if "scrape_timestamp" in cols:
            cols.insert(0, cols.pop(cols.index("scrape_timestamp")))
        if "scrape_number" in cols:
            cols.insert(0, cols.pop(cols.index("scrape_number")))
        df = df[cols]

    file_exists = os.path.exists(output_file)
    df.to_csv(output_file, mode='a', header=not file_exists, index=False)
    print(f"Data saved to {output_file}")


if __name__ == "__main__":
    main()
