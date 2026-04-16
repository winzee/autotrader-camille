"""
bmw_x3_scraper.py
-------------------

Scrapes used SUV listings from AutoTrader.ca for a configurable list of
make/model combinations (see ``VEHICLES``), centered on postal code H1X 3J1
(Montreal). Results are written to ``used_suv_listings.csv`` and an
interactive scatter plot is regenerated and pushed to GitHub Pages on every
run. Filename is kept as ``bmw_x3_scraper.py`` for historical reasons — the
scraper started life targeting BMW X3 listings.

Because autotrader.ca is a client-side JavaScript app, it cannot be scraped
reliably with ``requests`` alone. The script uses Selenium with
``webdriver_manager`` to download a matching ChromeDriver, launches Chrome
for Testing in headless mode, scrolls search pages to trigger lazy loads,
collects listing URLs, and visits each detail page to read an in-page
JavaScript data object.

==============================================================================
  AutoTrader dual-app architecture (IMPORTANT — read before modifying)
==============================================================================

Since AutoTrader's 2024 AutoScout24 acquisition, autotrader.ca runs TWO
parallel front-end applications and A/B-routes sessions between them:

  * Next.js (the "new" app) — uses ``/offers/<slug>`` detail URLs and
    exposes ``window.__NEXT_DATA__.props.pageProps.listingDetails``.

  * Angular (the "legacy" app) — uses ``/a/<make>/<model>/...`` detail
    URLs and exposes ``window.ngVdpModel``. Still fully functional.

The routing is server-side and appears time-windowed: a given client IP
can land on Next.js at 7 AM and on Angular at 10 AM on the same day. Neither
cookies, user-agent, nor fingerprint overrides the decision — see
``investigation_notes.md`` for the full reproduction and data (2026-04-15).

Earlier versions of this scraper (commit e97c882 and before) parsed
``ngVdpModel`` only. The scraper was then migrated to ``__NEXT_DATA__``
under the incorrect assumption that the Angular app was being deprecated.
On 2026-04-15, 16 of 16 sessions landed on Angular across four prx-variant
tests, with Angular detail pages still serving complete ``ngVdpModel`` data.
The scraper now handles BOTH paths.

How the dual path works:

  1. Search page — ``_collect_page_links()`` accepts either ``/offers/``
     or ``/a/`` anchors. Both apps are paginated with ``&page=N``; Angular
     also eagerly loads results via infinite scroll, so
     ``scroll_to_load_all()`` runs on every page fetch.

  2. Detail page — ``extract_listing_details()`` tries parsers in order:
       Tier 1a: ``window.__NEXT_DATA__`` -> ``parse_next_data()``
       Tier 1b: ``window.ngVdpModel``    -> ``parse_ngvdp_model()``
       Tier 2:  BeautifulSoup on embedded ``<script id="__NEXT_DATA__">``
       Tier 3:  BeautifulSoup on raw HTML (fragmentary fallback)

  3. ``main()`` warms up a session (landing page + cookie consent), loads
     the first search URL, logs which app it landed on, and proceeds
     regardless. No retry loop — we accept whatever app we are served.

==============================================================================
  Sample schema reference
==============================================================================

``gu.json`` in the repo is a sample dump of the Angular ``ngVdpModel``
object. Use it to navigate nested keys (``adBasicInfo``, ``hero``,
``priceAnalysis``, ``conditionAnalysis``, ``dealerTrust``, ``specifications``,
``featureHighlights``, etc.) when adding new fields.

There is no equivalent dump for the Next.js ``listingDetails`` object; its
shape is visible in ``parse_next_data()``. The two schemas differ enough
that each parser is maintained separately rather than being unified.

==============================================================================
  Running
==============================================================================

    source venv/bin/activate
    pip install selenium webdriver-manager beautifulsoup4 pandas
    python bmw_x3_scraper.py

Requires Chrome for Testing installed at
``/Applications/Google Chrome for Testing.app`` (see ``create_driver``).
CSV output is append-only with last-seen and soft-deletion tracking
(``last_scrape_timestamp``, ``is_deleted``). Duplicates are removed by
``(ad_id, dealer_co_id)`` before write.

If you revisit this code after a long absence and things break:
  1. Check ``investigation_notes.md`` for the latest known routing behaviour.
  2. Load a search URL in a real browser and check which globals exist
     (``__NEXT_DATA__`` vs ``ngVdpModel``), and whether the listing-link
     format has changed.
  3. If AutoTrader has introduced a THIRD app, add a Tier 1c parser
     alongside the existing two rather than replacing either.
  4. AutoTrader may also deploy anti-bot measures. If you hit CAPTCHAs, try
     random delays, rotating user agents, or a residential proxy. The
     ``_collect_page_links`` helper already detects and pauses for manual
     CAPTCHA resolution when the page returns very few anchors.
"""

# ── Scraper settings ─────────────────────────────────────────────────────
MAX_LISTINGS = None       # Max listings per vehicle (None = all)
LISTING_PAUSE_SECS = 5   # Pause between each listing scrape

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
    image_url: Optional[str] = None
    is_dealer: Optional[bool] = None
    carfax_url: Optional[str] = None
    body_color: Optional[str] = None
    body_color_original: Optional[str] = None
    comfort_equipment: Optional[str] = None
    safety_equipment: Optional[str] = None
    model_version: Optional[str] = None
    consumption_city: Optional[float] = None
    consumption_highway: Optional[float] = None
    transmission: Optional[str] = None
    upholstery_color: Optional[str] = None
    last_scrape_timestamp: Optional[str] = None
    is_deleted: Optional[str] = None
    source: Optional[str] = None


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
    # Attempt to accept cookie consent if present.
    try:
        consent_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Accept')]")
        consent_button.click()
        time.sleep(1)
    except Exception:
        pass
    # AutoTrader listing URLs use /offers/ (previously /a/)
    LISTING_SELECTORS = ["a[href*='/offers/']"]

    # Wait for listing links to appear (up to 15s), retry on failure
    for attempt in range(3):
        try:
            WebDriverWait(driver, 15).until(
                lambda d: any(d.find_elements(By.CSS_SELECTOR, sel)
                              for sel in LISTING_SELECTORS)
            )
            break
        except Exception:
            if attempt < 2:
                print(f"  Listings not loaded (attempt {attempt + 1}), retrying...")
                driver.refresh()
                time.sleep(3)
            else:
                # Check for CAPTCHA — a blocked page has very few links
                total_links = len(driver.find_elements(By.TAG_NAME, "a"))
                if total_links < 20:
                    log("CAPTCHA detected — please solve it in the browser. Waiting up to 2 min...")
                    for _ in range(24):
                        time.sleep(5)
                        if any(driver.find_elements(By.CSS_SELECTOR, sel)
                               for sel in LISTING_SELECTORS):
                            log("CAPTCHA solved, continuing...")
                            break
                    else:
                        log("Timed out waiting for CAPTCHA resolution")
                        return set()
                    break  # CAPTCHA solved — proceed to scroll + collect
                print(f"  WARNING: no listing links found (page title: {driver.title})")
                return set()
    # Scroll to load all results
    scroll_to_load_all(driver)
    # Collect listing links from all known selectors
    links: Set[str] = set()
    for sel in LISTING_SELECTORS:
        for anchor in driver.find_elements(By.CSS_SELECTOR, sel):
            href = anchor.get_attribute("href")
            if href:
                clean = href.split("?")[0]
                links.add(clean)
    return links


def get_listing_urls(driver: webdriver.Chrome, search_url: str,
                     first_page_loaded: bool = False) -> List[str]:
    """Return all vehicle listing URLs, paginating through results.

    Uses ``page=`` parameter (AutoScout24 platform) for pagination.
    If *first_page_loaded* is True, collects links from the current page
    first before advancing to page 2.
    """
    RESULTS_PER_PAGE = 100  # matches rcp=100 in COMMON_PARAMS
    all_links: Set[str] = set()
    page = 1
    while True:
        if page == 1 and first_page_loaded:
            # Page already loaded by caller — just collect links
            pass
        else:
            page_url = f"{search_url}&page={page}"
            driver.get(page_url)
        page_links = _collect_page_links(driver)
        new_links = page_links - all_links
        if not new_links:
            break
        all_links.update(new_links)
        log(f"  Page {page}: {len(new_links)} new URLs (total: {len(all_links)})")
        # If this page returned fewer than a full page, there is no next page.
        if len(page_links) < RESULTS_PER_PAGE:
            break
        page += 1
    return list(all_links)


def parse_next_data(data: Dict[str, Any]) -> VehicleListing:
    """Extract fields from __NEXT_DATA__ listingDetails into a VehicleListing.

    Parameters
    ----------
    data : dict
        The ``listingDetails`` object from
        ``window.__NEXT_DATA__.props.pageProps.listingDetails``.

    Returns
    -------
    VehicleListing
        A populated VehicleListing dataclass instance.
    """
    listing = VehicleListing(source="autotrader")
    vehicle = data.get("vehicle", {})
    seller_obj = data.get("seller", {})
    prices = data.get("prices", {})
    identifier = data.get("identifier", {})
    location = data.get("location", {})
    tracking = data.get("trackingParams", {})
    raw_data = vehicle.get("rawData", {})
    raw_clf = raw_data.get("classification", {})
    trader_info = data.get("traderProvisioningInfo", {})
    wltp = vehicle.get("wltp") or {}
    equipment = vehicle.get("equipment", {})

    # ── Core fields ─────────────────────────────────────────────────────
    listing.make = vehicle.get("make") or (raw_clf.get("make") or {}).get("formatted")
    listing.model = vehicle.get("model") or (raw_clf.get("model") or {}).get("formatted")

    year_val = vehicle.get("modelYear") or raw_clf.get("modelYear")
    if year_val is not None:
        try:
            listing.year = int(year_val)
        except (ValueError, TypeError):
            listing.year = None

    listing.title = " ".join(
        str(p) for p in (listing.year, listing.make, listing.model) if p
    )

    # ── Price (raw integers) ────────────────────────────────────────────
    price_val = (
        (prices.get("public") or {}).get("priceRaw")
        or (prices.get("dealer") or {}).get("priceRaw")
        or tracking.get("classified_price")
    )
    if price_val is not None:
        try:
            listing.price_cad = int(price_val)
        except (ValueError, TypeError):
            listing.price_cad = None

    # ── Mileage (raw km) ───────────────────────────────────────────────
    mileage_val = (
        vehicle.get("mileageInKmRaw")
        or (raw_data.get("condition", {}).get("mileageInKm") or {}).get("raw")
    )
    if mileage_val is not None:
        try:
            listing.mileage_km = int(mileage_val)
        except (ValueError, TypeError):
            listing.mileage_km = None

    # ── Seller ──────────────────────────────────────────────────────────
    listing.seller_name = seller_obj.get("companyName")
    listing.is_dealer = seller_obj.get("isDealer", seller_obj.get("type") == "Dealer")
    listing.is_private_seller = not listing.is_dealer if listing.is_dealer is not None else None

    # ── Location ────────────────────────────────────────────────────────
    listing.city = location.get("city")
    dealer_info = seller_obj.get("dealer") or {}
    listing.province = dealer_info.get("region")

    # ── Vehicle details ─────────────────────────────────────────────────
    listing.model_version = vehicle.get("modelVersionInput") or raw_clf.get("modelVersionInput")
    listing.trim = listing.model_version
    listing.body_color = vehicle.get("bodyColor") or vehicle.get("bodyColorOriginal")
    listing.body_color_original = vehicle.get("bodyColorOriginal")
    listing.exterior_colour = listing.body_color
    fuel_cat = vehicle.get("fuelCategory") or {}
    listing.fuel_type = fuel_cat.get("formatted") if isinstance(fuel_cat, dict) else None
    listing.transmission = vehicle.get("transmissionType")
    listing.upholstery_color = vehicle.get("upholsteryColor")
    listing.status = (vehicle.get("legalCategories") or [None])[0]

    # ── VIN ─────────────────────────────────────────────────────────────
    v_ident = vehicle.get("identifier") or {}
    listing.vin = v_ident.get("vin") if isinstance(v_ident, dict) else None

    # ── IDs ─────────────────────────────────────────────────────────────
    listing.ad_id = identifier.get("crossReferenceId")
    listing.dealer_co_id = data.get("externalCustomerId")

    # ── Image ───────────────────────────────────────────────────────────
    images = data.get("images") or []
    listing.image_url = images[0] if images else None

    # ── CarFax ──────────────────────────────────────────────────────────
    listing.carfax_url = trader_info.get("CarFaxReportUrl") or None

    # ── Consumption (WLTP raw values) ───────────────────────────────────
    city_cons = wltp.get("consumptionCity") or {}
    hwy_cons = wltp.get("consumptionHighway") or {}
    listing.consumption_city = city_cons.get("raw") if isinstance(city_cons, dict) else None
    listing.consumption_highway = hwy_cons.get("raw") if isinstance(hwy_cons, dict) else None

    # ── Equipment (comma-delimited strings) ─────────────────────────────
    comfort_items = equipment.get("comfortAndConvenience") or []
    safety_items = equipment.get("safetyAndSecurity") or []
    listing.comfort_equipment = ", ".join(
        item.get("id", "") for item in comfort_items if item.get("id")
    ) or None
    listing.safety_equipment = ", ".join(
        item.get("id", "") for item in safety_items if item.get("id")
    ) or None

    # ── Description ─────────────────────────────────────────────────────
    desc = data.get("description") or ""
    if isinstance(desc, str):
        listing.description = " ".join(desc.split())
    else:
        listing.description = None

    # ── Price analysis ──────────────────────────────────────────────────
    listing.price_evaluation = data.get("price", {}).get("priceEvaluation")
    public_prices = prices.get("public") or {}
    eval_ranges = public_prices.get("evaluationRanges") or []
    fair_range = next((r for r in eval_ranges if r.get("category") == 2), None)
    if fair_range and listing.price_cad is not None:
        fair_min = fair_range.get("minimum")
        fair_max = fair_range.get("maximum")
        if fair_min is not None and fair_max is not None:
            market_mid = int((fair_min + fair_max) / 2)
            listing.average_market_price = market_mid
            listing.price_vs_market = listing.price_cad - market_mid
            diff = abs(listing.price_vs_market)
            if listing.price_vs_market < 0:
                listing.price_analysis_description = f"${diff:,} BELOW MARKET"
            else:
                listing.price_analysis_description = f"${diff:,} ABOVE MARKET"

    # ── Inferred booleans ───────────────────────────────────────────────
    desc_text = (listing.description or "").lower()
    title_text = (listing.title or "").lower()
    version_text = (listing.model_version or "").lower()
    all_equipment = (listing.comfort_equipment or "") + " " + (listing.safety_equipment or "")
    equip_lower = all_equipment.lower()

    listing.has_cruise = "cruise" in equip_lower or "cruise" in desc_text or "cruise" in title_text
    listing.has_carplay = "carplay" in desc_text or "carplay" in title_text or "carplay" in version_text
    listing.has_driver_assistance = (
        "eyesight" in desc_text or "driving assistant" in desc_text
        or "assistant de conduite" in desc_text
    )

    return listing


def parse_ngvdp_model(data: Dict[str, Any]) -> VehicleListing:
    """Parse a vehicle detail page from AutoTrader's LEGACY Angular app.

    The legacy Angular detail page injects a ``window.ngVdpModel`` object
    with the full listing data. This parser was the scraper's only data
    source until commit e97c882 (2026 migration to ``__NEXT_DATA__``) and
    was resurrected on 2026-04-15 when we discovered the Angular app is
    still very much alive and actively serves traffic. See the module
    docstring and ``investigation_notes.md`` for context.

    The sample schema in ``gu.json`` documents the nested structure this
    function navigates. Key top-level objects used here:

      * ``adBasicInfo``    - ad ID, dealer ID, VIN, odometer, price, adType
      * ``hero``           - title, make, model, year, location, mileage
      * ``priceAnalysis``  - price position vs market, average market price
      * ``conditionAnalysis`` - odometer condition rating
      * ``dealerTrust``    - dealer company name, cityProvinceName, map URL
      * ``description``    - listing description (structured, needs unwrap)
      * ``specifications`` - array of ``{key, value}`` spec pairs
      * ``featureHighlights`` - list of marketing highlights
      * ``carInsurance``   - carries VIN fallback
      * ``vehicle``        - legacy container, used as secondary fallback

    This schema differs enough from the Next.js ``listingDetails`` object
    (parsed by ``parse_next_data``) that we keep the two parsers separate
    rather than trying to unify them. Both populate the same
    ``VehicleListing`` dataclass so downstream CSV code doesn't care which
    path produced a given row.

    Missing fields remain ``None`` - not every listing has every field.
    """
    listing = VehicleListing(source="autotrader")
    ad_basic = data.get("adBasicInfo", {})
    hero = data.get("hero", {})
    seller = data.get("seller", {})
    price_analysis = data.get("priceAnalysis", {})
    condition_analysis = data.get("conditionAnalysis", {})
    dealer_trust = data.get("dealerTrust", {})
    vehicle = data.get("vehicle", {})

    # Title: prefer hero.title, fallback to vehicle.title or build from parts.
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
            listing.price_cad = int(re.sub(r"[^0-9]", "", str(price_val)))

    listing.status = (
        data.get("status")
        or hero.get("status")
        or vehicle.get("condition")
        or ad_basic.get("adType")
    )

    # Seller name comes from dealerCoName on dealer ads; fall back through
    # the various objects that have historically carried it across schema
    # revisions.
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

    # Specifications come as a list of {key, value} dicts; flatten to dict.
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

    # ``description`` may be a nested object ``{description: [{description: str}]}``
    # or a plain string depending on the listing. Unwrap defensively.
    desc_obj = data.get("description", {})
    if isinstance(desc_obj, dict):
        desc_list = desc_obj.get("description")
        if isinstance(desc_list, list) and desc_list and isinstance(desc_list[0], dict):
            listing.description = desc_list[0].get("description")
        else:
            listing.description = str(desc_obj) if desc_obj else None
    else:
        listing.description = desc_obj
    if listing.description:
        # Collapse whitespace so CSV rows stay single-line.
        listing.description = " ".join(listing.description.split())

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
            pass

    listing.odometer_condition = condition_analysis.get("odometerCondition")
    listing.google_map_url = dealer_trust.get("googleMapUrl")
    listing.price_position = price_analysis.get("currentAskingPricePosition")
    listing.price_evaluation = price_analysis.get("priceEvaluation")

    # Feature flags are derived heuristically from text + structured highlights.
    # Same approach as parse_next_data so both parsers produce comparable rows.
    desc_text = (listing.description or "").lower()
    title_text = (listing.title or "").lower()
    highlights = [
        h.lower() for h in
        data.get("featureHighlights", {}).get("highlights", [])
    ]
    listing.has_driver_assistance = (
        "eyesight" in desc_text or "driving assistant" in desc_text
        or "assistant de conduite" in desc_text
    )
    listing.has_carplay = (
        "carplay" in desc_text or "carplay" in title_text
        or any("carplay" in h for h in highlights)
    )
    listing.has_cruise = (
        "cruise control" in desc_text or "cruise" in title_text
        or any("cruise" in h for h in highlights)
    )

    # In the Angular schema, ``model`` is not reliably populated as a standalone
    # field, so derive it from trim or title as the old scraper did.
    listing.model = listing.trim or listing.title

    return listing


# ============================================================================
#  extract_listing_details - dual-path dispatcher
# ============================================================================
#
# Tries each parser in order of preference. Because AutoTrader A/B-routes
# between the Next.js and Angular apps (see module docstring), we don't know
# in advance which data source a detail page will expose, and a single
# scraping run may hit a mix. The dispatcher tries them all and returns on
# the first success.
#
# Tier ordering rationale:
#   1a. __NEXT_DATA__   : richer schema (more specs, WLTP, equipment lists)
#   1b. ngVdpModel      : legacy but complete; covers Angular sessions
#   2.  BeautifulSoup on embedded <script id="__NEXT_DATA__">
#                       : salvages Next.js pages when JS didn't hydrate
#   3.  BeautifulSoup on raw HTML
#                       : last-ditch for when both JS globals are missing;
#                         extracts title / mileage / price / seller only.
# ============================================================================


def extract_listing_details(driver: webdriver.Chrome, url: str) -> VehicleListing:
    """Visit a vehicle page and extract details into a VehicleListing.

    Reads ``window.__NEXT_DATA__.props.pageProps.listingDetails`` via
    JavaScript.  If that fails, falls back to parsing the
    ``<script id="__NEXT_DATA__">`` tag from the raw HTML.
    """
    listing = VehicleListing(url=url, source="autotrader")
    try:
        driver.get(url)
    except Exception:
        return listing

    # Tier 1: JavaScript extraction
    try:
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script(
                "return window.__NEXT_DATA__"
                " && window.__NEXT_DATA__.props"
                " && window.__NEXT_DATA__.props.pageProps"
                " && window.__NEXT_DATA__.props.pageProps.listingDetails"
            )
        )
        data = driver.execute_script(
            "return window.__NEXT_DATA__.props.pageProps.listingDetails"
        )
        listing = parse_next_data(data)
        listing.url = url
        log("  -> extracted via __NEXT_DATA__")
        return listing
    except Exception:
        pass

    # Tier 2: BeautifulSoup fallback (parse embedded JSON)
    log("  -> JS unavailable, falling back to BeautifulSoup")
    try:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        script_tag = soup.find("script", id="__NEXT_DATA__")
        if script_tag and script_tag.string:
            next_data = json.loads(script_tag.string)
            details = (next_data.get("props", {})
                       .get("pageProps", {})
                       .get("listingDetails", {}))
            if details:
                listing = parse_next_data(details)
                listing.url = url
                log("  -> extracted via BeautifulSoup __NEXT_DATA__")
                return listing
    except Exception as exc:
        log(f"  -> BeautifulSoup fallback failed: {exc}")

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
        # Use (source, ad_id) for robust deduplication across sources
        key = (listing.source, listing.ad_id)
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
    "subaru/outback",
    "subaru/crosstrek",
    "toyota/rav4",
    "honda/hr-v",
    "honda/cr-v",
    "hyundai/kona",
]

# Per-vehicle Facebook Marketplace search config. Keyed by AutoTrader slug.
#   query:            free-text search term (FB filter-by-make is unreliable)
#   regex:            case-insensitive regex applied to the listing title
#   model_canonical:  model string to write into the ``model`` column
#   year_range:       (min, max) year kept
FB_QUERIES: Dict[str, Dict[str, Any]] = {
    "subaru/forester": {"query": "subaru forester",  "regex": r"forester",   "model_canonical": "Forester", "year_range": (2016, 2099)},
    "subaru/outback":   {"query": "subaru outback",   "regex": r"outback",    "model_canonical": "OUTBACK",    "year_range": (2016, 2099)},
    "subaru/crosstrek": {"query": "subaru crosstrek", "regex": r"crosstrek",  "model_canonical": "Crosstrek", "year_range": (2016, 2099)},
    "toyota/rav4":      {"query": "toyota rav4",      "regex": r"rav[\s-]?4", "model_canonical": "RAV 4",     "year_range": (2016, 2099)},
    "honda/hr-v":      {"query": "honda hr-v",       "regex": r"hr[\s-]?v",  "model_canonical": "HR-V",     "year_range": (2016, 2099)},
    "honda/cr-v":      {"query": "honda cr-v",       "regex": r"cr[\s-]?v",  "model_canonical": "CR-V",     "year_range": (2016, 2099)},
    "hyundai/kona":    {"query": "hyundai kona",     "regex": r"kona",       "model_canonical": "KONA",     "year_range": (2016, 2099)},
}

COMMON_PARAMS = (
    "?rcp=100"          # results per page (max 100)
    "&yRng=2016%2C"     # year 2016+
    "&priceto=15000"    # max price (AutoScout24 param)
    "&prx=300"          # radius in km
    "&loc=H1X%203J1"    # postal code
    "&sts=Used"         # used vehicles only
)



def scrape_vehicle(driver: webdriver.Chrome, search_url: str,
                   make_model: str, output_file: str,
                   scrape_num: int, scrape_time: str,
                   first_page_loaded: bool = False) -> None:
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

    if existing_df is not None and "source" not in existing_df.columns:
        existing_df["source"] = "autotrader"

    existing_urls: Set[str] = set()
    if existing_df is not None and "url" in existing_df.columns:
        existing_urls = set(existing_df["url"].dropna())

    log("Collecting listing URLs...")
    urls = get_listing_urls(driver, search_url, first_page_loaded=first_page_loaded)
    # Skip Ontario listings
    urls = [u for u in urls if "/ontario/" not in u.lower()]
    log(f"Found {len(urls)} vehicle URLs")

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
        vehicle_mask = (
            existing_df["url"].str.contains(f"/a/{make_model}/", na=False)
            | existing_df["url"].str.contains(f"/offers/{make_model.replace('/', '-')}", na=False)
        ) & (existing_df["source"] == "autotrader")

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
        # Keep only QC listings
        if "province" in combined.columns:
            combined = combined[combined["province"] == "QC"]

        cols = combined.columns.tolist()
        if "scrape_timestamp" in cols:
            cols.insert(0, cols.pop(cols.index("scrape_timestamp")))
        if "scrape_number" in cols:
            cols.insert(0, cols.pop(cols.index("scrape_number")))
        combined = combined[cols]

    combined.to_csv(output_file, index=False)
    log(f"Data saved to {output_file}")


SCATTER_HTML = "suv_scatter.html"
GITHUB_REPO = "winzee/autotrader-camille"
PAGES_URL = f"https://winzee.github.io/autotrader-camille/{SCATTER_HTML}"


def collapse_cross_source_duplicates(csv_file: str) -> int:
    """Drop rows that are the same physical car re-posted (within or across sources).

    Key: ``(make.lower, model.lower, year, mileage_km, price_cad)``. Rows with
    any key field null are NOT eligible (kept as-is).

    Among duplicates of a single key, keeps:
      1. ``source == 'autotrader'`` over any other (AutoTrader has richer fields
         and includes price-vs-market, carfax, etc.)
      2. Within same source, the earliest ``scrape_timestamp`` (first-seen wins
         — so stable ad_id persists and incremental rescrapes don't reshuffle).

    Only active (``is_deleted`` NaN) rows are considered; deleted rows pass
    through untouched. Returns number of rows dropped.
    """
    df = pd.read_csv(csv_file)
    key_cols = ["make", "model", "year", "mileage_km", "price_cad"]
    eligible = df["is_deleted"].isna() & df[key_cols].notna().all(axis=1)
    if not eligible.any():
        return 0
    cand = df[eligible].copy()
    cand["_k_make"] = cand["make"].astype(str).str.lower().str.strip()
    cand["_k_model"] = cand["model"].astype(str).str.lower().str.strip()
    cand["_k_year"] = cand["year"].astype(int)
    cand["_k_mi"] = cand["mileage_km"].astype(int)
    cand["_k_pr"] = cand["price_cad"].astype(int)
    cand["_src_rank"] = (cand["source"].astype(str).str.lower() != "autotrader").astype(int)
    cand = cand.sort_values(by=["_src_rank", "scrape_timestamp"], kind="stable")
    keep = cand.drop_duplicates(
        subset=["_k_make", "_k_model", "_k_year", "_k_mi", "_k_pr"], keep="first"
    )
    drop_idx = cand.index.difference(keep.index)
    removed = len(drop_idx)
    if removed == 0:
        return 0

    def _row_url(row: pd.Series) -> str:
        src = str(row.get("source") or "").lower()
        if src == "facebook" and pd.notna(row.get("ad_id")):
            return f"https://www.facebook.com/marketplace/item/{int(row['ad_id'])}/"
        return str(row.get("url") or "")

    try:
        from fb_scraper import FB_CARD_TRACE_FILE
        trace_path: Optional[str] = FB_CARD_TRACE_FILE
        with open(trace_path, "a") as f:
            f.write(f"\n--- collapse_cross_source_duplicates ---\n")
    except Exception:
        trace_path = None

    key_cols_k = ["_k_make", "_k_model", "_k_year", "_k_mi", "_k_pr"]
    keep_by_key = keep.set_index(key_cols_k)
    for idx in drop_idx:
        drop_row = cand.loc[idx]
        key = tuple(drop_row[c] for c in key_cols_k)
        try:
            kept_row = keep_by_key.loc[key]
            if isinstance(kept_row, pd.DataFrame):
                kept_row = kept_row.iloc[0]
        except KeyError:
            kept_row = None
        make, model = drop_row["make"], drop_row["model"]
        year, km, price = int(drop_row["year"]), int(drop_row["mileage_km"]), int(drop_row["price_cad"])
        dropped_src = str(drop_row.get("source") or "?")
        dropped_url = _row_url(drop_row)
        if kept_row is not None:
            kept_src = str(kept_row.get("source") or "?")
            kept_url = _row_url(kept_row)
            msg = (f"  Collapse: {make} {model} {year} {km}km ${price} — "
                   f"dropping {dropped_src} {dropped_url} (kept {kept_src} {kept_url})")
        else:
            msg = (f"  Collapse: {make} {model} {year} {km}km ${price} — "
                   f"dropping {dropped_src} {dropped_url}")
        log(msg)
        if trace_path:
            with open(trace_path, "a") as f:
                f.write(msg.lstrip() + "\n")

    df = df.drop(drop_idx).reset_index(drop=True)
    df.to_csv(csv_file, index=False)
    return removed


def generate_scatter_html(csv_file: str, output_file: str,
                          max_price: int = 15000,
                          max_km: int = 200000) -> None:
    """Read the CSV and generate an interactive scatter plot HTML file."""
    df = pd.read_csv(csv_file)
    # Filter: active listings, within thresholds, known makes
    df = df[df["is_deleted"].isna()]
    df = df[(df["price_cad"] <= max_price) & (df["mileage_km"] <= max_km)]
    df = df[df["make"].isin(["Subaru", "Toyota", "Honda", "Hyundai"])]
    if "province" in df.columns:
        df = df[df["province"] != "ON"]

    # Freshness tiers: "latest" (last scrape), "recent" (today/yesterday), "old"
    scrape_nums = df["scrape_number"].dropna().unique()
    max_scrape = df["scrape_number"].max() if len(scrape_nums) else 0
    has_multiple = len(scrape_nums) > 1
    today = datetime.now().date()
    yesterday = today - __import__("datetime").timedelta(days=1)

    MODEL_DISPLAY = {
        "CR-V": "CR-V",
        "HR-V": "HR-V",
        "KONA": "Kona",
        "Crosstrek": "Crosstrek",
        "Forester": "Forester",
        "OUTBACK": "Outback",
        "RAV 4": "RAV4",
    }

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

        model_raw = row["model"] if pd.notna(row.get("model")) else ""
        model_display = MODEL_DISPLAY.get(model_raw)
        if not model_display:
            continue

        records.append({
            "source": str(row["source"]).lower() if pd.notna(row.get("source")) else "autotrader",
            "make": row["make"],
            "model": model_display,
            "year": int(row["year"]) if pd.notna(row["year"]) else 0,
            "mileage_km": int(row["mileage_km"]) if pd.notna(row["mileage_km"]) else 0,
            "price_cad": int(row["price_cad"]) if pd.notna(row["price_cad"]) else 0,
            "url": row["url"] or "",
            "title": row["title"] if pd.notna(row.get("title")) else "",
            "city": row["city"] if pd.notna(row.get("city")) else "",
            "has_cruise": bool(row["has_cruise"]) if pd.notna(row.get("has_cruise")) else False,
            "has_carplay": bool(row["has_carplay"]) if pd.notna(row.get("has_carplay")) else False,
            "seller_name": row["seller_name"] if pd.notna(row.get("seller_name")) else "",
            "price_analysis": row["price_analysis_description"] if pd.notna(row.get("price_analysis_description")) else "",
            "freshness": freshness,
            "body_color": row["body_color"] if pd.notna(row.get("body_color")) else "",
            "image_url": row["image_url"] if pd.notna(row.get("image_url")) else "",
            "carfax_url": row["carfax_url"] if pd.notna(row.get("carfax_url")) else "",
            "comfort_equipment": row["comfort_equipment"] if pd.notna(row.get("comfort_equipment")) else "",
            "safety_equipment": row["safety_equipment"] if pd.notna(row.get("safety_equipment")) else "",
            "consumption_city": float(row["consumption_city"]) if pd.notna(row.get("consumption_city")) else None,
            "consumption_highway": float(row["consumption_highway"]) if pd.notna(row.get("consumption_highway")) else None,
            "transmission": row["transmission"] if pd.notna(row.get("transmission")) else "",
            "upholstery_color": row["upholstery_color"] if pd.notna(row.get("upholstery_color")) else "",
            "model_version": row["model_version"] if pd.notna(row.get("model_version")) else "",
        })

    json_data = json.dumps(records, ensure_ascii=False)
    y_min = int(df.loc[df["price_cad"] >= 4000, "price_cad"].min() - 200) if not df.empty else 0

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
  #tooltip { position: fixed; background: #0f3460; border: 1px solid #555; border-radius: 8px; padding: 10px 14px; font-size: 0.82rem; pointer-events: auto; display: none; z-index: 10; line-height: 1.5; }
  #tooltip .tt-title { font-weight: bold; }
  #tooltip .tt-detail { color: #ccc; }
  #tooltip .tt-color { color: #bbb; font-size: 0.78rem; }
  #tooltip .tt-city { color: #999; }
  #tooltip .tt-source { color: #999; font-size: 0.78rem; }
  #tooltip .tt-extra { color: #aaa; font-size: 0.78rem; margin-top: 4px; white-space: pre-line; }
  #tooltip .tt-buttons { display: flex; gap: 6px; margin-top: 6px; }
  #tooltip .tt-open { display: inline-block; background: #e74c3c; color: #fff; padding: 4px 10px; border-radius: 4px; font-size: 0.78rem; text-decoration: none; }
  #tooltip .tt-info-btn { display: inline-block; background: #2980b9; color: #fff; padding: 4px 10px; border-radius: 4px; font-size: 0.78rem; border: none; cursor: pointer; }
  .info-overlay { position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.6); z-index: 100; display: none; justify-content: center; align-items: center; }
  .info-popup { background: #16213e; border: 1px solid #555; border-radius: 12px; padding: 20px; max-width: 420px; width: 90%; max-height: 80vh; overflow-y: auto; position: relative; color: #eee; font-size: 0.85rem; line-height: 1.6; }
  .info-popup .info-close { position: absolute; top: 8px; right: 12px; background: none; border: none; color: #aaa; font-size: 1.2rem; cursor: pointer; }
  .info-popup .info-title { font-weight: bold; font-size: 1rem; margin-bottom: 10px; }
  .info-popup .info-body div { margin-bottom: 4px; }
  .info-popup .info-body a { color: #3498db; }
  .info-popup .info-img { width: 100%; border-radius: 8px; margin-top: 10px; }
</style>
</head>
<body>

<div class="controls">
  <label><input type="checkbox" checked data-model="Forester"> <svg width="14" height="14"><circle cx="7" cy="7" r="5" fill="#7f8c8d" stroke="#fff" stroke-width="1"/></svg> Forester</label>
  <label><input type="checkbox" checked data-model="Outback"> <svg width="14" height="14"><polygon points="2,7 7,2 12,7 7,12" fill="#7f8c8d" stroke="#fff" stroke-width="1"/></svg> Outback</label>
  <label><input type="checkbox" checked data-model="RAV4"> <svg width="14" height="14"><polygon points="7,2 13,12 1,12" fill="#7f8c8d" stroke="#fff" stroke-width="1"/></svg> RAV4</label>
  <label><input type="checkbox" checked data-model="HR-V"> <svg width="14" height="14"><rect x="2" y="2" width="10" height="10" fill="#7f8c8d" stroke="#fff" stroke-width="1"/></svg> HR-V</label>
  <label><input type="checkbox" checked data-model="CR-V"> <svg width="14" height="14"><rect x="2" y="2" width="10" height="10" rx="3" fill="#7f8c8d" stroke="#fff" stroke-width="1"/></svg> CR-V</label>
  <label><input type="checkbox" checked data-model="Kona"> <svg width="14" height="14"><polygon points="7,1 9,5 13,5.5 10,8.5 11,13 7,11 3,13 4,8.5 1,5.5 5,5" fill="#7f8c8d" stroke="#fff" stroke-width="1"/></svg> Kona</label>
  <label><input type="checkbox" checked data-model="Crosstrek"> <svg width="14" height="14"><line x1="7" y1="2" x2="7" y2="12" stroke="#7f8c8d" stroke-width="2"/><line x1="2" y1="7" x2="12" y2="7" stroke="#7f8c8d" stroke-width="2"/></svg> Crosstrek</label>
  <span style="opacity:0.35">|</span>
  <label><input type="checkbox" checked data-source="autotrader"> AutoTrader</label>
  <label><input type="checkbox" checked data-source="facebook"> Facebook</label>
</div>

<div class="chart-wrap">
  <canvas id="chart"></canvas>
</div>

<div id="tooltip">
  <div class="tt-title"></div>
  <div class="tt-detail"></div>
  <div class="tt-color"></div>
  <div class="tt-city"></div>
  <div class="tt-source"></div>
  <div class="tt-extra"></div>
  <div class="tt-buttons">
    <a class="tt-open" href="#" target="_blank">Open listing</a>
    <button class="tt-info-btn">Info</button>
  </div>
</div>

<div class="info-overlay">
  <div class="info-popup">
    <button class="info-close">&times;</button>
    <div class="info-title"></div>
    <div class="info-body"></div>
  </div>
</div>

<script>
const SHAPES = { 'Forester': 'circle', 'Outback': 'rectRot', 'RAV4': 'triangle', 'HR-V': 'rect', 'CR-V': 'rectRounded', 'Kona': 'star', 'Crosstrek': 'cross' };
const COLORS = { latest: '#ffd700', recent: '#2ecc71', old: '#7f8c8d' };
const SIZES  = { latest: { r: 7, hr: 9, bw: 0 }, recent: { r: 7, hr: 9, bw: 0 }, old: { r: 7, hr: 9, bw: 0 } };
const SOURCE_LABELS = { autotrader: 'AutoTrader', facebook: 'Facebook' };
const normSource = v => (v || 'autotrader').toString().toLowerCase();

const data = ''' + json_data + ''';

const datasets = {};
Object.keys(SHAPES).forEach(model => {
  datasets[model] = { label: model, data: [], meta: [], backgroundColor: [], borderColor: [], pointRadius: [], pointHoverRadius: [], pointBorderColor: [], pointBorderWidth: [], pointStyle: SHAPES[model], showLine: false };
});

function rebuild() {
  const active = new Set([...document.querySelectorAll('.controls input[data-source]')].filter(c => c.checked).map(c => c.dataset.source));
  Object.values(datasets).forEach(ds => {
    ds.data = []; ds.meta = []; ds.backgroundColor = []; ds.borderColor = [];
    ds.pointRadius = []; ds.pointHoverRadius = []; ds.pointBorderColor = []; ds.pointBorderWidth = [];
  });
  data.forEach(d => {
    if (!active.has(normSource(d.source))) return;
    const ds = datasets[d.model];
    if (!ds) return;
    ds.data.push({ x: d.mileage_km, y: d.price_cad });
    ds.meta.push(d);
    const f = d.freshness || 'old';
    const c = COLORS[f], s = SIZES[f];
    ds.backgroundColor.push(c);
    ds.borderColor.push(c);
    ds.pointRadius.push(s.r);
    ds.pointHoverRadius.push(s.hr);
    ds.pointBorderColor.push(c);
    ds.pointBorderWidth.push(['star', 'cross'].includes(SHAPES[d.model]) ? 2 : 0);
  });
}
rebuild();

Chart.register(ChartDataLabels);

let selectedKey = null;
let currentItem = null;

function showTooltip(item, cx, cy) {
  currentItem = item;
  const tt = document.getElementById('tooltip');
  tt.querySelector('.tt-title').textContent = item.title;
  tt.querySelector('.tt-detail').textContent = item.year + ' \\u00b7 ' + item.mileage_km.toLocaleString() + ' km \\u00b7 $' + item.price_cad.toLocaleString();
  tt.querySelector('.tt-color').textContent = item.body_color ? 'Color: ' + item.body_color : '';
  tt.querySelector('.tt-city').textContent = item.city;
  const srcKey = normSource(item.source);
  tt.querySelector('.tt-source').textContent = 'Source: ' + (SOURCE_LABELS[srcKey] || item.source || 'AutoTrader');
  const cruise = item.has_cruise ? 'Yes' : 'No';
  const carplay = item.has_carplay ? 'Yes' : 'No';
  const extra = 'Cruise: ' + cruise + ' | CarPlay: ' + carplay + '\\n' + item.seller_name + (item.price_analysis ? '\\n' + item.price_analysis : '');
  tt.querySelector('.tt-extra').textContent = extra;
  tt.querySelector('.tt-open').href = item.url;
  tt.style.display = 'block';
  const rect = tt.getBoundingClientRect();
  const left = Math.min(cx + 14, window.innerWidth - rect.width - 10);
  const top = Math.min(cy - 10, window.innerHeight - rect.height - 10);
  tt.style.left = left + 'px';
  tt.style.top = Math.max(10, top) + 'px';
}

function showInfoPopup(item) {
  const overlay = document.querySelector('.info-overlay');
  const popup = overlay.querySelector('.info-popup');
  popup.querySelector('.info-title').textContent = item.title;
  const body = popup.querySelector('.info-body');
  body.innerHTML = '';

  function addLine(label, value) {
    if (!value) return;
    const div = document.createElement('div');
    const b = document.createElement('strong');
    b.textContent = label + ': ';
    div.appendChild(b);
    const span = document.createElement('span');
    span.textContent = value;
    div.appendChild(span);
    body.appendChild(div);
  }

  if (item.carfax_url) {
    const div = document.createElement('div');
    const a = document.createElement('a');
    a.href = item.carfax_url;
    a.target = '_blank';
    a.textContent = 'CarFax Report';
    div.appendChild(a);
    body.appendChild(div);
  }
  addLine('Source', SOURCE_LABELS[normSource(item.source)] || item.source);
  addLine('Model version', item.model_version);
  addLine('Transmission', item.transmission);
  addLine('Body color', item.body_color);
  addLine('Upholstery', item.upholstery_color);
  if (item.consumption_city != null || item.consumption_highway != null) {
    const parts = [];
    if (item.consumption_city != null) parts.push('City: ' + item.consumption_city + ' L/100km');
    if (item.consumption_highway != null) parts.push('Hwy: ' + item.consumption_highway + ' L/100km');
    addLine('Consumption', parts.join(' / '));
  }
  addLine('Comfort equipment', item.comfort_equipment);
  addLine('Safety equipment', item.safety_equipment);

  if (item.image_url) {
    const img = document.createElement('img');
    img.src = item.image_url;
    img.className = 'info-img';
    img.alt = item.title;
    body.appendChild(img);
  }

  overlay.style.display = 'flex';
}

document.querySelector('.tt-info-btn').addEventListener('click', function(e) {
  e.stopPropagation();
  if (currentItem) showInfoPopup(currentItem);
});

document.querySelector('.info-overlay').addEventListener('click', function(e) {
  if (e.target === this) this.style.display = 'none';
});
document.querySelector('.info-close').addEventListener('click', function() {
  document.querySelector('.info-overlay').style.display = 'none';
});

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
          return item.model + '\\n' + item.year;
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

document.querySelectorAll('.controls input[data-model]').forEach(cb => {
  cb.addEventListener('change', () => {
    const idx = chart.data.datasets.findIndex(ds => ds.label === cb.dataset.model);
    if (idx >= 0) { chart.setDatasetVisibility(idx, cb.checked); chart.update(); }
  });
});
document.querySelectorAll('.controls input[data-source]').forEach(cb => {
  cb.addEventListener('change', () => { rebuild(); chart.update(); });
});
</script>

</body>
</html>'''

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)
    log(f"Scatter plot written to {output_file}")


def _parse_args():
    import argparse
    p = argparse.ArgumentParser(description="Scrape used SUV listings and update the plot.")
    p.add_argument("--source", choices=["autotrader", "facebook", "all"], default="all",
                   help="Which source(s) to scrape. Default: all")
    p.add_argument("--limit", type=int, default=None,
                   help="Max listings per vehicle (applied to FB; useful for testing).")
    p.add_argument("--days", type=int, default=None,
                   help="Override daysSinceListed (FB only). Default: computed from last-scrape state, max 365.")
    p.add_argument("--make-model", dest="make_model", default=None,
                   help="Scrape only one vehicle slug (e.g. 'subaru/forester').")
    p.add_argument("--generate-html-only", action="store_true",
                   help="Skip scraping — just regenerate the HTML plot from the existing CSV.")
    p.add_argument("--no-publish", action="store_true",
                   help="Do not commit/push the HTML to GitHub Pages.")
    return p.parse_args()


def _scrape_autotrader(vehicles: List[str], scrape_num: int, scrape_time: str) -> None:
    first = vehicles[0]
    first_search_url = (
        f"https://www.autotrader.ca/cars/{first}/qc/montr%c3%a9al/" + COMMON_PARAMS
    )
    driver = create_driver(headless=False)
    log("Warming up AutoTrader session...")
    driver.get("https://www.autotrader.ca/")
    time.sleep(5)
    try:
        consent_btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(), 'Accept')]")
            )
        )
        consent_btn.click()
        log("Cookies accepted during warm-up")
        time.sleep(1)
    except Exception:
        pass
    driver.get(first_search_url)
    time.sleep(8)

    try:
        print(f"\n{'='*60}\nScraping AutoTrader: {first}\n{'='*60}")
        scrape_vehicle(driver, first_search_url, first, OUTPUT_FILE,
                       scrape_num, scrape_time, first_page_loaded=True)
        for make_model in vehicles[1:]:
            search_url = (
                f"https://www.autotrader.ca/cars/{make_model}/qc/montr%c3%a9al/" + COMMON_PARAMS
            )
            print(f"\n{'='*60}\nScraping AutoTrader: {make_model}\n{'='*60}")
            scrape_vehicle(driver, search_url, make_model, OUTPUT_FILE,
                           scrape_num, scrape_time)
    finally:
        driver.quit()


def _scrape_facebook(vehicles: List[str], scrape_num: int, scrape_time: str,
                     max_listings: int, days_override: Optional[int] = None) -> None:
    from fb_scraper import (
        create_fb_driver, scrape_vehicle_facebook,
        load_fb_scrape_state, save_fb_scrape_state, reset_card_trace,
    )
    reset_card_trace()
    # Snapshot the last-scrape timestamp ONCE — so every vehicle in this run
    # uses the same daysSinceListed window (computed from the prior run's end).
    session_last_ts = load_fb_scrape_state().get("last_fb_scrape_timestamp")
    driver = create_fb_driver(headless=False)
    try:
        for make_model in vehicles:
            cfg = FB_QUERIES.get(make_model)
            if not cfg:
                log(f"No FB config for {make_model} — skipping")
                continue
            print(f"\n{'='*60}\nScraping Facebook: {make_model} (query={cfg['query']!r})\n{'='*60}")
            scrape_vehicle_facebook(
                driver, make_model=make_model, query=cfg["query"],
                model_regex_src=cfg["regex"], model_canonical=cfg["model_canonical"],
                output_file=OUTPUT_FILE, scrape_num=scrape_num, scrape_time=scrape_time,
                max_listings=max_listings, year_range=cfg.get("year_range"),
                session_last_scrape_timestamp=session_last_ts,
                days_override=days_override,
            )
    finally:
        try:
            driver.quit()
        except Exception:
            pass
    # Persist the run's end time ONCE so the next run's window starts here.
    save_fb_scrape_state({"last_fb_scrape_timestamp": scrape_time})


def main() -> None:
    """Scrape all configured vehicles, one at a time.

    The search URL is assembled with filters for make (BMW), model (X3), year
    range (2021–2023), vehicle condition (Used), radius (500 km) and postal
    code (H1X 3J1).  The ``rcp`` parameter is set to 100 to request the
    maximum number of results per page, as suggested by the AutoTrader
    scraping documentation【298376264705576†L224-L233】.  Additional pages
    (``rcs`` offsets) could be processed in a loop if required.
    """
    args = _parse_args()

    if not args.generate_html_only:
        scrape_time = datetime.now().isoformat()
        scrape_num = 1
        if os.path.exists(OUTPUT_FILE):
            try:
                existing_df = pd.read_csv(OUTPUT_FILE)
                if "scrape_number" in existing_df.columns:
                    scrape_num = int(existing_df["scrape_number"].max()) + 1
            except (pd.errors.EmptyDataError, KeyError, ValueError):
                pass

        if args.make_model:
            if args.make_model not in VEHICLES:
                log(f"Unknown make-model: {args.make_model}. Valid: {VEHICLES}")
                return
            vehicles = [args.make_model]
        else:
            vehicles = list(VEHICLES)

        if args.source in ("autotrader", "all"):
            _scrape_autotrader(vehicles, scrape_num, scrape_time)
        if args.source in ("facebook", "all"):
            max_listings = args.limit if args.limit is not None else 200
            _scrape_facebook(vehicles, scrape_num, scrape_time,
                             max_listings=max_listings, days_override=args.days)

        removed = collapse_cross_source_duplicates(OUTPUT_FILE)
        if removed:
            log(f"Collapsed {removed} cross-source/same-car duplicate(s)")

    generate_scatter_html(OUTPUT_FILE, SCATTER_HTML)
    log("Scatter plot updated")

    if args.no_publish:
        return
    try:
        subprocess.run(["git", "add", SCATTER_HTML], check=True, capture_output=True)
        subprocess.run(
            ["git", "commit", "-m", f"Update {SCATTER_HTML}"],
            check=True, capture_output=True, text=True,
        )
        subprocess.run(["git", "push"], check=True, capture_output=True, text=True)
        log(f"Pushed to GitHub Pages: {PAGES_URL}")
    except Exception as e:
        log(f"GitHub push failed (non-fatal): {e}")


if __name__ == "__main__":
    main()
