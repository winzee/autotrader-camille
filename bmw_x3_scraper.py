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
for Testing in a visible window (``_scrape_autotrader`` calls
``create_driver(headless=False)``, so CAPTCHAs can be solved by hand), scrolls
search pages to trigger lazy loads, collects listing URLs, and visits each
detail page to read an in-page JavaScript data object.

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
     CAPTCHA resolution when the page returns very few anchors, and reports
     the event so ``scrape_vehicle`` refuses to infer deletions from a search
     that may have been served a block page.
"""

# ── Scraper settings ─────────────────────────────────────────────────────
# Anti-runaway safety valves for AutoTrader pagination (internal, not in YAML).
# The fallback path used to break only when a page yielded neither new links NOR
# any links at all — so a search that kept re-serving the same page (page param
# ignored past some rank, or the postal filter emptying every page) looped
# forever. Only reachable when numberOfPages metadata is missing.
AT_MAX_PAGES_WITHOUT_NEW = 3   # consecutive pages adding no unseen URL
AT_MAX_PAGES_ABSOLUTE = 200    # hard ceiling, runaway guard only
AT_MAX_SCROLLS_PER_PAGE = 30   # anti-wedge ceiling on scroll_to_load_all
LISTING_PAUSE_SECS = 20  # Base pause between each listing scrape (jittered ±5s)
VEHICLE_PAUSE_SECS = 45  # Pause between vehicle search transitions (captcha trigger point)

import glob
import json
import random
import re
import sys
import time
from datetime import datetime as _dt

def log(msg: str) -> None:
    print(f"[{_dt.now().strftime('%H:%M:%S')}] {msg}", flush=True)
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import os
import subprocess
import urllib.parse
from typing import List, Dict, Any, Optional, Set, Tuple, IO

from config import Config, SearchUnit, AutotraderSearch, load_config


# ── Run-log tee ──────────────────────────────────────────────────────────
# Mirror stdout/stderr to a per-run file under ``logs/`` so prior runs can be
# inspected after the fact (useful for diagnosing low-yield scrapes, CAPTCHA
# events, or detail-page parser failures). Activated from ``main()``.
LOG_DIR = "logs"


class _Tee:
    """Write-through proxy that fans every write to multiple streams."""

    def __init__(self, *streams: IO[str]) -> None:
        self._streams = streams

    def write(self, data: str) -> int:
        for s in self._streams:
            try:
                s.write(data)
            except Exception:
                pass
        return len(data)

    def flush(self) -> None:
        for s in self._streams:
            try:
                s.flush()
            except Exception:
                pass

    def isatty(self) -> bool:
        return any(getattr(s, "isatty", lambda: False)() for s in self._streams)


def setup_run_log() -> Optional[str]:
    """Tee stdout and stderr to a timestamped file under ``logs/``.

    Returns the absolute path of the log file, or ``None`` if setup fails
    (logging to a file is best-effort and must never break the scrape).
    """
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        path = os.path.join(LOG_DIR, _dt.now().strftime("run_%Y-%m-%d_%H%M%S.log"))
        # Line-buffered so partial output is visible if the run is interrupted.
        f = open(path, "a", buffering=1)
        f.write(f"=== run started {_dt.now().isoformat()} ===\n")
        sys.stdout = _Tee(sys.__stdout__, f)  # type: ignore[assignment]
        sys.stderr = _Tee(sys.__stderr__, f)  # type: ignore[assignment]
        return os.path.abspath(path)
    except Exception as exc:
        print(f"WARNING: could not set up run log: {exc}", flush=True)
        return None

from bs4 import BeautifulSoup, Tag  # type: ignore
import pandas as pd  # type: ignore

from selenium import webdriver  # type: ignore
from selenium.webdriver.chrome.options import Options  # type: ignore
from selenium.webdriver.common.by import By  # type: ignore
from selenium.webdriver.support.ui import WebDriverWait  # type: ignore
from selenium.webdriver.support import expected_conditions as EC  # type: ignore
from webdriver_manager.chrome import ChromeDriverManager  # type: ignore
from selenium.webdriver.chrome.service import Service  # type: ignore
from selenium.common.exceptions import TimeoutException, WebDriverException  # type: ignore


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
    """Instantiate a Chrome WebDriver using webdriver‑manager.

    The ``webdriver_manager`` library automatically downloads an appropriate
    version of ChromeDriver and ensures that Selenium can locate it.  With
    ``headless=True`` the ``--headless=new`` flag suppresses the GUI; the
    AutoTrader path calls this with ``headless=False`` so the window is
    visible and a CAPTCHA can be solved by hand mid-run.

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
    # Prevent page loads from hanging indefinitely. 90s, not 30: a cold first
    # load on a memory-starved machine was measured timing out at ~29.4s.
    driver.set_page_load_timeout(90)
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

    ``AT_MAX_SCROLLS_PER_PAGE`` bounds the loop. A settling page height is a
    sound end signal on AT (unlike Facebook, where it ends runs early — see
    CLAUDE.md); the cap only exists so a page that keeps growing forever cannot
    wedge the run.
    """
    last_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(AT_MAX_SCROLLS_PER_PAGE):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            return
        last_height = new_height
    log(f"  Page still growing after {AT_MAX_SCROLLS_PER_PAGE} scrolls — "
        f"collecting what has loaded")


def _collect_page_links(driver: webdriver.Chrome) -> Tuple[Set[str], bool]:
    """Collect listing links from the current search results page.

    Returns ``(links, captcha_hit)``. ``captcha_hit`` is True when the page
    tripped the CAPTCHA branch below — solved or not. A challenged page cannot
    testify about which listings still exist, so callers must not read its
    (empty or partial) result as listings having disappeared.
    """
    # Attempt to accept cookie consent if present.
    try:
        consent_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Accept')]")
        consent_button.click()
        time.sleep(1)
    except Exception:
        pass
    # AutoTrader listing URLs come in two flavours depending on which app the
    # session was A/B-routed to: Next.js → /offers/, Angular → /a/. We collect
    # both; scrape_vehicle() post-filters to URLs matching the expected
    # make/model so we don't pick up navigation or related-vehicle anchors.
    LISTING_SELECTORS = ["a[href*='/offers/']", "a[href*='/a/']"]

    # Wait for listing links to appear (up to 15s), retry on failure
    captcha_hit = False
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
                    captcha_hit = True
                    log("CAPTCHA detected — please solve it in the browser. Waiting up to 2 min...")
                    for _ in range(24):
                        time.sleep(5)
                        if any(driver.find_elements(By.CSS_SELECTOR, sel)
                               for sel in LISTING_SELECTORS):
                            log("CAPTCHA solved, continuing...")
                            break
                    else:
                        log("Timed out waiting for CAPTCHA resolution")
                        return set(), captcha_hit
                    break  # CAPTCHA solved — proceed to scroll + collect
                print(f"  WARNING: no listing links found (page title: {driver.title})")
                return set(), captcha_hit
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
    return links, captcha_hit


def _read_search_page_count(driver: webdriver.Chrome) -> Optional[int]:
    """Read total pages from ``window.__NEXT_DATA__.props.pageProps.numberOfPages``.

    Returns None when the metadata is missing — typically because the session
    landed on the Angular app (which has no __NEXT_DATA__). Callers fall back
    to "stop when a page returns no new URLs" in that case.
    """
    try:
        n = driver.execute_script(
            "try { return window.__NEXT_DATA__.props.pageProps.numberOfPages; }"
            " catch (e) { return null; }"
        )
        if isinstance(n, int) and n > 0:
            return n
    except Exception:
        pass
    return None


def _read_page_listings_locations(driver: webdriver.Chrome) -> Dict[str, str]:
    """Map each listing URL on the current search page to its dealer postal code.

    Reads ``__NEXT_DATA__.props.pageProps.listings`` (Next.js search page).
    Returns ``{}`` on Angular sessions or when the array is missing.
    """
    try:
        pairs = driver.execute_script(
            "try {"
            "  var L = window.__NEXT_DATA__.props.pageProps.listings;"
            "  if (!Array.isArray(L)) return [];"
            "  return L.map(function (l) {"
            "    return [l && l.url, l && l.location && l.location.zip];"
            "  });"
            "} catch (e) { return []; }"
        )
        return {url: zip_ for url, zip_ in (pairs or []) if url and zip_}
    except Exception:
        return {}


def _postal_passes(zip_code: str, prefixes: List[str]) -> bool:
    """Return True if ``zip_code`` starts with any allowed prefix.

    Both the postal code and the prefixes are uppercased and stripped of
    spaces before matching, so "H1X 3J1" matches prefix "H".
    """
    if not zip_code or not prefixes:
        return True
    norm = zip_code.upper().replace(" ", "")
    return any(norm.startswith(p) for p in prefixes)


def get_listing_urls(driver: webdriver.Chrome, search_url: str,
                     first_page_loaded: bool = False,
                     postal_prefixes: Optional[List[str]] = None
                     ) -> Tuple[List[str], bool]:
    """Return all vehicle listing URLs across every page of the search.

    AT renders ~20 listings per search-results page regardless of ``rcp=``;
    to capture everything matching the filter set we paginate via
    ``&page=N``. The total page count is read from
    ``__NEXT_DATA__.props.pageProps.numberOfPages`` after page 1 loads, so we
    know exactly how many pages to fetch (e.g. 11 pages × 20 = 216 listings
    for Émile's Subaru AWD search, rather than just the first 20).

    When ``postal_prefixes`` is provided, listings whose dealer postal code
    (read from ``pageProps.listings[].location.zip``) doesn't match any
    prefix are dropped at search time — saving one detail-page fetch per
    out-of-area URL. AT honors prx/loc unreliably, so without this filter
    a make-only Émile search returns dealers from across Canada.

    Returns ``(urls, search_healthy)``. ``search_healthy`` is False when a
    CAPTCHA was hit on any page or the search yielded no link at all — either
    way the search could not testify about what still exists, so the caller
    must not infer deletions from it.
    """
    all_links: Set[str] = set()
    skipped_postal = 0
    skipped_unlisted = 0
    seen_in_listings = 0
    captcha_hit = False

    def _filter_with_listings(page_links: Set[str]) -> Set[str]:
        nonlocal skipped_postal, seen_in_listings, skipped_unlisted
        if not postal_prefixes:
            return page_links
        loc = _read_page_listings_locations(driver)
        if not loc:
            return page_links  # Angular session or missing metadata; keep all
        kept: Set[str] = set()
        for url in page_links:
            zip_code = loc.get(url)
            if zip_code is None:
                # Not in pageProps.listings — sponsored/national ad-tier slots
                # routinely surface cross-country dealers (Ottawa, Abbotsford BC).
                # Drop them; legit QC listings are always in the listings array.
                skipped_unlisted += 1
                continue
            seen_in_listings += 1
            if _postal_passes(zip_code, postal_prefixes):
                kept.add(url)
            else:
                skipped_postal += 1
        return kept

    if not first_page_loaded:
        driver.get(search_url)
    page1_links, page1_captcha = _collect_page_links(driver)
    captcha_hit = captcha_hit or page1_captcha
    page1_kept = _filter_with_listings(page1_links)
    all_links.update(page1_kept)
    log(f"  Page 1: {len(page1_kept)} URLs (of {len(page1_links)})")

    total_pages = _read_search_page_count(driver)
    if total_pages:
        log(f"  Search reports {total_pages} total pages")
    else:
        log("  No numberOfPages metadata (Angular session?) — paginating until empty")

    page = 2
    pages_without_new = 0
    while page <= AT_MAX_PAGES_ABSOLUTE:
        # Clean stop: we've reached the reported last page.
        if total_pages and page > total_pages:
            break
        page_url = f"{search_url}&page={page}"
        driver.get(page_url)
        page_links, page_captcha = _collect_page_links(driver)
        captcha_hit = captcha_hit or page_captcha
        page_kept = _filter_with_listings(page_links)
        new_links = page_kept - all_links
        if not new_links:
            # No unseen URL: past the last page, the page errored, or AT is
            # re-serving one we already consumed. The old rule also required
            # page_links to be empty, which never fired in that third case, so
            # the loop could spin forever.
            #
            # FALLBACK for the no-metadata case ONLY. When AT reports a page
            # count we trust it: the postal filter can empty several consecutive
            # pages (out-of-province dealers arrive grouped), and bailing there
            # would silently drop every later page.
            pages_without_new += 1
            if total_pages is None and pages_without_new >= AT_MAX_PAGES_WITHOUT_NEW:
                log(f"  Page {page}: 0 new URLs on "
                    f"{pages_without_new} consecutive pages — stopping")
                break
            page += 1
            continue
        pages_without_new = 0
        all_links.update(new_links)
        log(f"  Page {page}: {len(new_links)} new URLs (total: {len(all_links)})")
        page += 1
    else:
        log(f"  Reached the {AT_MAX_PAGES_ABSOLUTE}-page ceiling — stopping")
    if postal_prefixes and seen_in_listings:
        log(f"  Postal filter: skipped {skipped_postal} of {seen_in_listings} listings (out-of-area)")
    if postal_prefixes and skipped_unlisted:
        log(f"  Postal filter: skipped {skipped_unlisted} unlisted/sponsored URLs (not in pageProps.listings)")
    search_healthy = not captcha_hit and bool(all_links)
    return list(all_links), search_healthy


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
#
# When every tier misses, the caller gets a VehicleListing carrying only the
# URL — there is no raw-HTML scraping tier.
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

    # Tier 1a: __NEXT_DATA__ (Next.js detail page)
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
        listing.source = "autotrader"
        log("  -> extracted via __NEXT_DATA__")
        return listing
    except Exception:
        pass

    # Tier 1b: window.ngVdpModel (legacy Angular detail page)
    try:
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script("return !!window.ngVdpModel")
        )
        data = driver.execute_script("return window.ngVdpModel")
        if data:
            listing = parse_ngvdp_model(data)
            listing.url = url
            listing.source = "autotrader"
            log("  -> extracted via ngVdpModel")
            return listing
    except Exception:
        pass

    # Tier 2: BeautifulSoup fallback (parse embedded JSON)
    log("  -> JS unavailable, falling back to BeautifulSoup")
    try:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        script_tag = soup.find("script", id="__NEXT_DATA__")
        if isinstance(script_tag, Tag) and script_tag.string:
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
# Per-user knobs (vehicle list, FB queries, search filters, output paths,
# GitHub Pages target) live in YAML profiles loaded by ``config.py``. See
# ``camille.yaml`` and ``emile.yaml``.


# Lat/lon for the postal code in the YAML profiles (both use H1X 3J1, the
# Olympic Stadium area in Montréal). Sourced from the redirect AT performs
# when you submit H1X 3J1 in the location box. If a profile ever uses a
# different postal code, add its coords here.
_POSTAL_LATLON = {
    "H1X3J1": (45.54811096191406, -73.55992889404307),
}


def _at_common_params(search: AutotraderSearch) -> str:
    """Build the AT search query string in AT's own canonical form.

    Earlier we used ``prx=<km>&loc=<postal>``; AT redirected those away the
    moment we paginated, dumping us into a country-wide search (the URL on
    page=N became ``/cars/<make>/ot_used?...`` with no location at all). The
    canonical form ships ``lat``/``lon``/``zipr`` instead — these survive
    pagination redirects. Use ``size=100`` (UI cap) to cut page count by 5x
    over the default ``size=20``; ``rcp=100`` was silently ignored.
    """
    norm_zip = search.postal_code.upper().replace(" ", "")
    lat_lon = _POSTAL_LATLON.get(norm_zip)
    parts = [
        f"priceto={search.price_max}",
        f"modelyearfrom={search.year_min}",
        f"zipr={search.radius_km}",
        "cy=CA",
        "ustate=N%2CU",
        "damaged_listing=exclude",
        "atype=C",
        "search_type=C",
        "sort=standard",
        "size=100",
    ]
    if lat_lon is not None:
        parts.append(f"lat={lat_lon[0]}")
        parts.append(f"lon={lat_lon[1]}")
    if search.price_min and search.price_min > 0:
        parts.append(f"pricefrom={search.price_min}")
    for key, value in search.extra_params.items():
        parts.append(f"{key}={urllib.parse.quote(str(value))}")
    return "?" + "&".join(parts)


def build_at_search_url(unit: SearchUnit, search: AutotraderSearch) -> str:
    """Build the AT search URL for a single (make, optional-model) unit.

    Path template is AT's canonical ``/cars/<make>[/<model>]/reg_qc/cit_montreal/ot_used``
    — the form AT redirects to once a postal code is submitted. Earlier we used
    ``/cars/<make>/qc/montréal/`` which AT silently rewrites away the moment
    you paginate, dropping the location entirely.

    For digit-suffixed models AT requires a hyphen between the letter and the
    digit in the URL slug (``rav4`` is rejected; ``rav-4`` works). Letter-only
    hyphenations (``cr-v``, ``hr-v``) work either way; the YAML form is used
    as-is. Listing URL filtering still accepts both spellings via
    ``_make_model_url_patterns``.
    """
    if unit.model:
        model_slug = re.sub(r"([a-z])(\d)", r"\1-\2", unit.model.lower())
        path = f"{unit.make}/{model_slug}"
    else:
        path = unit.make
    return (
        f"https://www.autotrader.ca/cars/{path}/reg_qc/cit_montreal/ot_used"
        + _at_common_params(search)
    )


def _make_model_url_patterns(unit: SearchUnit) -> List[str]:
    """Return the substrings that identify URLs for a given search unit.

    AT's listing URLs come in two flavours (Next.js ``/offers/<make>-<model>-``
    and Angular ``/a/<make>/<model>/``) and the ``<model>`` part can be spelled
    differently from our search slug — most notably ``rav4`` vs ``rav-4``. We
    generate both spellings so the scraper recognises every valid listing URL
    and the deletion logic can correctly compare against the existing CSV.

    When ``unit.model`` is omitted (make-only search), the patterns match any
    listing under that make regardless of model.
    """
    make = unit.make.lower()
    if not unit.model:
        return sorted({f"/a/{make}/", f"/offers/{make}-"})

    model = unit.model.lower()
    mm_slash = f"{make}/{model}"
    mm_dash = f"{make}-{model}"
    patterns = {
        f"/a/{mm_slash}/",
        f"/offers/{mm_dash}-",
        f"/offers/{mm_dash}/",
    }
    # Insert hyphen between letter and digit (rav4 -> rav-4) to catch the
    # alternate URL spelling AT uses for digit-suffixed models.
    mm_dash_alt = re.sub(r"([a-z])(\d)", r"\1-\2", mm_dash)
    if mm_dash_alt != mm_dash:
        patterns.add(f"/offers/{mm_dash_alt}-")
        patterns.add(f"/offers/{mm_dash_alt}/")
        mm_slash_alt = re.sub(r"([a-z])(\d)", r"\1-\2", mm_slash)
        patterns.add(f"/a/{mm_slash_alt}/")
    return sorted(patterns)


def scrape_vehicle(driver: webdriver.Chrome, search_url: str,
                   unit: SearchUnit, output_file: str,
                   scrape_num: int, scrape_time: str,
                   province_filter: Optional[str] = None,
                   first_page_loaded: bool = False,
                   postal_prefixes: Optional[List[str]] = None,
                   max_new_listings: Optional[int] = None) -> None:
    """Scrape listings for a single vehicle search and save to CSV.

    Updates ``last_scrape_timestamp`` for listings still present and sets
    ``is_deleted`` for listings that have disappeared from search results.
    """
    # Load existing data
    existing_df = None
    if os.path.exists(output_file):
        try:
            existing_df = pd.read_csv(output_file, dtype={"ad_id": str})
        except pd.errors.EmptyDataError:
            existing_df = None

    if existing_df is not None and "source" not in existing_df.columns:
        existing_df["source"] = "autotrader"

    # is_deleted holds an ISO timestamp or NaN. pandas types an all-NaN column
    # as float64, and assigning a string into it is deprecated — cast once here
    # rather than on every assignment below.
    if existing_df is not None and "is_deleted" in existing_df.columns:
        existing_df["is_deleted"] = existing_df["is_deleted"].astype(object)

    existing_urls: Set[str] = set()
    if existing_df is not None and "url" in existing_df.columns:
        existing_urls = set(existing_df["url"].dropna())

    log("Collecting listing URLs...")
    urls, search_healthy = get_listing_urls(
        driver, search_url, first_page_loaded=first_page_loaded,
        postal_prefixes=postal_prefixes)
    # Skip Ontario listings
    urls = [u for u in urls if "/ontario/" not in u.lower()]
    # Restrict to URLs that actually belong to this make/model. The page-level
    # /a/ selector picks up unrelated anchors (related vehicles, navigation,
    # popular searches), so filter both URL flavours by the expected slug.
    url_patterns = _make_model_url_patterns(unit)
    urls = [u for u in urls if any(p in u.lower() for p in url_patterns)]
    log(f"Found {len(urls)} vehicle URLs")

    scraped_url_set = set(urls)

    # Only scrape details for URLs not already in CSV
    new_urls = [u for u in urls if u not in existing_urls]
    if len(new_urls) < len(urls):
        print(f"Skipping {len(urls) - len(new_urls)} already scraped, {len(new_urls)} new")
    urls_to_process = new_urls[:max_new_listings] if max_new_listings else new_urls
    if max_new_listings and len(new_urls) > max_new_listings:
        log(f"  Capping at {max_new_listings} of {len(new_urls)} new listings "
            f"(limits.autotrader.max_new_listings / --limit)")
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
            time.sleep(LISTING_PAUSE_SECS + random.uniform(-5, 5))

    unique_listings = deduplicate_listings(listings)
    log(f"Keeping {len(unique_listings)} unique new listings after deduplication")
    new_df = pd.DataFrame([asdict(l) for l in unique_listings])

    # Province filter applies to the rows we are ADDING, never to the ones
    # already stored: the CSV contract is that listings are flagged, not
    # deleted, and filtering the combined frame would silently drop every
    # stored row whose province is blank (all FB rows whose city parse failed).
    # A null province is no opinion, so it is kept — the read-time filter in
    # generate_scatter_html decides what actually charts.
    if province_filter and not new_df.empty and "province" in new_df.columns:
        wrong_province = new_df["province"].notna() & (new_df["province"] != province_filter)
        dropped = int(wrong_province.sum())
        if dropped:
            log(f"Province filter: dropping {dropped} newly-scraped listings "
                f"outside {province_filter}")
            new_df = new_df[~wrong_province].reset_index(drop=True)

    # Update existing rows for this vehicle type
    if existing_df is not None and not existing_df.empty:
        vehicle_url_pattern = "|".join(re.escape(p) for p in url_patterns)
        vehicle_mask = (
            existing_df["url"].str.contains(vehicle_url_pattern, na=False, regex=True)
            & (existing_df["source"] == "autotrader")
        )

        # Update last_scrape_timestamp for listings still present
        still_present = vehicle_mask & existing_df["url"].isin(scraped_url_set)
        existing_df.loc[still_present, "last_scrape_timestamp"] = scrape_time

        # Resurrection: if a previously-deleted URL reappears in this scrape,
        # clear is_deleted so the row goes back to active. Protects us from
        # earlier false-positive deletions caused by partial scrape failures.
        if "is_deleted" in existing_df.columns:
            resurrected = still_present & existing_df["is_deleted"].notna()
            if resurrected.any():
                log(f"Resurrecting {int(resurrected.sum())} listings that came back "
                    f"(previously marked deleted)")
                existing_df.loc[resurrected, "is_deleted"] = None

        # Deletion safeguard, two independent questions:
        #  1. COULD the search testify? A CAPTCHA or a page that served no link
        #     at all produces an empty or partial scraped_url_set, and the
        #     difference against the CSV then reads as disappearance. Size
        #     checks cannot see this — a blocked search is small, not wrong.
        #  2. Was the scrape big enough? A scrape that returns very few URLs, or
        #     re-discovers only a small fraction of expected listings, is treated
        #     as a partial failure.
        existing_active = int((vehicle_mask & existing_df["is_deleted"].isna()).sum())
        seen_count = int(still_present.sum())
        MIN_URLS_FOR_DELETION = 5
        MIN_HEALTH_RATIO = 0.5
        big_enough = (
            len(scraped_url_set) >= MIN_URLS_FOR_DELETION
            and (existing_active < 3 or seen_count / existing_active >= MIN_HEALTH_RATIO)
        )
        if not search_healthy:
            log("Skipping deletion marking — the search could not testify "
                "(CAPTCHA hit, or no listing link served at all). "
                "Resurrections and last_scrape_timestamp updates still applied.")
        elif big_enough:
            disappeared = (vehicle_mask
                           & ~existing_df["url"].isin(scraped_url_set)
                           & existing_df["is_deleted"].isna())
            if disappeared.any():
                log(f"Marking {int(disappeared.sum())} listings as deleted")
                existing_df.loc[disappeared, "is_deleted"] = scrape_time
        else:
            log(f"Skipping deletion marking — scrape looks unhealthy "
                f"(scraped={len(scraped_url_set)}, "
                f"re-found {seen_count}/{existing_active} expected). "
                f"Treating as partial failure.")

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
    # ad_id as str: float64 rewrites 17-digit ids and loses digits for good.
    df = pd.read_csv(csv_file, dtype={"ad_id": str})
    # year + mileage + price alone identify a physical car: two listings sharing
    # an exact odometer reading at the same price and year are the same vehicle,
    # not a coincidence. make/model stay in the key but are allowed to be null —
    # requiring them exempted precisely the rows most likely to be reposts, since
    # a title like "2009 Pontiac Vibe" yields no make for a Japanese/Korean
    # profile. That left 5 known duplicates (one BMW posted 3x) in the CSV.
    strong_cols = ["year", "mileage_km", "price_cad"]
    eligible = df["is_deleted"].isna() & df[strong_cols].notna().all(axis=1)
    if not eligible.any():
        return 0
    cand = df[eligible].copy()
    cand["_k_make"] = cand["make"].fillna("").astype(str).str.lower().str.strip()
    cand["_k_model"] = cand["model"].fillna("").astype(str).str.lower().str.strip()
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

    from fb_scraper import _ad_id_str

    def _row_url(row: pd.Series) -> str:
        """Best-effort display URL for the drop log. Never raises.

        This runs after the drop set is computed but before the CSV write, so
        an exception here loses the whole pass — and legacy rows carry ad_ids
        as '1588888658875172.0' or '3.56e+16', both of which ``int()`` rejects
        outright. The stored ``url`` wins whenever it is already an FB item
        link: past 2**53 the id's trailing digits are genuinely lost, so a URL
        rebuilt from ``ad_id`` would point at a listing that does not exist,
        while ``url`` still carries the true id.
        """
        url = str(row.get("url") or "")
        if str(row.get("source") or "").lower() == "facebook":
            if "/marketplace/item/" in url:
                return url
            ad_id = _ad_id_str(row.get("ad_id"))
            if ad_id.isdigit():
                return f"https://www.facebook.com/marketplace/item/{ad_id}/"
        return url

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
        kept_row: Optional[pd.Series] = None
        try:
            looked_up = keep_by_key.loc[key]
            if isinstance(looked_up, pd.DataFrame):
                kept_row = looked_up.iloc[0]
            elif isinstance(looked_up, pd.Series):
                kept_row = looked_up
        except KeyError:
            pass
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


_SCATTER_SHAPE_POOL = [
    "circle", "rectRot", "triangle", "rect", "rectRounded",
    "star", "cross", "crossRot", "dash", "line",
]
# Chart.js draws these point styles as stroke-only (no fill). They render at
# 0px without borderWidth, so we must give every stroked shape a real line
# weight — otherwise the point's label appears on the chart with no glyph
# under it. Filled shapes (circle/rect/rectRounded/rectRot/triangle) are
# unaffected.
_SCATTER_LINE_SHAPES = ["star", "cross", "crossRot", "dash", "line"]


def _shape_svg(shape: str) -> str:
    """Return an inline SVG icon matching a Chart.js pointStyle."""
    fills = 'fill="#7f8c8d" stroke="#fff" stroke-width="1"'
    strokes = 'stroke="#7f8c8d" stroke-width="2"'
    svgs = {
        "circle":      f'<circle cx="7" cy="7" r="5" {fills}/>',
        "rectRot":     f'<polygon points="2,7 7,2 12,7 7,12" {fills}/>',
        "triangle":    f'<polygon points="7,2 13,12 1,12" {fills}/>',
        "rect":        f'<rect x="2" y="2" width="10" height="10" {fills}/>',
        "rectRounded": f'<rect x="2" y="2" width="10" height="10" rx="3" {fills}/>',
        "star":        '<polygon points="7,1 9,5 13,5.5 10,8.5 11,13 7,11 3,13 4,8.5 1,5.5 5,5" '
                       'fill="#7f8c8d" stroke="#fff" stroke-width="1"/>',
        "cross":       f'<line x1="7" y1="2" x2="7" y2="12" {strokes}/>'
                       f'<line x1="2" y1="7" x2="12" y2="7" {strokes}/>',
        "crossRot":    f'<line x1="2" y1="2" x2="12" y2="12" {strokes}/>'
                       f'<line x1="2" y1="12" x2="12" y2="2" {strokes}/>',
        "dash":        '<rect x="2" y="6" width="10" height="2" fill="#7f8c8d"/>',
        "line":        f'<line x1="2" y1="7" x2="12" y2="7" {strokes}/>',
    }
    return f'<svg width="14" height="14">{svgs.get(shape, svgs["circle"])}</svg>'


def _iqr_fences(series: pd.Series, k: float = 3.0) -> Tuple[float, float]:
    """Tukey fences at k×IQR — used to drop only *extreme* outliers.

    k=1.5 is the textbook "outlier" threshold but cuts into legitimate
    cheap/high-km listings on small samples. k=3 is the textbook "extreme
    outlier" threshold; it catches things like a $5 broken-car listing or
    a $32k Subaru that slipped past the price filter, without trimming
    real budget-end deals.
    """
    if series.empty:
        return float("-inf"), float("inf")
    q1 = float(series.quantile(0.25))
    q3 = float(series.quantile(0.75))
    iqr = q3 - q1
    return q1 - k * iqr, q3 + k * iqr


def generate_scatter_html(csv_file: str, output_file: str,
                          cfg: Optional["Config"] = None) -> None:
    """Read the CSV and generate an interactive scatter plot HTML file.

    Outlier filtering uses Tukey 3×IQR fences on price and mileage, and the
    axes are then sized in the browser to the points actually displayed (see
    ``computeAxisBounds`` in the inline JS), so each profile gets a chart sized
    to the bulk of its listings instead of being skewed by damaged-car outliers
    or 500k-km lemons. ``cfg.html.chart_price_floor`` / ``chart_price_max``
    clamp those derived price bounds when set; unset means pure auto-scale.

    Models, datasets, and legend checkboxes are also derived from the data —
    no hardcoded model whitelist. Shape encodes MAKE, colour encodes scrape
    freshness.

    This is the read-time authority on what charts: the CSV keeps every row
    ever scraped (flagged, never deleted), and everything filtered out below —
    deleted rows, other profiles' makes, out-of-province listings, non-cars —
    stays on disk.
    """
    from fb_scraper import title_is_non_car

    if cfg is None:
        # Loaded directly (e.g. from a one-off script). Default to Camille's profile.
        cfg = load_config("camille.yaml")
    # ad_id as str: float64 rewrites 17-digit ids and loses digits for good.
    df = pd.read_csv(csv_file, dtype={"ad_id": str})
    df = df[df["is_deleted"].isna()]
    df = df.dropna(subset=["price_cad", "mileage_km", "model"])

    profile_makes = {u.make.capitalize() for u in cfg.search_units}
    if profile_makes:
        df = df[df["make"].isin(profile_makes)]
    if cfg.filters.province and "province" in df.columns:
        df = df[df["province"] == cfg.filters.province]

    # Motorcycles and powersports share FB's Vehicles category with cars and
    # carry a mileage and a year, so only the title tells them apart. The
    # scrape-time filter can't reach rows collected before it existed; apply it
    # again here so they leave the chart without leaving the CSV.
    # Call ``title_is_non_car``, never ``_FB_NON_CAR_TITLE_RX``: a second,
    # make-scoped pattern holds the tokens that are real car names on their own
    # (Rebel, Shadow, a bare GSX), and only the helper reads both. It also
    # absorbs NaN titles, so the column maps as-is.
    if "title" in df.columns:
        non_car = df["title"].map(title_is_non_car)
        if non_car.any():
            log(f"Excluding {int(non_car.sum())} non-car listings (motorcycle / "
                f"powersport titles) from the chart")
            df = df[~non_car]

    # Model casing varies by source ("SANTA FE" vs "Santa Fe", "CR-V" vs
    # "Cr-v"), which used to split one model across two datasets, two legend
    # entries and two shapes. Normalise once, here, so every downstream use
    # — datasets, legend, shape assignment, datalabels — shares one key.
    df["model"] = df["model"].astype(str).str.strip().str.upper()

    # Drop only *extreme* outliers (3×IQR — broken-car $5 listings, $32k
    # over-budget cars that slipped past the URL filter, etc.). Real
    # budget-end deals stay in.
    price_fence_lo, price_fence_hi = _iqr_fences(df["price_cad"])
    km_fence_lo, km_fence_hi = _iqr_fences(df["mileage_km"])
    df = df[(df["price_cad"] >= price_fence_lo) & (df["price_cad"] <= price_fence_hi)
            & (df["mileage_km"] >= km_fence_lo) & (df["mileage_km"] <= km_fence_hi)]

    # Freshness tiers: "latest" (last scrape), "recent" (today/yesterday), "old"
    scrape_nums = df["scrape_number"].dropna().unique()
    max_scrape = df["scrape_number"].max() if len(scrape_nums) else 0
    has_multiple = len(scrape_nums) > 1
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    records = []
    for _, row in df.iterrows():
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
            "source": str(row["source"]).lower() if pd.notna(row.get("source")) else "autotrader",
            "make": row["make"],
            "model": str(row["model"]),
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

    # Per-model datasets, ordered by frequency so the most common models sit
    # at the top of each make's list in the dropdown.
    model_counts = df["model"].value_counts() if not df.empty else pd.Series(dtype=int)
    model_order = list(model_counts.index)
    model_to_make = {m: str(df.loc[df["model"] == m, "make"].iloc[0])
                     for m in model_order}

    # Shape encodes MAKE, not model. Émile's profile alone reaches ~150
    # distinct models against a 10-glyph pool, so a per-model shape repeats
    # roughly 15 times over and tells the reader nothing; the ~13 makes fit the
    # pool almost exactly. Datasets stay per-model (the legend and the
    # datalabels still name the model) — only the glyph is shared.
    make_order = list(df["make"].value_counts().index) if not df.empty else []
    make_shapes = {mk: _SCATTER_SHAPE_POOL[i % len(_SCATTER_SHAPE_POOL)]
                   for i, mk in enumerate(make_order)}
    model_shapes = {m: make_shapes.get(model_to_make[m], _SCATTER_SHAPE_POOL[0])
                    for m in model_order}

    # Hierarchical make → models for the Cars dropdown. Makes ordered
    # alphabetically; models within a make ordered by overall frequency.
    makes_to_models: Dict[str, List[str]] = {}
    for m in model_order:
        makes_to_models.setdefault(model_to_make[m], []).append(m)
    makes_sorted = sorted(makes_to_models.keys())

    # Slider bounds — derived from the post-IQR-fence data so the sliders
    # cover the visible range exactly. Empty-data fallback keeps the
    # markup well-formed.
    if df.empty:
        year_lo, year_hi = 2000, 2025
        price_data_lo, price_data_hi = 0, 1
        km_data_lo, km_data_hi = 0, 1
    else:
        year_lo = int(df["year"].min())
        year_hi = int(df["year"].max())
        price_data_lo = int(df["price_cad"].min())
        price_data_hi = int(df["price_cad"].max())
        km_data_lo = int(df["mileage_km"].min())
        km_data_hi = int(df["mileage_km"].max())
    if year_hi <= year_lo:
        year_hi = year_lo + 1
    if price_data_hi <= price_data_lo:
        price_data_hi = price_data_lo + 1
    if km_data_hi <= km_data_lo:
        km_data_hi = km_data_lo + 1

    sources_present = sorted({r["source"] for r in records}) or ["autotrader"]
    source_label = {"autotrader": "AutoTrader", "facebook": "Facebook"}

    def esc(s: str) -> str:
        return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                 .replace('"', "&quot;"))

    cars_dropdown_html_parts: List[str] = []
    for mk in makes_sorted:
        cars_dropdown_html_parts.append('<div class="make-group">')
        cars_dropdown_html_parts.append(
            f'<label class="make-label"><input type="checkbox" class="make-cb" '
            f'data-make="{esc(mk)}" checked><span class="make-name">{esc(mk)}</span></label>'
        )
        cars_dropdown_html_parts.append('<div class="model-list">')
        for m in makes_to_models[mk]:
            cars_dropdown_html_parts.append(
                f'<label class="model-label"><input type="checkbox" class="model-cb" '
                f'data-make="{esc(mk)}" data-model="{esc(m)}" checked>'
                f'{_shape_svg(model_shapes[m])}<span class="model-name">{esc(m)}</span></label>'
            )
        cars_dropdown_html_parts.append('</div>')
        cars_dropdown_html_parts.append('</div>')
    cars_dropdown_html = "\n      ".join(cars_dropdown_html_parts)

    source_dropdown_html_parts: List[str] = []
    for src in sources_present:
        lbl = source_label.get(src, src.title())
        source_dropdown_html_parts.append(
            f'<label class="source-label"><input type="checkbox" class="source-cb" '
            f'data-source="{esc(src)}" checked><span>{esc(lbl)}</span></label>'
        )
    source_dropdown_html = "\n      ".join(source_dropdown_html_parts)

    shapes_js = json.dumps(model_shapes, ensure_ascii=False)
    line_shapes_js = json.dumps(_SCATTER_LINE_SHAPES)
    # Optional hard clamps on the price axis. The axes are computed in the
    # browser (they follow the user's filters), so the knobs have to travel
    # there — computing them in Python left them with nothing to affect.
    price_floor_js = json.dumps(cfg.html.chart_price_floor)
    price_max_js = json.dumps(cfg.html.chart_price_max)

    json_data = json.dumps(records, ensure_ascii=False)

    page_title_html = (cfg.html.page_title or "")\
        .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    heading_html = (cfg.html.heading or "")\
        .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    if cfg.html.public_url:
        live_link_html = (
            f'<a class="live-link" href="{cfg.html.public_url}" '
            f'target="_blank">view live</a>'
        )
    else:
        live_link_html = ""

    html = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>''' + page_title_html + '''</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2"></script>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  html, body { height: 100%; height: 100dvh; }
  body { font-family: -apple-system, system-ui, sans-serif; background: #1a1a2e; color: #eee; padding: 12px; display: flex; flex-direction: column; }
  .heading { display: flex; align-items: baseline; justify-content: center; gap: 12px; margin-bottom: 6px; }
  .heading h1 { font-size: 1.05rem; font-weight: 600; color: #eee; }
  .heading .live-link { color: #3498db; font-size: 0.78rem; text-decoration: none; }
  .heading .live-link:hover { text-decoration: underline; }
  .controls { display: flex; flex-direction: column; gap: 6px; margin-bottom: 8px; font-size: 0.85rem; }
  .controls-row { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; }
  .controls-row.dropdown-row { justify-content: center; }
  .controls svg { vertical-align: middle; }
  .dropdown { position: relative; }
  .dd-toggle { background: #16213e; color: #eee; border: 1px solid #555; padding: 6px 14px; border-radius: 6px; cursor: pointer; font-size: 0.9rem; }
  .dd-toggle:hover { background: #1e2c50; }
  .dd-toggle .dd-caret { margin-left: 8px; opacity: 0.7; }
  .dd-toggle .dd-summary { color: #aaa; font-size: 0.78rem; margin-left: 6px; }
  .dd-panel { display: none; position: absolute; top: calc(100% + 4px); left: 0; background: #16213e; border: 1px solid #555; border-radius: 8px; padding: 8px; min-width: 240px; width: max-content; max-width: calc(100vw - 24px); max-height: 70vh; overflow-y: auto; z-index: 50; box-shadow: 0 4px 12px rgba(0,0,0,0.4); -webkit-overflow-scrolling: touch; }
  .dropdown.open .dd-panel { display: block; }
  .dd-content { display: flex; flex-direction: column; }
  .all-row { padding: 8px 4px; border-bottom: 1px solid #2a2a4a; margin-bottom: 4px; }
  .all-label { display: flex; align-items: center; gap: 8px; cursor: pointer; min-height: 36px; font-weight: 600; -webkit-tap-highlight-color: transparent; }
  .make-group { margin-bottom: 2px; }
  .make-label { display: flex; align-items: center; gap: 8px; cursor: pointer; padding: 6px 4px; min-height: 36px; font-weight: 600; -webkit-tap-highlight-color: transparent; }
  .model-list { display: flex; flex-direction: column; padding-left: 26px; }
  .model-label { display: flex; align-items: center; gap: 8px; cursor: pointer; font-weight: 400; padding: 6px 4px; min-height: 32px; -webkit-tap-highlight-color: transparent; }
  .source-label { display: flex; align-items: center; gap: 8px; cursor: pointer; padding: 6px 4px; min-height: 36px; -webkit-tap-highlight-color: transparent; }
  .all-cb, .make-cb, .model-cb, .source-cb { width: 18px; height: 18px; cursor: pointer; flex-shrink: 0; }
  .slider-group { display: flex; align-items: center; gap: 10px; flex: 1 1 280px; min-width: 240px; }
  .slider-group .slider-label { color: #bbb; min-width: 60px; font-size: 0.82rem; }
  .slider-group .slider-display { color: #ddd; min-width: 130px; font-size: 0.82rem; font-variant-numeric: tabular-nums; text-align: right; }
  .dual-slider { position: relative; flex: 1; height: 32px; min-width: 120px; touch-action: pan-y; }
  .dual-slider .track { position: absolute; left: 0; right: 0; top: 14px; height: 4px; background: #2a2a4a; border-radius: 2px; pointer-events: none; }
  .dual-slider .track-active { position: absolute; top: 14px; height: 4px; background: #3498db; border-radius: 2px; pointer-events: none; }
  .dual-slider input[type="range"] { position: absolute; left: 0; right: 0; top: 0; width: 100%; height: 32px; background: transparent; -webkit-appearance: none; appearance: none; pointer-events: none; margin: 0; -webkit-tap-highlight-color: transparent; }
  .dual-slider input[type="range"]::-webkit-slider-thumb { pointer-events: auto; -webkit-appearance: none; width: 22px; height: 22px; border-radius: 50%; background: #3498db; cursor: pointer; border: 2px solid #fff; box-shadow: 0 1px 4px rgba(0,0,0,0.5); }
  .dual-slider input[type="range"]::-moz-range-thumb { pointer-events: auto; width: 22px; height: 22px; border-radius: 50%; background: #3498db; cursor: pointer; border: 2px solid #fff; box-shadow: 0 1px 4px rgba(0,0,0,0.5); }
  .dual-slider input[type="range"]::-webkit-slider-runnable-track { background: transparent; }
  .dual-slider input[type="range"]::-moz-range-track { background: transparent; }
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

<div class="heading">
  <h1>''' + heading_html + '''</h1>
  ''' + live_link_html + '''
</div>

<div class="controls">
  <div class="controls-row dropdown-row">
    <div class="dropdown" data-dropdown="cars">
      <button class="dd-toggle" type="button">Cars<span class="dd-summary"></span><span class="dd-caret">▾</span></button>
      <div class="dd-panel">
        <div class="dd-content">
          <div class="all-row">
            <label class="all-label"><input type="checkbox" class="all-cb" checked><span>All</span></label>
          </div>
      ''' + cars_dropdown_html + '''
        </div>
      </div>
    </div>
    <div class="dropdown" data-dropdown="source">
      <button class="dd-toggle" type="button">Source<span class="dd-summary"></span><span class="dd-caret">▾</span></button>
      <div class="dd-panel">
        <div class="dd-content">
      ''' + source_dropdown_html + '''
        </div>
      </div>
    </div>
  </div>
  <div class="controls-row">
    <div class="slider-group" data-slider="year">
      <span class="slider-label">Year</span>
      <div class="dual-slider">
        <div class="track"></div>
        <div class="track-active"></div>
        <input type="range" class="slider-min" min="''' + str(year_lo) + '''" max="''' + str(year_hi) + '''" step="1" value="''' + str(year_lo) + '''">
        <input type="range" class="slider-max" min="''' + str(year_lo) + '''" max="''' + str(year_hi) + '''" step="1" value="''' + str(year_hi) + '''">
      </div>
      <span class="slider-display"></span>
    </div>
    <div class="slider-group" data-slider="price">
      <span class="slider-label">Price</span>
      <div class="dual-slider">
        <div class="track"></div>
        <div class="track-active"></div>
        <input type="range" class="slider-min" min="''' + str(price_data_lo) + '''" max="''' + str(price_data_hi) + '''" step="50" value="''' + str(price_data_lo) + '''">
        <input type="range" class="slider-max" min="''' + str(price_data_lo) + '''" max="''' + str(price_data_hi) + '''" step="50" value="''' + str(price_data_hi) + '''">
      </div>
      <span class="slider-display"></span>
    </div>
    <div class="slider-group" data-slider="mileage">
      <span class="slider-label">Mileage</span>
      <div class="dual-slider">
        <div class="track"></div>
        <div class="track-active"></div>
        <input type="range" class="slider-min" min="''' + str(km_data_lo) + '''" max="''' + str(km_data_hi) + '''" step="1000" value="''' + str(km_data_lo) + '''">
        <input type="range" class="slider-max" min="''' + str(km_data_lo) + '''" max="''' + str(km_data_hi) + '''" step="1000" value="''' + str(km_data_hi) + '''">
      </div>
      <span class="slider-display"></span>
    </div>
  </div>
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
const SHAPES = ''' + shapes_js + ''';
const LINE_SHAPES = new Set(''' + line_shapes_js + ''');
const isLineShape = (model) => LINE_SHAPES.has(SHAPES[model]);
// html.chart_price_floor / html.chart_price_max from the profile YAML.
// null = no clamp, i.e. pure auto-scale.
const PRICE_FLOOR = ''' + price_floor_js + ''';
const PRICE_MAX = ''' + price_max_js + ''';
const COLORS = { latest: '#ffd700', recent: '#2ecc71', old: '#7f8c8d' };
const SIZES  = { latest: { r: 7, hr: 9, bw: 0 }, recent: { r: 7, hr: 9, bw: 0 }, old: { r: 7, hr: 9, bw: 0 } };
const SOURCE_LABELS = { autotrader: 'AutoTrader', facebook: 'Facebook' };
const normSource = v => (v || 'autotrader').toString().toLowerCase();

const data = ''' + json_data + ''';

const datasets = {};
Object.keys(SHAPES).forEach(model => {
  datasets[model] = { label: model, data: [], meta: [], backgroundColor: [], borderColor: [], pointRadius: [], pointHoverRadius: [], pointBorderColor: [], pointBorderWidth: [], pointStyle: SHAPES[model], showLine: false };
});

// Filter state — single ``applied`` object. Every checkbox / slider input
// mutates this directly and triggers ``rebuild + chart.update`` inline; the
// dataset is small enough (few hundred points worst case) that filtering
// in JS is sub-frame, so an Apply step would just add a click.
const allModels = new Set(Object.keys(SHAPES));
const allSources = new Set([...document.querySelectorAll('.source-cb')].map(c => c.dataset.source));
const sliderEl = (key) => document.querySelector('.slider-group[data-slider="' + key + '"]');
const readSliderRange = (key) => {
  const g = sliderEl(key);
  return [parseFloat(g.querySelector('.slider-min').value),
          parseFloat(g.querySelector('.slider-max').value)];
};
const [yearMin0, yearMax0] = readSliderRange('year');
const [priceMin0, priceMax0] = readSliderRange('price');
const [kmMin0, kmMax0] = readSliderRange('mileage');

const applied = {
  models: new Set(allModels),
  sources: new Set(allSources),
  yearMin: yearMin0, yearMax: yearMax0,
  priceMin: priceMin0, priceMax: priceMax0,
  kmMin: kmMin0, kmMax: kmMax0,
};

function rebuild() {
  Object.values(datasets).forEach(ds => {
    ds.data = []; ds.meta = []; ds.backgroundColor = []; ds.borderColor = [];
    ds.pointRadius = []; ds.pointHoverRadius = []; ds.pointBorderColor = []; ds.pointBorderWidth = [];
  });
  data.forEach(d => {
    if (!applied.sources.has(normSource(d.source))) return;
    if (!applied.models.has(d.model)) return;
    if (d.year < applied.yearMin || d.year > applied.yearMax) return;
    if (d.price_cad < applied.priceMin || d.price_cad > applied.priceMax) return;
    if (d.mileage_km < applied.kmMin || d.mileage_km > applied.kmMax) return;
    const ds = datasets[d.model];
    if (!ds) return;
    ds.data.push({ x: d.mileage_km, y: d.price_cad });
    ds.meta.push(d);
    const f = d.freshness || 'old';
    const c = COLORS[f], s = SIZES[f];
    ds.backgroundColor.push(c);
    ds.borderColor.push(c);
    // Stroke-only shapes need bigger radius + thicker stroke to read at the
    // same visual weight as filled shapes.
    const stroked = isLineShape(d.model);
    ds.pointRadius.push(stroked ? s.r + 1 : s.r);
    ds.pointHoverRadius.push(stroked ? s.hr + 1 : s.hr);
    ds.pointBorderColor.push(c);
    ds.pointBorderWidth.push(stroked ? 2.5 : 0);
  });
}
rebuild();

// Axis bounds derived from the data we just put on the chart (post-filter),
// so the view hugs the actually-displayed points rather than the broader
// IQR-fenced range. PRICE_FLOOR / PRICE_MAX clamp the price axis on top.
function computeAxisBounds() {
  let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
  Object.values(datasets).forEach(ds => {
    ds.data.forEach(p => {
      if (p.x < minX) minX = p.x;
      if (p.x > maxX) maxX = p.x;
      if (p.y < minY) minY = p.y;
      if (p.y > maxY) maxY = p.y;
    });
  });
  if (!isFinite(minX)) return null;
  const padX = Math.max(1, (maxX - minX) * 0.04);
  const padY = Math.max(1, (maxY - minY) * 0.04);
  let yMin = Math.floor(minY - padY), yMax = Math.ceil(maxY + padY);
  if (PRICE_FLOOR !== null) yMin = Math.max(yMin, PRICE_FLOOR);
  if (PRICE_MAX !== null) yMax = Math.min(yMax, PRICE_MAX);
  if (yMax <= yMin) yMax = yMin + 1;
  return {
    xMin: Math.floor(minX - padX), xMax: Math.ceil(maxX + padX),
    yMin: yMin, yMax: yMax,
  };
}
const initialBounds = computeAxisBounds() || { xMin: 0, xMax: 1, yMin: 0, yMax: 1 };

function applyAxisBounds(b) {
  if (!b) return;
  chart.options.scales.x.min = b.xMin;
  chart.options.scales.x.max = b.xMax;
  chart.options.scales.y.min = b.yMin;
  chart.options.scales.y.max = b.yMax;
}
// Single entry point for any filter change: rebuild visible datasets, hug
// the axes to them, redraw without animation. Sub-frame on the full dataset.
function refreshChart() {
  rebuild();
  applyAxisBounds(computeAxisBounds());
  chart.update('none');
}

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
        min: initialBounds.xMin,
        max: initialBounds.xMax,
        title: { display: true, text: 'Kilometres', color: '#aaa', font: { size: 14 } },
        ticks: { color: '#888', callback: v => (v/1000).toFixed(0) + 'k' },
        grid: { color: '#2a2a4a' }
      },
      y: {
        min: initialBounds.yMin,
        max: initialBounds.yMax,
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
      if ('ontouchstart' in window) return;
      if (!elements.length) return;
      const el = elements[0];
      const ds = chart.data.datasets[el.datasetIndex];
      const item = ds.meta[el.index];
      selectedKey = el.datasetIndex + '-' + el.index;
      showTooltip(item, e.native.clientX, e.native.clientY);
    }
  }
});

// ── Cars dropdown: tri-state hierarchy (All → Make → Model) ────────────
function cssEscape(s) {
  return (window.CSS && CSS.escape) ? CSS.escape(s) : s.replace(/"/g, '\\\\"');
}
const allCb = document.querySelector('.all-cb');

function syncMakeFromModels(makeName) {
  const makeBox = document.querySelector('.make-cb[data-make="' + cssEscape(makeName) + '"]');
  if (!makeBox) return;
  const siblings = [...document.querySelectorAll('.model-cb[data-make="' + cssEscape(makeName) + '"]')];
  const checked = siblings.filter(c => c.checked).length;
  if (checked === 0) { makeBox.checked = false; makeBox.indeterminate = false; }
  else if (checked === siblings.length) { makeBox.checked = true; makeBox.indeterminate = false; }
  else { makeBox.checked = false; makeBox.indeterminate = true; }
}
function syncAllFromState() {
  if (!allCb) return;
  const total = allModels.size;
  const picked = applied.models.size;
  if (picked === 0) { allCb.checked = false; allCb.indeterminate = false; }
  else if (picked === total) { allCb.checked = true; allCb.indeterminate = false; }
  else { allCb.checked = false; allCb.indeterminate = true; }
}
function updateDropdownSummary(name) {
  const dd = document.querySelector('.dropdown[data-dropdown="' + name + '"]');
  if (!dd) return;
  const summaryEl = dd.querySelector('.dd-summary');
  if (name === 'cars') {
    const total = allModels.size, picked = applied.models.size;
    summaryEl.textContent = ' ' + (picked === total ? '(all)' : '(' + picked + '/' + total + ')');
  } else if (name === 'source') {
    const total = allSources.size, picked = applied.sources.size;
    summaryEl.textContent = ' ' + (picked === total ? '(all)' : '(' + picked + '/' + total + ')');
  }
}
function commitCars() {
  syncAllFromState();
  updateDropdownSummary('cars');
  refreshChart();
}
function commitSources() {
  updateDropdownSummary('source');
  refreshChart();
}

if (allCb) {
  allCb.addEventListener('change', () => {
    const v = allCb.checked;
    document.querySelectorAll('.model-cb').forEach(cb => {
      cb.checked = v;
      if (v) applied.models.add(cb.dataset.model); else applied.models.delete(cb.dataset.model);
    });
    document.querySelectorAll('.make-cb').forEach(cb => {
      cb.checked = v; cb.indeterminate = false;
    });
    allCb.indeterminate = false;
    commitCars();
  });
}

document.querySelectorAll('.make-cb').forEach(cb => {
  cb.addEventListener('change', () => {
    const make = cb.dataset.make;
    document.querySelectorAll('.model-cb[data-make="' + cssEscape(make) + '"]').forEach(m => {
      m.checked = cb.checked;
      if (cb.checked) applied.models.add(m.dataset.model); else applied.models.delete(m.dataset.model);
    });
    cb.indeterminate = false;
    commitCars();
  });
});
document.querySelectorAll('.model-cb').forEach(cb => {
  cb.addEventListener('change', () => {
    if (cb.checked) applied.models.add(cb.dataset.model); else applied.models.delete(cb.dataset.model);
    syncMakeFromModels(cb.dataset.make);
    commitCars();
  });
});
document.querySelectorAll('.source-cb').forEach(cb => {
  cb.addEventListener('change', () => {
    if (cb.checked) applied.sources.add(cb.dataset.source); else applied.sources.delete(cb.dataset.source);
    commitSources();
  });
});

// ── Dropdown open/close (no commit step — changes are live) ──────────
document.querySelectorAll('.dropdown').forEach(dd => {
  const toggle = dd.querySelector('.dd-toggle');
  toggle.addEventListener('click', e => {
    e.stopPropagation();
    const wasOpen = dd.classList.contains('open');
    document.querySelectorAll('.dropdown.open').forEach(o => o.classList.remove('open'));
    if (!wasOpen) dd.classList.add('open');
  });
  dd.querySelector('.dd-panel').addEventListener('click', e => e.stopPropagation());
});
document.addEventListener('click', e => {
  if (e.target.closest('.dropdown')) return;
  document.querySelectorAll('.dropdown.open').forEach(o => o.classList.remove('open'));
});
updateDropdownSummary('cars');
updateDropdownSummary('source');
syncAllFromState();

// ── Sliders: dual-handle ranges with live chart updates ───────────────
const SLIDER_FORMATTERS = {
  year: v => String(v),
  price: v => '$' + Math.round(v).toLocaleString(),
  mileage: v => Math.round(v / 1000) + 'k km',
};
function setupSlider(name) {
  const group = sliderEl(name);
  if (!group) return;
  const lo = group.querySelector('.slider-min');
  const hi = group.querySelector('.slider-max');
  const display = group.querySelector('.slider-display');
  const trackActive = group.querySelector('.track-active');
  const fmt = SLIDER_FORMATTERS[name];
  const fullLo = parseFloat(lo.min);
  const fullHi = parseFloat(lo.max);
  const span = fullHi - fullLo;
  function refresh() {
    let minV = parseFloat(lo.value);
    let maxV = parseFloat(hi.value);
    if (minV > maxV) {
      // Whichever handle is being dragged stays put; the other yields.
      if (document.activeElement === lo) { hi.value = minV; maxV = minV; }
      else { lo.value = maxV; minV = maxV; }
    }
    display.textContent = fmt(minV) + ' – ' + fmt(maxV);
    if (span > 0) {
      const leftPct = ((minV - fullLo) / span) * 100;
      const rightPct = ((maxV - fullLo) / span) * 100;
      trackActive.style.left = leftPct + '%';
      trackActive.style.right = (100 - rightPct) + '%';
    }
    if (name === 'year') { applied.yearMin = minV; applied.yearMax = maxV; }
    else if (name === 'price') { applied.priceMin = minV; applied.priceMax = maxV; }
    else if (name === 'mileage') { applied.kmMin = minV; applied.kmMax = maxV; }
    refreshChart();
  }
  lo.addEventListener('input', refresh);
  hi.addEventListener('input', refresh);
  refresh();
}
setupSlider('year');
setupSlider('price');
setupSlider('mileage');
</script>

</body>
</html>'''

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)
    log(f"Scatter plot written to {output_file}")


def _parse_args():
    import argparse
    p = argparse.ArgumentParser(
        description="Scrape used-vehicle listings and update the scatter plot.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
HOW LIMITS WORK
---------------
A "listing" counted by --limit is a NEW one: anything already in the profile's
CSV is skipped for free and does NOT count toward the limit. So re-running does
not re-collect the same cars — it picks up where the last run stopped.

The limit applies PER SEARCH UNIT, not per run:
  AutoTrader  once per make/model in autotrader.search_units
  Facebook    once per entry in facebook.queries
With 13 AutoTrader makes, --limit 50 therefore allows up to 650 new listings.

Defaults live in the profile YAML and differ per source:

  limits:
    autotrader: {max_new_listings: null}   # null = take everything served
    facebook:   {max_new_listings: 400}

--limit overrides BOTH sources for this run. Omit it to use the profile.

SOLD LISTINGS
-------------
Every Facebook scrape first re-opens EVERY active Facebook listing already in
the CSV, one at a time, and retires the ones that are sold or expired. Nothing
is sampled: measured at 13.3s per listing end to end, so ~680 stored listings
need about 2.5 HOURS before the scrape itself starts. Progress is saved to disk
every 20 listings, so an interrupted pass keeps what it already checked.
Use --no-delete-sold to skip it.

Retired rows are flagged (is_deleted), never removed — they leave the chart but
stay in the CSV. AutoTrader needs no such pass: re-scanning its search results
already retires listings that vanished.

A run can still stop before reaching the limit — that is normal, and means the
source ran out of matching listings. The scrapers also carry internal
anti-runaway valves (consecutive-reject and no-new-page/card detectors, absolute
iteration ceilings). Those are not tunable and are sized so they never cut a run
short before the limit you asked for. See "Stop conditions" in CLAUDE.md.

COST
----
Each new listing costs a detail page fetch: ~5s on Facebook, ~20s on AutoTrader
(plus 45s between AutoTrader makes). --limit is the main lever on run time.

EXAMPLES
--------
  %(prog)s --config emile.yaml                      # both sources, profile limits
  %(prog)s --config emile.yaml --source facebook    # Facebook only
  %(prog)s --config emile.yaml --limit 25           # quick test run
  %(prog)s --config emile.yaml --days 30            # widen the FB window
  %(prog)s --config emile.yaml --generate-html-only # rebuild the plot, no scrape
""")
    p.add_argument("--config", default="camille.yaml",
                   help="Path to a profile YAML (default: camille.yaml).")
    p.add_argument("--source", choices=["autotrader", "facebook", "all"], default="all",
                   help="Which source(s) to scrape (default: all). AutoTrader runs "
                        "first and is the slow half — use 'facebook' to skip it.")
    p.add_argument("--limit", type=int, default=None, metavar="N",
                   help="Cap NEW listings at N per search unit (AutoTrader) and per "
                        "query (Facebook). Already-scraped listings are skipped and "
                        "do not count. Overrides limits.<source>.max_new_listings for "
                        "both sources; omit to use the profile. See notes below.")
    p.add_argument("--days", type=int, default=None, metavar="D",
                   help="Facebook only: only consider ads posted in the last D days. "
                        "By default this is derived from the last run (time since it, "
                        "plus 2 days of overlap), so a small window after a recent "
                        "scrape is expected. Widen it to backfill older ads.")
    p.add_argument("--make-model", dest="make_model", default=None,
                   help="Scrape only one vehicle slug (e.g. 'subaru/forester' or 'toyota'). "
                        "Must match a make[/model] from the active profile.")
    p.add_argument("--generate-html-only", action="store_true",
                   help="Skip scraping — just regenerate the HTML plot from the existing CSV.")
    p.add_argument("--full-delete-sweep", dest="full_delete_sweep", action="store_true",
                   help="Force the sold-listing pass to re-open EVERY active FB listing, "
                        "ignoring facebook.defaults.sweep_recheck_days.")
    p.add_argument("--no-delete-sold", dest="no_delete_sold", action="store_true",
                   help="Skip the sold-listing pass. By default, every Facebook scrape "
                        "is followed by a pass that re-opens each active FB listing with "
                        "no sign of life in sweep_recheck_days and retires "
                        "the ones that are sold or expired (~13s each). "
                        "AutoTrader needs "
                        "no such pass — its search re-scan already retires vanished "
                        "listings.")
    p.add_argument("--keep-browser", dest="keep_browser", action="store_true",
                   help="Leave the Chrome window open when the Facebook run ends, so "
                        "you can inspect the last page. The window keeps fb_profile/ "
                        "locked, so close it before the next run.")
    p.add_argument("--no-publish", action="store_true",
                   help="Do not commit/push the HTML to GitHub Pages.")
    return p.parse_args()


def _at_get_with_retry(driver, url: str, attempts: int = 3,
                       wait_secs: int = 30) -> bool:
    """driver.get with retries; True on success, False when all attempts fail.

    A memory-starved machine makes renderer timeouts routine (measured
    2026-08-11: the cold warm-up load died at the 30s page-load timeout and
    took the whole run with it). Waiting between attempts gives the box time
    to page Chrome back in.
    """
    for attempt in range(1, attempts + 1):
        try:
            driver.get(url)
            return True
        except (TimeoutException, WebDriverException) as exc:
            log(f"Navigation to {url} failed (attempt {attempt}/{attempts}, "
                f"{type(exc).__name__})")
            if attempt < attempts:
                time.sleep(wait_secs)
    return False


def _scrape_autotrader(units: List[SearchUnit], cfg: Config,
                       scrape_num: int, scrape_time: str, max_new_listings: Optional[int] = None) -> None:
    first = units[0]
    first_search_url = build_at_search_url(first, cfg.autotrader_search)
    driver = create_driver(headless=False)
    log("Warming up AutoTrader session...")
    if not _at_get_with_retry(driver, "https://www.autotrader.ca/"):
        log("AutoTrader unreachable after retries — skipping the AutoTrader pass "
            "(no rows touched; the deletion guard treats this as no-testimony)")
        try:
            driver.quit()
        except Exception:
            pass
        return
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
    first_page_loaded = _at_get_with_retry(driver, first_search_url)
    if first_page_loaded:
        time.sleep(8)

    postal_prefixes = cfg.filters.allowed_postal_prefixes
    try:
        for idx, unit in enumerate(units):
            if idx > 0:
                log(f"Pausing {VEHICLE_PAUSE_SECS}s between vehicle searches...")
                time.sleep(VEHICLE_PAUSE_SECS)
            search_url = (first_search_url if idx == 0
                          else build_at_search_url(unit, cfg.autotrader_search))
            print(f"\n{'='*60}\nScraping AutoTrader: {unit.slug}\n{'='*60}")
            try:
                scrape_vehicle(driver, search_url, unit, cfg.output.csv,
                               scrape_num, scrape_time,
                               province_filter=cfg.filters.province,
                               first_page_loaded=(idx == 0 and first_page_loaded),
                               postal_prefixes=postal_prefixes,
                               max_new_listings=max_new_listings)
            except (TimeoutException, WebDriverException) as exc:
                # One flaky page must cost one make, never the run. The unit's
                # deletion marking either ran on a healthy collection or never
                # ran at all, so no rows are falsely flagged by bailing here.
                log(f"Unit '{unit.slug}' failed ({type(exc).__name__}: "
                    f"{str(exc).splitlines()[0][:160]}) — recreating the driver "
                    "and moving to the next make")
                try:
                    driver.quit()
                except Exception:
                    pass
                try:
                    driver = create_driver(headless=False)
                    if not _at_get_with_retry(driver, "https://www.autotrader.ca/"):
                        log("Driver recreated but AutoTrader unreachable — "
                            "aborting the AutoTrader pass")
                        return
                    time.sleep(5)
                except Exception as exc2:
                    log(f"Driver recreation failed ({type(exc2).__name__}) — "
                        "aborting the AutoTrader pass")
                    return
    finally:
        try:
            driver.quit()
        except Exception:
            pass


_FB_TRACE_CARD_LINE_RX = re.compile(r"^\[\d{2}:\d{2}:\d{2}\] ")


def _fb_cards_traced() -> int:
    """Count per-card lines written to the FB card trace so far.

    ``scrape_vehicle_facebook`` returns nothing, so this is how the caller
    learns whether the feed actually served cards: every card the scraper
    evaluates gets one ``[HH:MM:SS] TAG | ...`` line, kept or rejected.
    Section headers and the file banner don't match. Returns 0 when the file
    can't be read — an unreadable trace must not be mistaken for a scrape that
    produced results.
    """
    try:
        from fb_scraper import FB_CARD_TRACE_FILE
        with open(FB_CARD_TRACE_FILE, "r") as f:
            return sum(1 for line in f if _FB_TRACE_CARD_LINE_RX.match(line))
    except Exception:
        return 0


def _scrape_facebook(cfg: Config, scrape_num: int, scrape_time: str,
                     max_listings: Optional[int],
                     days_override: Optional[int] = None,
                     delete_sold: bool = True,
                     full_sweep: bool = False,
                     keep_browser: bool = False) -> None:
    from fb_scraper import (
        create_fb_driver, scrape_vehicle_facebook,
        load_fb_scrape_state, save_fb_scrape_state, reset_card_trace,
        delete_sold_listings,
    )
    if not cfg.fb_queries:
        log("No FB queries configured — skipping Facebook scrape")
        return
    reset_card_trace()
    # Per-profile state file: profiles share the repo but must not share the
    # last-scrape timestamp, or one profile's recent run silently narrows the
    # other's lookback window.
    state_path = f".fb_scrape_state.{cfg.profile_name}.json"
    # Snapshot the last-scrape timestamp ONCE — so every vehicle in this run
    # uses the same daysSinceListed window (computed from the prior run's end).
    session_last_ts = load_fb_scrape_state(state_path).get("last_fb_scrape_timestamp")
    allowed_provinces = {cfg.filters.province} if cfg.filters.province else None
    # Did the feed serve anything at all this run? Advancing the stored
    # timestamp after a run that saw zero cards narrows the next run's
    # daysSinceListed window for a scrape that never happened, and the loss
    # compounds silently across runs.
    saw_any_card = False
    driver = create_fb_driver(headless=False)
    try:
        for q in cfg.fb_queries:
            print(f"\n{'='*60}\nScraping Facebook: query={q.query!r}\n{'='*60}")
            cards_before = _fb_cards_traced()
            scrape_vehicle_facebook(
                driver, query=q.query,
                model_regex_src=q.regex, model_canonical=q.model_canonical,
                make=q.make,
                output_file=cfg.output.csv, scrape_num=scrape_num, scrape_time=scrape_time,
                max_days_since_listed=cfg.fb_defaults.days_since_listed,
                max_listings=max_listings, year_range=q.year_range,
                max_price=cfg.fb_defaults.price_max,
                min_price=cfg.fb_defaults.price_min,
                allowed_provinces=allowed_provinces,
                session_last_scrape_timestamp=session_last_ts,
                days_override=days_override,
            )
            cards_seen = _fb_cards_traced() - cards_before
            if cards_seen > 0:
                saw_any_card = True
            else:
                log(f"  Query {q.query!r} saw no card at all")
        # Sweep AFTER scraping, on purpose: the scroll pass stamps
        # last_scrape_timestamp on every row it re-finds in the feed, and each
        # of those stamps is a free proof of life the sweep can skip. Skipping
        # can only DELAY a flag (the row is re-checked within
        # sweep_recheck_days), never create a false one.
        if delete_sold:
            from fb_scraper import FB_POST_SWEEP_COOLDOWN_SECS
            log(f"Cooling down {FB_POST_SWEEP_COOLDOWN_SECS}s between the search "
                f"feed and the delete-sold sweep")
            time.sleep(FB_POST_SWEEP_COOLDOWN_SECS)
            recheck_days = 0 if full_sweep else cfg.fb_defaults.sweep_recheck_days
            if full_sweep:
                log("--full-delete-sweep: re-checking every active FB listing regardless "
                    "of recent signs of life")
            delete_sold_listings(driver, cfg.output.csv, scrape_time,
                                 stale_after_days=recheck_days)
    finally:
        if keep_browser:
            # Leave Chrome up so the run can be inspected afterwards. Also drop
            # the atexit teardown, which would otherwise close it on exit.
            import atexit
            from fb_scraper import _quit_quietly
            try:
                atexit.unregister(_quit_quietly)
            except Exception:
                pass
            log("Browser left open (--keep-browser). NOTE: it holds fb_profile/, "
                "so the next run cannot start until you close this Chrome window.")
        else:
            try:
                driver.quit()
            except Exception:
                pass
    # Persist the run's end time ONCE so the next run's window starts here —
    # but only if this run actually observed the feed.
    if saw_any_card:
        save_fb_scrape_state({"last_fb_scrape_timestamp": scrape_time}, state_path)
    else:
        log("No Facebook query returned a single card — leaving the last-scrape "
            "timestamp untouched so the next run keeps the same lookback window")


def _filter_units_by_arg(units: List[SearchUnit], slug: str) -> List[SearchUnit]:
    """Resolve a ``--make-model`` arg against the profile's search units.

    Accepts ``make/model`` (matches a model-specific unit) or just ``make``
    (matches any unit of that make — make-only or make/model).
    """
    if "/" in slug:
        return [u for u in units if u.slug == slug]
    return [u for u in units if u.make == slug]


def main() -> None:
    """Scrape every search unit of the active profile, one at a time.

    Each AutoTrader search URL is assembled by ``build_at_search_url`` from the
    profile's ``autotrader.search`` block (year_min, price range, radius_km,
    postal_code, plus any ``extra_params``); pagination happens via ``&page=N``
    in ``get_listing_urls``, which reads the page count from ``__NEXT_DATA__``.
    Facebook then runs from ``facebook.queries``. The scatter HTML is
    regenerated at the end of every run and pushed to GitHub Pages unless
    ``--no-publish`` is passed.
    """
    args = _parse_args()
    cfg = load_config(args.config)

    global LOG_DIR
    LOG_DIR = cfg.output.log_dir

    log_path = setup_run_log()
    if log_path:
        log(f"Run log: {log_path}")
    log(f"Loaded profile: {cfg.profile_name} from {args.config}")

    # Forensics for the 2026-08-11 mystery kills: two runs died mid-sweep on an
    # awake machine with no user action. Log the PID so system logs can be
    # searched for it, and log any catchable termination signal. If the run
    # dies with no "RECEIVED SIGNAL" line in the log, the killer used SIGKILL.
    import signal as _signal
    log(f"PID {os.getpid()} (parent {os.getppid()})")

    def _log_termination(signum, frame):
        try:
            name = _signal.Signals(signum).name
        except ValueError:
            name = str(signum)
        log(f"RECEIVED SIGNAL {name} — terminating")
        sys.stdout.flush()
        sys.stderr.flush()
        raise SystemExit(128 + signum)

    for _sig in (_signal.SIGTERM, _signal.SIGINT, _signal.SIGHUP, _signal.SIGQUIT):
        _signal.signal(_sig, _log_termination)

    if not args.generate_html_only:
        scrape_time = datetime.now().isoformat()
        scrape_num = 1
        if os.path.exists(cfg.output.csv):
            try:
                existing_df = pd.read_csv(cfg.output.csv, dtype={"ad_id": str})
                if "scrape_number" in existing_df.columns:
                    scrape_num = int(existing_df["scrape_number"].max()) + 1
            except (pd.errors.EmptyDataError, KeyError, ValueError):
                pass

        if args.make_model:
            units = _filter_units_by_arg(cfg.search_units, args.make_model)
            if not units:
                valid = sorted({u.slug for u in cfg.search_units})
                log(f"--make-model {args.make_model!r} doesn't match any unit in "
                    f"{args.config}. Valid: {valid}")
                return
        else:
            units = list(cfg.search_units)

        # --limit overrides the per-source YAML budget for BOTH sources.
        at_limit = (args.limit if args.limit is not None
                    else cfg.limits.autotrader_max_new_listings)
        fb_limit = (args.limit if args.limit is not None
                    else cfg.limits.facebook_max_new_listings)
        if cfg.autotrader_enabled and args.source in ("autotrader", "all"):
            _scrape_autotrader(units, cfg, scrape_num, scrape_time,
                               max_new_listings=at_limit)
        if cfg.facebook_enabled and args.source in ("facebook", "all"):
            _scrape_facebook(cfg, scrape_num, scrape_time,
                             max_listings=fb_limit, days_override=args.days,
                             delete_sold=not args.no_delete_sold,
                             full_sweep=args.full_delete_sweep,
                             keep_browser=args.keep_browser)

        removed = collapse_cross_source_duplicates(cfg.output.csv)
        if removed:
            log(f"Collapsed {removed} cross-source/same-car duplicate(s)")

    if os.path.exists(cfg.output.csv):
        generate_scatter_html(cfg.output.csv, cfg.output.scatter_html, cfg=cfg)
        log("Scatter plot updated")
    else:
        log(f"CSV {cfg.output.csv} does not exist yet — skipping scatter plot generation")
        return

    if args.no_publish or not cfg.github_pages.enabled:
        return
    try:
        subprocess.run(["git", "add", cfg.output.scatter_html], check=True, capture_output=True)
        subprocess.run(
            ["git", "commit", "-m", f"Update {cfg.output.scatter_html} ({cfg.profile_name})"],
            check=True, capture_output=True, text=True,
        )
        subprocess.run(["git", "push"], check=True, capture_output=True, text=True)
        if cfg.html.public_url:
            log(f"Pushed to GitHub Pages: {cfg.html.public_url}")
        else:
            log(f"Pushed {cfg.output.scatter_html} to {cfg.github_pages.repo}")
    except Exception as e:
        log(f"GitHub push failed (non-fatal): {e}")


if __name__ == "__main__":
    main()
