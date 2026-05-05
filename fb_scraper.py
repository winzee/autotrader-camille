"""Facebook Marketplace scraper.

Mirrors the public shape of ``bmw_x3_scraper.py``:

* ``create_fb_driver()`` ≈ ``create_driver()``
* ``scrape_vehicle_facebook()`` ≈ ``scrape_vehicle()``
* ``extract_fb_details()`` ≈ ``extract_listing_details()``
* ``parse_fb_detail_dom()`` ≈ ``parse_next_data()`` (dict-walking stage)

Data flow:
  1. Navigate to the Marketplace search page with ``sortBy=creation_time_descend``.
  2. Parse the embedded Relay/Apollo JSON from the SSR ``<script>``.
  3. Card-level filter: category_id==Vehicles, has mileage subtitle, title
     matches target model.
  4. For each passing card, navigate to the detail page and extract fields
     from the rendered DOM (``[role="main"]`` innerText).
  5. Return populated ``VehicleListing`` objects with ``source="facebook"``.

Stop conditions while scrolling the search page (whichever hits first):
  * ``max_listings`` passing cards collected
  * ``daysSinceListed`` window exhausted (URL param; FB stops serving)
  * An already-known ``ad_id`` encountered (incremental scrape)
  * Scroll no longer increases page height
"""
from __future__ import annotations

import json
import os
import random
import re
import time
import urllib.parse
from dataclasses import asdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from bmw_x3_scraper import VehicleListing, deduplicate_listings, log


# ── Constants ─────────────────────────────────────────────────────────────

FB_VEHICLES_CATEGORY_ID = "807311116002614"
FB_PROFILE_DIR = "fb_profile"
FB_STATE_FILE = ".fb_scrape_state.json"
FB_CARD_TRACE_FILE = "fb_card_trace.log"

FB_LISTING_PAUSE_SECS = 5
FB_LISTING_PAUSE_JITTER = 2.0
FB_SCROLL_PAUSE_SECS = 3.0


# ── Card trace logging ────────────────────────────────────────────────────

def reset_card_trace(path: str = FB_CARD_TRACE_FILE) -> None:
    """Truncate the card-trace file at the start of a fresh scrape run."""
    with open(path, "w") as f:
        f.write(f"=== FB card trace — started {datetime.now().isoformat(timespec='seconds')} ===\n")


def trace_section(header: str, path: str = FB_CARD_TRACE_FILE) -> None:
    with open(path, "a") as f:
        f.write(f"\n--- {header} ---\n")


def trace_card(status: str, card: Dict[str, Any], reason: str = "",
               path: str = FB_CARD_TRACE_FILE) -> None:
    """Append one card line. Order: description | price | location | km | URL."""
    title = (card.get("title") or "?").strip().replace("\n", " ")
    price_raw = card.get("price_str")
    try:
        price = f"CA${int(float(price_raw)):,}" if price_raw else "?"
    except (TypeError, ValueError):
        price = f"CA${price_raw}"
    city = card.get("city") or ""
    state = card.get("state") or ""
    loc = ", ".join(x for x in (city, state) if x) or "?"
    km = card.get("mileage_subtitle") or "?"
    aid = str(card.get("id") or "")
    url = card.get("url") or (f"https://www.facebook.com/marketplace/item/{aid}/" if aid else "?")
    tag = f"{status}:{reason}" if reason else status
    ts = datetime.now().strftime("%H:%M:%S")
    with open(path, "a") as f:
        f.write(f"[{ts}] {tag:<22} | {title} | {price} | {loc} | {km} | {url}\n")

CHROME_BIN = "/Applications/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
)


# ── Driver ────────────────────────────────────────────────────────────────

def create_fb_driver(headless: bool = False,
                     user_data_dir: Optional[str] = FB_PROFILE_DIR) -> webdriver.Chrome:
    """Create a Chrome driver configured for Facebook Marketplace scraping.

    Uses a persistent ``user_data_dir`` so the first-run login sticks across
    subsequent scrapes. Headful by default — FB aggressively detects headless.
    """
    options = Options()
    if os.path.exists(CHROME_BIN):
        options.binary_location = CHROME_BIN
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1400,1000")
    options.add_argument(f"--user-agent={USER_AGENT}")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    if user_data_dir:
        abs_dir = os.path.abspath(user_data_dir)
        os.makedirs(abs_dir, exist_ok=True)
        options.add_argument(f"--user-data-dir={abs_dir}")
    service = Service(ChromeDriverManager(driver_version="130.0.6723.69").install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(60)
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
        )
    except Exception:
        pass
    return driver


# ── Search URL & card-level parsing ───────────────────────────────────────

def build_search_url(query: str, max_price: int = 15000, min_price: int = 6000,
                     days_since_listed: int = 30) -> str:
    """Build the Marketplace search URL (Montréal) for a free-text ``query``.

    Param choices — kept minimal because FB is picky:
    * ``exact=false`` — ``true`` returns zero hits even for obvious queries.
    * No ``itemCondition`` filter — folding it in also empties the results.
    The card-level filter later restricts to vehicles matching the target model.
    """
    q = urllib.parse.quote(query)
    return (
        f"https://www.facebook.com/marketplace/montreal/search"
        f"?minPrice={min_price}"
        f"&maxPrice={max_price}"
        f"&daysSinceListed={days_since_listed}"
        f"&sortBy=creation_time_descend"
        f"&query={q}"
        f"&exact=false"
    )


_APOLLO_LISTING_RX = re.compile(
    r'"listing":\{[^{}]*?"id":"(?P<id>\d+)"[\s\S]*?'
    r'"listing_price":\{[^}]*?"amount":"(?P<price>[^"]+)"[\s\S]*?'
    r'"location":\{"reverse_geocode":\{"city":"(?P<city>[^"]*)","state":"(?P<state>[^"]*)"'
    r'[\s\S]*?"is_live":(?P<live>true|false)'
    r',"is_pending":(?P<pending>true|false)'
    r',"is_sold":(?P<sold>true|false)'
    r'[\s\S]*?"marketplace_listing_category_id":"(?P<cat>\d+)"'
    r'[\s\S]*?"marketplace_listing_title":"(?P<title>[^"]+)"'
    r'[\s\S]*?"custom_sub_titles_with_rendering_flags":(?P<subs>\[[^\]]*\])'
    r'[\s\S]*?"marketplace_listing_seller":\{[^}]*?"name":"(?P<seller>[^"]*)"',
    re.DOTALL,
)


def _decode_jsonish(s: str) -> str:
    """Decode \\u-escapes in a JSON-ish substring extracted via regex."""
    try:
        return json.loads(f'"{s}"')
    except Exception:
        return s


def extract_apollo_listings(page_source: str) -> List[Dict[str, Any]]:
    """Extract the vehicle-card dicts from the SSR Apollo bootstrap blob."""
    results: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for m in _APOLLO_LISTING_RX.finditer(page_source):
        aid = m.group("id")
        if aid in seen:
            continue
        seen.add(aid)
        subs_raw = m.group("subs")
        mileage_sub = None
        try:
            arr = json.loads(subs_raw)
            if arr and isinstance(arr[0], dict):
                mileage_sub = arr[0].get("subtitle")
        except Exception:
            pass
        results.append({
            "id": aid,
            "price_str": m.group("price"),
            "category_id": m.group("cat"),
            "title": _decode_jsonish(m.group("title")),
            "city": _decode_jsonish(m.group("city")),
            "state": m.group("state"),
            "seller_name": _decode_jsonish(m.group("seller")),
            "mileage_subtitle": mileage_sub,
            "is_live": m.group("live") == "true",
            "is_pending": m.group("pending") == "true",
            "is_sold": m.group("sold") == "true",
            "url": f"https://www.facebook.com/marketplace/item/{aid}/",
        })
    return results


def _parse_mileage_km(subtitle: Optional[str]) -> Optional[int]:
    """Parse ``'237K km'`` or ``'148,000 km'`` into integer kilometres."""
    if not subtitle:
        return None
    s = subtitle.replace(",", "").replace(" ", "").strip().lower()
    m = re.match(r"(\d+(?:\.\d+)?)(k|m)?km", s)
    if not m:
        return None
    num = float(m.group(1))
    unit = m.group(2)
    if unit == "k":
        num *= 1000
    elif unit == "m":
        num *= 1_000_000
    return int(num)


def _parse_year_from_title(title: str) -> Optional[int]:
    m = re.search(r"\b(19[89]\d|20\d{2})\b", title)
    return int(m.group(1)) if m else None


def reject_reason(card: Dict[str, Any],
                  model_regex: "re.Pattern[str]",
                  year_range: Optional[Tuple[int, int]] = None,
                  allowed_provinces: Optional[Set[str]] = None,
                  min_price: int = 6000) -> Optional[str]:
    """Return ``None`` if card passes the filter, else a short reason tag."""
    if card.get("category_id") != FB_VEHICLES_CATEGORY_ID:
        return "not-vehicle"
    if card.get("is_sold") or card.get("is_pending"):
        return "sold-or-pending"
    if card.get("is_live") is False:
        return "not-live"
    if _parse_mileage_km(card.get("mileage_subtitle")) is None:
        return "no-mileage"
    if not model_regex.search(card.get("title") or ""):
        return "model-mismatch"
    if allowed_provinces and card.get("state") not in allowed_provinces:
        return f"province-{card.get('state') or 'none'}"
    try:
        price = float(card.get("price_str") or "0")
        if price < min_price:
            return f"below-min-price-{int(price)}"
    except (TypeError, ValueError):
        pass
    if year_range:
        y = _parse_year_from_title(card.get("title") or "")
        if y is None:
            return "no-year"
        if not (year_range[0] <= y <= year_range[1]):
            return f"outside-year-{y}"
    return None


def is_target_vehicle(card: Dict[str, Any],
                      model_regex: "re.Pattern[str]",
                      year_range: Optional[Tuple[int, int]] = None,
                      allowed_provinces: Optional[Set[str]] = None,
                      min_price: int = 6000) -> bool:
    return reject_reason(card, model_regex, year_range, allowed_provinces, min_price) is None


# ── Scrape state (last-run timestamp) ─────────────────────────────────────

def load_fb_scrape_state(path: str = FB_STATE_FILE) -> Dict[str, Any]:
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_fb_scrape_state(state: Dict[str, Any], path: str = FB_STATE_FILE) -> None:
    with open(path, "w") as f:
        json.dump(state, f, indent=2)


def compute_days_since_listed(last_timestamp: Optional[str],
                              buffer_days: int = 2, max_days: int = 30) -> int:
    """Compute daysSinceListed window with overlap buffer, capped."""
    if not last_timestamp:
        return max_days
    try:
        last = datetime.fromisoformat(last_timestamp)
        delta = (datetime.now() - last).days + buffer_days
        return max(1, min(max_days, delta))
    except Exception:
        return max_days


# ── Search page navigation + card collection ──────────────────────────────

def _maybe_login_wait(driver, target_url: str, max_wait_secs: int = 300) -> bool:
    """If a login form appears, poll until it disappears (user logs in manually).

    Returns True if we arrived (or are already) on a logged-in session that can
    see Marketplace. Returns False after ``max_wait_secs`` of no progress.
    """
    def _needs_login() -> bool:
        try:
            if "/login" in (driver.current_url or ""):
                return True
            return bool(driver.find_elements(By.CSS_SELECTOR, 'input[name="email"]'))
        except Exception:
            return False

    if not _needs_login():
        return True
    log("FB login required — log in manually in the browser window. "
        f"Waiting up to {max_wait_secs}s...")
    waited = 0
    poll = 5
    while waited < max_wait_secs:
        time.sleep(poll)
        waited += poll
        if not _needs_login():
            log(f"  Login detected after {waited}s. Reloading search page...")
            try:
                driver.get(target_url)
            except Exception:
                pass
            time.sleep(3)
            return True
    log(f"  Login did not complete within {max_wait_secs}s — aborting FB scrape.")
    return False


def get_fb_listing_cards(driver, query: str, days_since_listed: int, max_listings: int,
                         model_regex: "re.Pattern[str]", seen_ad_ids: Set[str],
                         year_range: Optional[Tuple[int, int]] = None,
                         max_price: int = 15000,
                         max_nonmatching: int = 400,
                         allowed_provinces: Optional[Set[str]] = None,
                         min_price: int = 6000) -> List[Dict[str, Any]]:
    """Scroll the search page collecting passing cards until a stop condition.

    Stop conditions (whichever hits first):
      * ``max_listings`` passing cards collected
      * ``max_nonmatching`` *rejected* cards observed (filtered out by
        ``reject_reason``) — hard cap protecting against FB padding the feed
      * Page height stops growing

    Already-known ad_ids are skipped (no detail fetch) but do NOT stop the
    scroll — FB's feed is non-deterministic, so a known ad can appear before
    fresh unseen ones further down.
    """
    url = build_search_url(query, max_price=max_price, min_price=min_price,
                           days_since_listed=days_since_listed)
    log(f"FB search URL: {url}")
    driver.get(url)
    time.sleep(4)
    if not _maybe_login_wait(driver, target_url=url):
        return []

    passing: List[Dict[str, Any]] = []
    passing_ids: Set[str] = set()
    rejected_ids: Set[str] = set()   # cards rejected by reject_reason
    seen_card_ids: Set[str] = set()  # every card we've observed on the page
    traced_ids: Set[str] = set()     # dedup for trace-file writes
    consecutive_no_height = 0
    last_height = 0

    for scroll_i in range(40):  # cap scroll attempts
        cards = extract_apollo_listings(driver.page_source)
        for card in cards:
            aid = card["id"]
            seen_card_ids.add(aid)
            if aid in seen_ad_ids:
                if aid not in traced_ids:
                    trace_card("KNOWN_SKIP", card)
                    traced_ids.add(aid)
                continue  # skip detail fetch, but keep scrolling
            if aid in passing_ids or aid in rejected_ids:
                continue
            reason = reject_reason(card, model_regex, year_range,
                                   allowed_provinces=allowed_provinces,
                                   min_price=min_price)
            if aid not in traced_ids:
                trace_card("KEEP" if reason is None else "FILTER", card, reason or "")
                traced_ids.add(aid)
            if reason is None:
                passing.append(card)
                passing_ids.add(aid)
                log(f"  + card #{len(passing)}: {card['title']} | {card['mileage_subtitle']} | {card['city']}")
                if len(passing) >= max_listings:
                    break
            else:
                rejected_ids.add(aid)
        if len(passing) >= max_listings:
            break

        if len(rejected_ids) >= max_nonmatching:
            log(f"  Saw {len(rejected_ids)} non-matching cards (cap={max_nonmatching}) — stopping "
                f"with {len(passing)} passing")
            break

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(FB_SCROLL_PAUSE_SECS + random.uniform(0, 1.5))
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            consecutive_no_height += 1
            if consecutive_no_height >= 2:
                log(f"  Scroll height unchanged — ending at {len(passing)} passing cards")
                break
        else:
            consecutive_no_height = 0
            last_height = new_height

    return passing[:max_listings]


# ── Detail page parsing ───────────────────────────────────────────────────

_DOM_INNERTEXT_JS = (
    "const m = document.querySelector('[role=\"main\"]');"
    "return m ? m.innerText : '';"
)


def parse_fb_detail_dom(driver) -> Dict[str, Any]:
    """Extract structured fields from a rendered FB detail page via the DOM."""
    text = driver.execute_script(_DOM_INNERTEXT_JS) or ""
    fields: Dict[str, Any] = {}

    raw_title = (driver.title or "").replace("Marketplace - ", "").replace(" | Facebook", "").strip()
    fields["title"] = re.sub(r"^\(\d+\)\s*", "", raw_title) or None

    m = re.search(r"CA\$([\d,]+)", text)
    if m:
        fields["price_cad"] = int(m.group(1).replace(",", ""))

    m = re.search(r"Listed\s+[^\n]+?\s+in\s+([^,\n]+),\s*([A-Z]{2})", text)
    if m:
        fields["city"] = m.group(1).strip()
        fields["province"] = m.group(2).strip()

    m = re.search(r"Driven\s+([\d,]+)\s*km", text)
    if m:
        fields["mileage_km"] = int(m.group(1).replace(",", ""))

    m = re.search(r"(Automatic|Manual|CVT)\s+transmission", text, re.IGNORECASE)
    if m:
        fields["transmission"] = m.group(1).lower()

    m = re.search(r"Exterior color:\s*([^\n·]+?)(?:\s*·|\n)", text)
    if m:
        fields["body_color"] = m.group(1).strip()

    m = re.search(r"Interior color:\s*([^\n]+)", text)
    if m:
        fields["upholstery_color"] = m.group(1).strip()

    m = re.search(r"Fuel type:\s*([^\n]+)", text)
    if m:
        fields["fuel_type"] = m.group(1).strip()

    m = re.search(r"(Excellent|Good|Fair|Poor)\s+condition", text)
    if m:
        fields["status"] = m.group(1).lower()

    m = re.search(r"Seller's description\s*\n(.*?)(?:\nLocation is approximate|\nSeller information|\nSponsored|\Z)",
                  text, re.DOTALL)
    if m:
        desc = m.group(1).strip()
        fields["description"] = re.sub(r"\.\.\.\s*See more$", "", desc).strip() or None

    try:
        img_src = driver.execute_script(
            "const el = document.querySelector('[role=\"main\"] img'); return el ? el.src : null;"
        )
        if img_src:
            fields["image_url"] = img_src
    except Exception:
        pass

    # Seller name — appears on the "Seller information" block
    m = re.search(r"Seller information\s*\nSeller details\s*\n([^\n]+)", text)
    if m:
        fields["seller_name"] = m.group(1).strip()

    return fields


def extract_fb_details(driver, listing_id: str) -> VehicleListing:
    """Navigate to a listing and extract a populated ``VehicleListing``."""
    url = f"https://www.facebook.com/marketplace/item/{listing_id}/"
    listing = VehicleListing(url=url, source="facebook", ad_id=listing_id)
    try:
        driver.get(url)
    except Exception as exc:
        log(f"  Failed to navigate to {url}: {exc}")
        return listing

    try:
        WebDriverWait(driver, 20).until(
            lambda d: len(d.execute_script(_DOM_INNERTEXT_JS) or "") > 200
        )
    except Exception:
        pass
    time.sleep(1.5)  # small settle for async-loaded sections

    fields = parse_fb_detail_dom(driver)
    listing.title = fields.get("title")
    listing.price_cad = fields.get("price_cad")
    listing.city = fields.get("city")
    listing.province = fields.get("province")
    listing.mileage_km = fields.get("mileage_km")
    listing.transmission = fields.get("transmission")
    listing.body_color = fields.get("body_color")
    listing.upholstery_color = fields.get("upholstery_color")
    listing.fuel_type = fields.get("fuel_type")
    listing.status = fields.get("status")
    listing.description = fields.get("description")
    listing.image_url = fields.get("image_url")
    listing.seller_name = fields.get("seller_name")

    if listing.title:
        listing.year = _parse_year_from_title(listing.title)

    return listing


# ── End-to-end orchestrator (mirrors scrape_vehicle) ──────────────────────

def scrape_vehicle_facebook(driver, query: str,
                            model_regex_src: str,
                            model_canonical: Optional[str],
                            output_file: str, scrape_num: int, scrape_time: str,
                            make: Optional[str] = None,
                            max_listings: int = 10,
                            year_range: Optional[Tuple[int, int]] = None,
                            max_price: int = 15000,
                            allowed_provinces: Optional[Set[str]] = None,
                            min_price: int = 6000,
                            session_last_scrape_timestamp: Optional[str] = None,
                            days_override: Optional[int] = None) -> None:
    """Scrape FB for one query and merge into ``output_file`` CSV.

    Mirrors ``scrape_vehicle()`` for AutoTrader: loads existing CSV, collects
    fresh listings, dedups, merges with existing rows (marks vanished FB
    rows for this query as deleted), and writes back.

    ``model_canonical`` and ``make`` may be ``None`` for generic searches
    (e.g. body-type or drivetrain queries that don't pin a make/model). The
    CSV's ``make``/``model`` columns are left blank for those rows.
    """
    log_id = model_canonical or query
    # Load existing CSV
    existing_df = None
    if os.path.exists(output_file):
        try:
            existing_df = pd.read_csv(output_file)
        except pd.errors.EmptyDataError:
            existing_df = None

    if existing_df is not None and "source" not in existing_df.columns:
        existing_df["source"] = "autotrader"

    # Build set of already-seen FB ad_ids for THIS model (for incremental stop).
    # Scoped to model_canonical so an unrelated FB ad bleeding into another model's
    # search feed doesn't trigger a premature hit_known stop.
    seen_ad_ids: Set[str] = set()
    if existing_df is not None and "source" in existing_df.columns and "ad_id" in existing_df.columns:
        mask = ((existing_df["source"] == "facebook")
                & (existing_df["model"] == model_canonical)
                & existing_df["ad_id"].notna())
        seen_ad_ids = set(existing_df.loc[mask, "ad_id"].astype(str))

    # Compute daysSinceListed
    if days_override is not None:
        days = max(1, min(365, int(days_override)))
    else:
        if session_last_scrape_timestamp is not None:
            last_ts = session_last_scrape_timestamp
        else:
            last_ts = load_fb_scrape_state().get("last_fb_scrape_timestamp")
        days = compute_days_since_listed(last_ts)
    log(f"FB scrape: {log_id} (query={query!r}) max={max_listings} days={days} "
        f"seen_ad_ids={len(seen_ad_ids)}")
    trace_section(f"{log_id} query={query!r} days={days}")

    # Collect cards
    model_regex = re.compile(model_regex_src, re.IGNORECASE)
    cards = get_fb_listing_cards(
        driver, query=query, days_since_listed=days,
        max_listings=max_listings, model_regex=model_regex,
        seen_ad_ids=seen_ad_ids, year_range=year_range, max_price=max_price,
        allowed_provinces=allowed_provinces or {"QC"}, min_price=min_price,
    )
    log(f"Collected {len(cards)} cards passing filter; fetching details...")

    # Track ad_ids seen this run for deletion detection
    scraped_ad_ids: Set[str] = {c["id"] for c in cards}

    listings: List[VehicleListing] = []
    for idx, card in enumerate(cards, start=1):
        log(f"  [{idx}/{len(cards)}] {card['id']}: {card['title']}")
        try:
            listing = extract_fb_details(driver, card["id"])
        except Exception as exc:
            log(f"    extract failed: {exc}")
            trace_card("DROP", card, f"detail-fetch-error:{type(exc).__name__}")
            if idx < len(cards):
                time.sleep(FB_LISTING_PAUSE_SECS + random.uniform(0, FB_LISTING_PAUSE_JITTER))
            continue

        # Validation: drop junk with too many nulls
        key_fields = [listing.mileage_km, listing.transmission, listing.year]
        if sum(v is None for v in key_fields) >= 2:
            log(f"    dropped (too many null key fields) — {listing.title or card['title']}")
            trace_card("DROP", card, "detail-null-key-fields")
            if idx < len(cards):
                time.sleep(FB_LISTING_PAUSE_SECS + random.uniform(0, FB_LISTING_PAUSE_JITTER))
            continue

        # Fill-in from card if detail extraction missed anything
        if model_canonical:
            listing.model = model_canonical
        if make:
            listing.make = make.capitalize()
        if listing.seller_name is None:
            listing.seller_name = card.get("seller_name")
        if listing.price_cad is None and card.get("price_str"):
            try:
                listing.price_cad = int(float(card["price_str"]))
            except (TypeError, ValueError):
                pass
        if not listing.city:
            listing.city = card.get("city")
        if not listing.province:
            listing.province = card.get("state")

        listing.scrape_number = scrape_num
        listing.scrape_timestamp = scrape_time
        listing.last_scrape_timestamp = scrape_time

        listings.append(listing)

        if idx < len(cards):
            time.sleep(FB_LISTING_PAUSE_SECS + random.uniform(0, FB_LISTING_PAUSE_JITTER))

    unique_listings = deduplicate_listings(listings)
    log(f"Keeping {len(unique_listings)} unique new FB listings after dedup")

    new_df = pd.DataFrame([asdict(l) for l in unique_listings])

    # Merge with existing_df: update last_scrape_timestamp / is_deleted for this
    # source+model scope.
    if existing_df is not None and not existing_df.empty:
        fb_mask = (existing_df["source"] == "facebook") & (existing_df["model"] == model_canonical)

        still_present = fb_mask & existing_df["ad_id"].astype(str).isin(scraped_ad_ids)
        existing_df.loc[still_present, "last_scrape_timestamp"] = scrape_time

        if scraped_ad_ids:
            disappeared = (fb_mask
                           & ~existing_df["ad_id"].astype(str).isin(scraped_ad_ids)
                           & existing_df["is_deleted"].isna())
            if disappeared.any():
                log(f"Marking {disappeared.sum()} FB listings as deleted")
                existing_df.loc[disappeared, "is_deleted"] = scrape_time
        else:
            log("Skipping FB deletion marking — no ad_ids scraped")

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
    log(f"FB data merged into {output_file}")
    # NOTE: caller is responsible for save_fb_scrape_state() after all vehicles
    # finish — writing it per-vehicle narrows daysSinceListed for subsequent
    # vehicles in the same run.
