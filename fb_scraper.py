"""Facebook Marketplace scraper.

Mirrors the public shape of ``bmw_x3_scraper.py``:

* ``create_fb_driver()`` ≈ ``create_driver()``
* ``scrape_vehicle_facebook()`` ≈ ``scrape_vehicle()``
* ``extract_fb_details()`` ≈ ``extract_listing_details()``
* ``parse_fb_detail_dom()`` ≈ ``parse_next_data()`` (dict-walking stage)

Data flow:
  1. Navigate to the Marketplace search page with ``sortBy=creation_time_descend``.
  2. Read the cards from the live DOM, and their mileage/category from the JSON
     side-channel (SSR blob once, then the teed GraphQL responses per scroll) —
     neither field has been rendered into the card markup since 2026-08-06.
  3. Card-level filter: category, title, year, price and province.
  4. For each passing card, navigate to the detail page and extract fields
     from the rendered DOM (``[role="main"]`` innerText).
  5. Return populated ``VehicleListing`` objects with ``source="facebook"``.

Stop conditions while scrolling the search page (whichever hits first) — each
one logs a ``STOP_REASON`` tag:
  * ``max-listings`` — the caller's target was reached
  * ``fb-verdict-end`` — Facebook's own feed ``has_next_page`` came back false
  * ``feed-quiet`` — the whole patience ladder elapsed with no unseen card
  * ``consecutive-rejects`` — the junk valve tripped
  * ``scroll-budget`` — the derived scroll budget ran out (runaway guard)

The ``daysSinceListed`` URL param bounds the window before any of this: a
listing posted outside it cannot appear, which is why absence from a scrape is
never read as a disappearance (see ``scrape_vehicle_facebook``).
"""
from __future__ import annotations

import atexit
import json
import math
import os
import random
import re
import subprocess
import time
import urllib.parse
import urllib.request
from dataclasses import asdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


def _driver_get_with_retry(driver, url: str, *,
                           attempts: int = 3,
                           wait_between: float = 8.0) -> bool:
    """``driver.get(url)`` with retry on Selenium navigation failures.

    FB's renderer occasionally hangs on the initial page load and Selenium
    raises ``TimeoutException: Timed out receiving message from renderer``.
    A clean reload almost always recovers it. We call ``window.stop()`` first
    so the next ``get`` doesn't queue behind the still-loading request.
    Returns True on success; False if every attempt failed (caller should
    bail out gracefully rather than crash the whole scrape).
    """
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            driver.get(url)
            return True
        except WebDriverException as exc:
            last_exc = exc
            log(f"  driver.get failed (attempt {attempt}/{attempts}): "
                f"{type(exc).__name__}: {str(exc).splitlines()[0][:120]}")
            try:
                driver.execute_script("window.stop();")
            except Exception:
                pass
            if attempt < attempts:
                time.sleep(wait_between)
    log(f"  driver.get giving up after {attempts} attempts: {last_exc}")
    return False
from webdriver_manager.chrome import ChromeDriverManager

from bmw_x3_scraper import VehicleListing, deduplicate_listings, log


# ── Constants ─────────────────────────────────────────────────────────────

FB_VEHICLES_CATEGORY_ID = "807311116002614"
FB_PROFILE_DIR = "fb_profile"
FB_DEBUG_PORT = 9222
FB_STATE_FILE = ".fb_scrape_state.json"
FB_CARD_TRACE_FILE = "fb_card_trace.log"

# ── Anti-runaway safety valves (internal; deliberately NOT in the YAML) ──
# These exist so a degenerate feed cannot spin forever. They are not tuning
# knobs: the user's intent lives in `limits.facebook.max_new_listings`. Keeping
# them out of config is the whole point — a hardcoded 40-scroll cap used to
# silently override an explicit `--limit 1000`, capping it near 435.
FB_MAX_CONSECUTIVE_REJECTS = 150   # rejects back-to-back with no keep
# "Feed exhausted" is a question about TIME, not about iteration count. Counting
# fast scrolls cannot tell "the feed ended" from "FB has not answered yet": four
# ~4s tours give ~16s of patience, which ended a run at 24 cards on a feed that
# held 360+ when measured the next morning (2026-08-10 23:34 vs 08-11 10:09).
# WHY the batch was slow that night is NOT established — the delete-sold pass had
# just run, but at one navigation per 13s it is hardly hammering, so throttling is
# a guess, not a finding. The fix does not depend on the answer: more patience
# works whatever the cause. Paying ~92s once at the end of a run is nothing;
# truncating a run to 6% of the feed is not.
# The ladder of waits after each dry scroll. The run ends only once the LAST
# rung — a full 5 minutes — has also come back empty: at that point the feed is
# not slow, it is finished. Total quiet before giving up: ~6.5 minutes, paid once
# at the very end of a run.
FB_NO_NEW_BACKOFF_SECS = (4.0, 8.0, 15.0, 25.0, 40.0, 300.0)
FB_MAX_SCROLLS_ABSOLUTE = 500      # hard ceiling, runaway guard only
FB_SCROLL_BUDGET_BASE = 20         # floor, absorbs a slow-loading first batch
FB_SCROLL_KEEPS_PER_SCROLL = 4.0   # deliberately pessimistic: ~11 measured

# Deletion-marking health guard (mirrors MIN_URLS_FOR_DELETION on the AT path).
FB_MIN_IDS_FOR_DELETION = 5      # ids seen before any disappearance is believed
FB_MIN_ACTIVE_FOR_DELETION = 3   # below this many active rows, skip the ratio test
FB_MIN_SEEN_RATIO = 0.5          # share of active rows that must be re-found

# Precautionary cooldown after the delete-sold pass, NOT a fix for a proven
# cause. The night the feed stalled, that pass had just run — but at 13.3s per
# listing it is a gentle rate, so "FB throttled us" remains unverified. Cheap
# insurance; the actual fix is the patience above.
FB_POST_SWEEP_COOLDOWN_SECS = 60

# Measured end-to-end cost per listing in a real sweep (676 listings, 150 min on
# 2026-08-10): the 5-7s pause plus navigation and the poll-to-verdict loop.
FB_SWEEP_SECS_PER_LISTING = 13.3

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
# No --user-agent override: Chrome 130's own UA is already the string we used to
# spoof, and a hand-set UA desyncs from the Sec-CH-UA client hints. See
# create_fb_driver() for why we keep the browser's natural fingerprint intact.


# ── Driver ────────────────────────────────────────────────────────────────

def _quit_quietly(driver, proc: "Optional[subprocess.Popen]" = None) -> None:
    """Best-effort teardown that never raises — used for atexit cleanup.

    Quits the Selenium session, then makes sure the Chrome we launched is really
    gone. A graceful exit is what flushes cookies to disk, so a leaked process
    means the next run starts logged out.
    """
    try:
        driver.quit()
    except Exception:
        pass
    if proc is not None and proc.poll() is None:
        try:
            proc.terminate()
            proc.wait(timeout=15)
        except Exception:
            pass


def _debug_port_alive(port: int, timeout: float = 1.0) -> bool:
    """True when a Chrome DevTools endpoint answers on ``port``."""
    try:
        with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/json/version", timeout=timeout):
            return True
    except Exception:
        return False


def _launch_real_chrome(user_data_dir: str, port: int,
                        headless: bool) -> Optional[subprocess.Popen]:
    """Start Chrome for Testing as an ordinary browser exposing a CDP port.

    Returns the process handle, or None when a suitable Chrome is already
    listening (we then attach without owning it).
    """
    if _debug_port_alive(port):
        log(f"  Attaching to Chrome already listening on port {port}")
        # We did not start this browser, so we cannot know it carries the
        # anti-throttling flags below. A Chrome left over from an older run (or
        # from --keep-browser before those flags existed) will still pause its
        # timers when the window is occluded, which stalls Marketplace's
        # infinite scroll and looks exactly like an exhausted feed.
        log("  WARNING: attached to a browser this run did not launch — if it "
            "was started without --disable-backgrounding-occluded-windows the "
            "feed can stall behind another window. Close Chrome between runs "
            "(especially after --keep-browser) to get a clean launch.")
        return None
    if not os.path.exists(CHROME_BIN):
        raise RuntimeError(f"Chrome for Testing not found at {CHROME_BIN}")
    proc = subprocess.Popen(
        [
            CHROME_BIN,
            f"--remote-debugging-port={port}",
            f"--user-data-dir={user_data_dir}",
            "--no-first-run",
            "--no-default-browser-check",
            "--window-size=1400,1000",
            # Chrome throttles pages in background / occluded windows: timers drop
            # to ~1 tick per minute, requestAnimationFrame stops, and
            # IntersectionObserver callbacks stop firing. Marketplace's infinite
            # scroll depends on exactly those to request the next batch — so a
            # window sitting behind the terminal makes scrollTo() do nothing and
            # the feed look exhausted. This is a BROWSER behaviour, not Facebook
            # detecting automation.
            "--disable-background-timer-throttling",
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
        ] + (["--headless=new"] if headless else []),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(60):
        if _debug_port_alive(port):
            log(f"  Chrome ready on debugging port {port}")
            return proc
        time.sleep(0.5)
    try:
        proc.terminate()
    except Exception:
        pass
    raise RuntimeError(
        f"Chrome never opened debugging port {port}. Most likely another Chrome "
        f"is already using the profile at {user_data_dir} — close it and retry."
    )


def create_fb_driver(headless: bool = False,
                     user_data_dir: Optional[str] = FB_PROFILE_DIR,
                     debug_port: int = FB_DEBUG_PORT) -> webdriver.Chrome:
    """Attach Selenium to a normally-launched Chrome for Marketplace scraping.

    We deliberately do NOT let chromedriver spawn the browser. That path sets
    ``--enable-automation`` and ``navigator.webdriver = true``, and runs an
    ad-hoc re-signed binary clone — Facebook reads the result as a new,
    untrusted device and re-challenges with 2FA on every run, even when the
    profile's ``c_user``/``xs`` cookies are perfectly valid (observed
    2026-08-06: a hand-launched Chrome loaded Marketplace fine, the
    chromedriver-launched one hit ``/two_step_verification`` within a minute
    and FB then deleted the session cookies server-side).

    Launching Chrome the ordinary way and attaching over CDP keeps the exact
    fingerprint FB already approved: no automation switches, and
    ``navigator.webdriver`` is naturally false — so no masking hack is needed.

    Headful by default — FB also detects headless.
    """
    abs_dir = os.path.abspath(user_data_dir or FB_PROFILE_DIR)
    os.makedirs(abs_dir, exist_ok=True)
    proc = _launch_real_chrome(abs_dir, debug_port, headless)

    options = Options()
    options.add_experimental_option("debuggerAddress", f"127.0.0.1:{debug_port}")
    service = Service(ChromeDriverManager(driver_version="130.0.6723.69").install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(60)
    # Tee GraphQL responses so harvest_card_json() can recover the mileage and
    # category FB no longer renders. Must be registered before any navigation.
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument", {"source": _FB_GRAPHQL_HOOK_JS},
        )
    except Exception as exc:
        log(f"  WARNING: GraphQL capture hook not installed ({exc}); "
            f"card-level mileage/category filtering will be skipped.")
    # Attached mode: chromedriver owns no browser process, so a forgotten quit
    # leaves Chrome running and cookies unflushed. Callers should still quit in a
    # finally; this atexit guarantees teardown on normal exit, unhandled
    # exceptions, and Ctrl-C. (It can't run on kill -9.)
    atexit.register(_quit_quietly, driver, proc)
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


_DOM_CARDS_JS = """
const seen = new Set();
const out = [];
const anchors = document.querySelectorAll('a[href*="/marketplace/item/"]');
for (const a of anchors) {
  const href = a.getAttribute('href') || '';
  const m = href.match(/\\/marketplace\\/item\\/(\\d+)/);
  if (!m) continue;
  const id = m[1];
  if (seen.has(id)) continue;
  const text = (a.innerText || '').trim();
  if (!text) continue;
  seen.add(id);
  out.push({id, text});
}
return out;
"""

_KNOWN_MAKES = (
    "Toyota", "Honda", "Subaru", "Mazda", "Nissan", "Mitsubishi", "Suzuki",
    "Lexus", "Infiniti", "Acura", "Hyundai", "Kia", "Genesis",
    "Ford", "Chevrolet", "GMC", "Cadillac", "Buick", "Lincoln",
    "Dodge", "Chrysler", "Jeep", "Ram",
    "BMW", "Mercedes-Benz", "Mercedes", "Audi", "Volkswagen", "Volvo",
    "Land Rover", "Range Rover", "Porsche", "Tesla", "Mini", "Fiat", "Smart",
)
_MAKE_MATCH_RX = re.compile(
    r"\b(" + "|".join(re.escape(m) for m in _KNOWN_MAKES) + r")\b",
    re.IGNORECASE,
)


def make_from_title(title: Optional[str]) -> Optional[str]:
    """Best-effort make extraction from a listing title.

    Returns the canonical make (e.g. "Hyundai") with the original casing from
    ``_KNOWN_MAKES`` so downstream filters that compare against
    ``search_units`` makes match. Returns None when no known make is present.
    """
    if not title:
        return None
    m = _MAKE_MATCH_RX.search(title)
    if not m:
        return None
    matched = m.group(1).lower()
    for canonical in _KNOWN_MAKES:
        if canonical.lower() == matched:
            return canonical
    return None


# Multi-word model special cases: a single "first word after make" doesn't
# uniquely name the car (e.g. Hyundai "Santa" → must be "Santa Fe"). The
# "grand"/"town" entries come from real rows that landed in the CSV as a bare
# "Grand" or "Town": Dodge Grand Caravan, Jeep Grand Cherokee, Suzuki Grand
# Vitara, Chrysler Town & Country.
_MULTI_WORD_MODEL_HEADS = {
    ("hyundai", "santa"): "Santa Fe",
    ("toyota", "land"): "Land Cruiser",
    ("mazda", "cx"): "CX",  # CX-5/CX-9 etc — preserve hyphenated tail below
    ("nissan", "x"): "X-Trail",
    ("dodge", "grand"): "Grand Caravan",
    ("jeep", "grand"): "Grand Cherokee",
    ("suzuki", "grand"): "Grand Vitara",
    ("chrysler", "town"): "Town & Country",
}

# Words that never name a model on their own, whatever the make follows them —
# so an unlisted "<make> Grand <something>" still yields both words instead of a
# bare "Grand".
_NEVER_ALONE_MODEL_HEADS = {"grand"}

# Placeholder models Facebook's own form offers when a seller picks no model.
# They are values, not information: storing them makes the scatter draw a
# "Autre" series.
_PLACEHOLDER_MODELS = {"autre", "other", "unknown", "n/a", "na"}

def model_from_title(title: Optional[str], make: Optional[str]) -> Optional[str]:
    """Best-effort model extraction. Looks for the make, then takes the next
    word(s) as the model. Used as a fallback when the FB query has no
    ``model_canonical`` (Émile's generic ``awd`` search).

    Returns None rather than a junk value when the title carries no real model:
    a placeholder ("Autre"), a single letter, or nothing after the make.
    """
    if not title or not make:
        return None
    # FB serves some titles with '+' where a space belongs ("Toyota+RAV4",
    # "Subaru+Impreza") — the model sits behind it either way.
    title = title.replace("+", " ")
    pattern = re.compile(
        rf"\b{re.escape(make)}\s+([A-Za-z][A-Za-z0-9-]*(?:\s+[A-Za-z0-9][A-Za-z0-9-]*)?)",
        re.IGNORECASE,
    )
    m = pattern.search(title)
    if not m:
        return None
    parts = m.group(1).strip().split()
    if not parts:
        return None

    # "2013 Mazda MAZDA MAZDA3", "2008 Toyota Toyota+RAV4" — sellers repeat the
    # make. Step over the repeat so the model is "Mazda3", not "Mazda" again.
    #
    # Only the make we matched on is skipped, NOT any known make: "Ram" and
    # "Genesis" are makes AND models, so skipping those turned "Dodge Ram 1500"
    # into "1500" and "Hyundai Genesis Coupe" into "Coupe" — two new wrong
    # values to fix one row whose title says both Suzuki and Toyota.
    if parts[0].lower() == make.lower():
        if len(parts) < 2:
            return None
        if parts[1].isdigit():
            # "Mazda 3" and "Mazda3" are the same car; the CSV spells it closed
            # up, as Mazda does.
            return f"{parts[0].capitalize()}{parts[1]}"
        parts = parts[1:]

    head = parts[0].lower()
    multi = _MULTI_WORD_MODEL_HEADS.get((make.lower(), head))
    if multi:
        return multi
    if head in _NEVER_ALONE_MODEL_HEADS and len(parts) > 1:
        return f"{parts[0].capitalize()} {parts[1].capitalize()}"
    if head in _PLACEHOLDER_MODELS or len(head) < 2:
        return None
    return parts[0].capitalize()


_PRICE_LINE_RX = re.compile(
    r"^\s*(?:CA|C|US)?\$\s*([\d,]+(?:\.\d+)?)\s*$",
    re.IGNORECASE,
)
_MILEAGE_LINE_RX = re.compile(
    r"^\s*[\d,]+(?:\.\d+)?\s*[KM]?\s*km\s*$",
    re.IGNORECASE,
)
_LOCATION_LINE_RX = re.compile(r"^\s*(.+?),\s*([A-Z]{2})\s*$")
_YEAR_IN_TITLE_RX = re.compile(r"\b(19[89]\d|20\d{2})\b")

# Motorcycles, scooters and powersports sit in FB's *same* Vehicles category as
# cars (807311116002614) and carry mileage and a year, so nothing structural
# separates them — verified 2026-08-09: vehicle_specifications is all-null for a
# Ruckus, a Can-Am Spyder and a car alike, and no vehicle_type field exists.
# A title filter is the only lever. Every term below is word-anchored. Bare
# keywords are NOT safe here — "sport" hits Kia Sportage, "cross" hits Subaru
# Crosstrek, "quad" hits a Dodge Ram Quad Cab, and "MT45" on a Freightliner van
# hit an unhyphenated Yamaha "MT" branch during this rewrite. Makes that also
# sell cars (Honda, Suzuki) are matched by MODEL name, never by make; makes with
# no car namesake in this market (Yamaha, Triumph, CFMOTO, Royal Enfield) are
# safe to match bare, which is what catches the model-of-the-month bikes.
#
# Validated 2026-08-11 against all 2351 titles in emile_suv_listings.csv +
# used_suv_listings.csv: 35 rows match, every one a genuine motorcycle, scooter
# or powersport, and zero cars. The previous version let 15 live motorcycles
# through (Triumph Street Triple, Suzuki GSX-*/SV650/Boulevard/Burgman/RV200,
# Honda Rebel and NC750S among them).
#
# Every model-code branch below ends in \w* on purpose. The group closes with
# \b, so a branch that matches only a PREFIX of a longer token fails the
# boundary and the whole term silently does nothing: "gsx-?[rs]" matched
# "GSX-S" inside "GSX-S1000F" and then died on the "1" that followed. Same trap
# defeated crf/fz/sv/dr against CRF250L, FZ6R, SV650S and DR650SE. The \w*
# eats the rest of the token so the boundary lands where a boundary exists.
_FB_NON_CAR_TITLE_RX = re.compile(r"""(?xi)
 \b(?:
     ruckus | grom | cbr\w* | nc\d{3}\w* | cb\d{3}\w*
   | crf\w* | africa\s*twin | gold\s*wing | goldwing
   | ninja | vulcan | yzf\w* | fz\d\w* | mt-\d{2}\w*
   | gsx-?\s?[rs8]\w* | katana | hayabusa | v-?strom
   | boulevard | burgman | sv\d{3}\w* | rv\d{3}\w* | m109r? | dr\d{3}\w* | intruder
   | sportster | softail | road\s*king | street\s*glide | fat\s*boy
   | harley(?:-?davidson)? | ducati | kawasaki | ktm | vespa | piaggio
   | aprilia | husqvarna | triumph | yamaha | cfmoto | benelli | moto\s*guzzi
   | royal[\s+]*enfield | enfield | victory\s*motorcycle | bonneville
   | can-?am | sea-?doo | ski-?doo | jet-?ski | motoneige | motocross
   | motocyclette | motorcycle | moto\s*cross | scooter | cyclomoteur
   | vtt | atv | utv | side-?by-?side | dirt\s*bike | pit\s*bike | snowmobile
   | golf\s*cart | voiturette
 )\b
""")

# Tokens that ALSO name a car, so they only mean "motorcycle" in the right
# company: "Rebel" is a Ram pickup trim, "Shadow" was a Dodge, and a bare "GSX"
# is the AWD Mitsubishi Eclipse — exactly the kind of car Émile's AWD-only
# search is looking for. "Victory" is too generic a word to match bare.
_FB_NON_CAR_SCOPED_RX = re.compile(r"""(?xi)
   \bhonda\b [^\n]{0,30}? \b(?:rebel|shadow)\b
 | \b(?:rebel|shadow)\b [^\n]{0,30}? \bhonda\b
 | \brebel\s*\d{3}\b
 | \bsuzuki\b [^\n]{0,30}? \bgsx\b
 | \bvictory\b [^\n]{0,30}?
   \b(?:cross\s*country|vegas|hammer|kingpin|vision|magnum|octane)\b
""")


def title_is_non_car(title: Optional[str]) -> bool:
    """True when the title names a motorcycle, scooter or other powersport.

    CALL THIS, never ``_FB_NON_CAR_TITLE_RX`` on its own. Two patterns are
    needed because the second one's tokens are real car names without a make to
    scope them (see ``_FB_NON_CAR_SCOPED_RX``), and only this helper consults
    both. Measured 2026-08-11: the bare regex alone misses "2020 Honda Rebel"
    and "2013 Victory CROSS COUNTRY TOUR", which then chart as cars.

    Safe to call straight off a DataFrame column. A missing title reads back as
    a float NaN and **NaN is truthy**, so the obvious ``title or ""`` hands a
    float to re.search and raises TypeError — the same trap that printed "nan"
    titles in the delete-sold sweep. The isinstance check is what makes
    .fillna("") unnecessary at the call site, so do not "simplify" it away.
    """
    text = title if isinstance(title, str) else ""
    return bool(_FB_NON_CAR_TITLE_RX.search(text)
                or _FB_NON_CAR_SCOPED_RX.search(text))


def _ad_id_str(value) -> str:
    """Normalise a CSV ad_id back to plain digits.

    pandas reads the column as float64, so ``astype(str)`` yields
    '1588888658875172.0' — and beyond 2**53 it degrades to '3.56e+16' with the
    trailing digits genuinely lost. Neither form ever matches the string ids
    scraped from cards, which is why ``seen_ad_ids`` was always empty and every
    run re-fetched the same listings.
    """
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    if text.isdigit():
        # Already exact. Do NOT round-trip through float: ids above 2**53 lose
        # their last digits, so normalising a clean 17-digit id would corrupt the
        # very values this helper exists to protect (37359594907019423 came back
        # as ...424, and the sweep then opened a listing that does not exist).
        return text
    try:
        return str(int(float(text)))
    except (TypeError, ValueError):
        return text


def _parse_dom_card_text(aid: str, text: str) -> Optional[Dict[str, Any]]:
    """Parse one card's innerText (FB Marketplace search-result anchor) into a
    card dict. Returns None if the text doesn't look like a vehicle card (let
    ``reject_reason`` filter the edge cases — we only bail when there's
    literally nothing useful)."""
    lines = [s.strip() for s in text.split("\n") if s.strip()]
    if not lines:
        return None

    price_str: Optional[str] = None
    mileage_sub: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    title: Optional[str] = None
    is_sold = False
    is_pending = False

    for ln in lines:
        low = ln.lower()
        if "sold" == low or low.startswith("sold "):
            is_sold = True
        if "pending" in low and len(low) < 30:
            is_pending = True
        if price_str is None:
            mp = _PRICE_LINE_RX.match(ln)
            if mp:
                price_str = mp.group(1).replace(",", "")
                continue
        if mileage_sub is None and _MILEAGE_LINE_RX.match(ln):
            mileage_sub = ln
            continue
        if state is None:
            mloc = _LOCATION_LINE_RX.match(ln)
            if mloc:
                city = mloc.group(1).strip()
                state = mloc.group(2)
                continue
        if title is None and _YEAR_IN_TITLE_RX.search(ln):
            title = ln

    if title is None:
        for ln in lines:
            if (_PRICE_LINE_RX.match(ln) or _MILEAGE_LINE_RX.match(ln)
                    or _LOCATION_LINE_RX.match(ln)):
                continue
            title = ln
            break

    return {
        "id": aid,
        "price_str": price_str,
        # Neither category nor mileage is in the rendered card text. Both are
        # filled from the JSON side-channel by get_fb_listing_cards(); None here
        # means "unknown", which reject_reason() treats as "don't judge".
        "category_id": None,
        "title": title or "",
        "city": city,
        "state": state,
        # Seller name not in card preview anymore; extract_fb_details fills it.
        "seller_name": None,
        "mileage_subtitle": mileage_sub,
        # If a card is in the live search results it's by definition live.
        "is_live": True,
        "is_pending": is_pending,
        "is_sold": is_sold,
        "url": f"https://www.facebook.com/marketplace/item/{aid}/",
    }


def extract_dom_listings(driver) -> List[Dict[str, Any]]:
    """Extract vehicle-card dicts from the live rendered DOM.

    FB Marketplace's infinite-scroll mounts new cards into the DOM as the user
    scrolls, but never updates the SSR blob in ``page_source``. Querying the DOM
    directly is the only way to see post-initial cards.

    Rendered text alone is missing mileage and category since 2026-08-06 —
    ``harvest_card_json()`` recovers both from the underlying JSON.
    """
    raw = driver.execute_script(_DOM_CARDS_JS) or []
    results: List[Dict[str, Any]] = []
    for entry in raw:
        parsed = _parse_dom_card_text(entry["id"], entry["text"])
        if parsed is not None:
            results.append(parsed)
    return results


# ── JSON side-channel: mileage & category (not rendered since 2026-08-06) ──
#
# FB stopped painting the "149K km" subtitle and the category into the card
# markup, but both still travel in the JSON that builds those cards. The initial
# page ships them in the SSR blob (readable via page_source); every later batch
# arrives as a GraphQL XHR during scroll, and the SSR blob is never updated — so
# we tee the responses. The hook must be installed with
# Page.addScriptToEvaluateOnNewDocument: injected after navigation it is too
# late, FB's bundle has already captured its own reference to window.fetch
# (measured: 0 responses captured when injected late, 17 when injected early).

_FB_GRAPHQL_HOOK_JS = r"""
(function () {
  if (window.__fbcap) return;
  window.__fbcap = [];
  const of = window.fetch;
  if (of) {
    window.fetch = function () {
      const p = of.apply(this, arguments);
      try {
        const a0 = arguments[0];
        const u = (typeof a0 === 'string') ? a0 : ((a0 && a0.url) || '');
        if (u.indexOf('/api/graphql') !== -1) {
          p.then(function (r) {
            try { r.clone().text().then(function (t) { window.__fbcap.push(t); }); } catch (e) {}
          });
        }
      } catch (e) {}
      return p;
    };
  }
  const OX = window.XMLHttpRequest;
  window.XMLHttpRequest = function () {
    const x = new OX();
    x.addEventListener('load', function () {
      try {
        if ((x.responseURL || '').indexOf('/api/graphql') !== -1) {
          window.__fbcap.push(x.responseText);
        }
      } catch (e) {}
    });
    return x;
  };
  window.XMLHttpRequest.prototype = OX.prototype;
})();
"""

# The listing id is the one immediately followed by "primary_listing_photo".
# Anchoring on a bare "id" instead picks up the nested photo / city_page ids that
# sit between the listing id and its subtitle.
_JSON_LISTING_ID_RX = re.compile(r'"id":"(\d{6,})","primary_listing_photo"')
_JSON_SUBTITLE_RX = re.compile(
    r'"custom_sub_titles_with_rendering_flags":\[\{"subtitle":"([^"]*)"')
_JSON_CATEGORY_RX = re.compile(r'"marketplace_listing_category_id":"(\d+)"')
# Facebook's own pagination verdict. This is the authoritative answer to "is the
# feed finished?" — measured 2026-08-11, a session reached 1685 listings over 59
# scrolls with has_next_page still true, while a run earlier that day concluded
# "exhausted" after 264. Silence from the feed does NOT mean the end of it.
#
# CRITICAL: a page carries MANY GraphQL connections (comments, suggestions,
# notifications), each with its own has_next_page. Grabbing the last match in the
# blob reads a stranger's flag — that mistake ended a --limit 10000 run after 17
# listings, because the SSR HTML's final connection happened to say false. Only
# the Marketplace feed's cursor carries a `pg` field, so we key on that.
#
# Field order inside page_info is NOT guaranteed, so this must not assume one.
# The previous version captured the span BETWEEN "page_info":{ and
# "has_next_page", and looked for the `pg` marker in that span — which only
# works when end_cursor happens to be serialized first. Emit has_next_page first
# and the span is empty, the marker is never found, and every verdict is None
# for the whole run (the symptom seen on 2026-08-11). Instead: take a window of
# what follows each page_info and require both the flag and the marker somewhere
# inside it, in either order.
_JSON_PAGEINFO_ANCHOR_RX = re.compile(r'"page_info":\s*\{')
_JSON_HAS_NEXT_RX = re.compile(r'"has_next_page":\s*(true|false)')
_FEED_CURSOR_MARKER = '\\"pg\\"'   # appears as \"pg\": inside the escaped end_cursor
_JSON_FEED_PG_RX = re.compile(re.escape(_FEED_CURSOR_MARKER) + r'\s*:\s*(\d+)')
# Generous enough to hold a page_info with a long escaped cursor, short enough
# that it cannot reach a neighbouring connection's fields. The scan is also cut
# at the next "page_info" whatever this says.
_PAGEINFO_WINDOW_CHARS = 1500


def _feed_page_info(blob: str) -> Tuple[Optional[bool], Optional[int]]:
    """Return ``(has_next_page, pg)`` for the MARKETPLACE FEED only.

    ``(None, None)`` means no feed page_info was found — "no opinion", which the
    caller must never read as "stop".
    """
    verdict: Optional[bool] = None
    page: Optional[int] = None
    anchors = [m.end() for m in _JSON_PAGEINFO_ANCHOR_RX.finditer(blob)]
    for i, start in enumerate(anchors):
        stop = min(start + _PAGEINFO_WINDOW_CHARS,
                   anchors[i + 1] if i + 1 < len(anchors) else len(blob))
        window = blob[start:stop]
        flag = _JSON_HAS_NEXT_RX.search(window)
        if not flag:
            continue
        # The discriminator. A page carries many GraphQL connections (comments,
        # suggestions, notifications), each with its own has_next_page; only the
        # Marketplace feed's cursor carries a `pg` field. Reading an unqualified
        # flag once ended a --limit 10000 run after 17 listings.
        if _FEED_CURSOR_MARKER not in window:
            continue
        verdict = (flag.group(1) == "true")
        pg = _JSON_FEED_PG_RX.search(window)
        page = int(pg.group(1)) if pg else None
    return verdict, page


def _feed_has_next_page(blob: str) -> Optional[bool]:
    """Read has_next_page for the MARKETPLACE FEED only, ignoring other connections."""
    return _feed_page_info(blob)[0]


def _scroll_budget(max_listings: Optional[int]) -> int:
    """Scrolls to allow, derived from how many listings the caller asked for.

    The budget must never be the binding constraint on an explicit target — that
    was the old bug, where a flat 40-scroll cap quietly turned `--limit 1000`
    into ~435. We assume a pessimistic 4 keeps per scroll (measured ~11), so the
    budget runs out only long after the real stop conditions should have fired.
    ``None`` (no cap) gets the absolute ceiling.
    """
    if max_listings is None or max_listings <= 0:
        return FB_MAX_SCROLLS_ABSOLUTE
    needed = FB_SCROLL_BUDGET_BASE + math.ceil(max_listings / FB_SCROLL_KEEPS_PER_SCROLL)
    return min(FB_MAX_SCROLLS_ABSOLUTE, needed)


# Ceiling on how far after its id a listing's own subtitle/category may sit.
# The next-id bound alone is not enough: when the id regex misses ONE listing,
# that listing's span merges into its predecessor's and its mileage is silently
# attributed to the previous car. A cap turns that into a dropped value (which
# reject_reason treats as "no opinion") instead of a wrong one.
# NOT measured — no captured feed blob survives in the repo — so it is set well
# above any plausible single-listing object rather than tuned.
_MAX_ID_TO_VALUE_CHARS = 20000


def _pair_by_position(blob: str, value_rx: "re.Pattern[str]",
                      ids: "Optional[List[Tuple[int, str]]]" = None) -> Dict[str, str]:
    """Map listing id → first ``value_rx`` match falling inside that listing.

    Positional rather than a single combined regex: field order inside the
    listing object is not guaranteed, and a greedy cross-listing match would
    silently attribute one car's mileage to another.
    """
    if ids is None:
        ids = _listing_id_positions(blob)
    vals = [(m.start(), m.group(1)) for m in value_rx.finditer(blob)]
    out: Dict[str, str] = {}
    j = 0
    for i, (pos, aid) in enumerate(ids):
        nxt = ids[i + 1][0] if i + 1 < len(ids) else len(blob)
        nxt = min(nxt, pos + _MAX_ID_TO_VALUE_CHARS)
        while j < len(vals) and vals[j][0] < pos:
            j += 1
        if j < len(vals) and vals[j][0] < nxt:
            out[aid] = vals[j][1]
    return out


def _listing_id_positions(blob: str) -> List[Tuple[int, str]]:
    return [(m.start(), m.group(1)) for m in _JSON_LISTING_ID_RX.finditer(blob)]


def _merge_card_json(blob: str, into: Dict[str, Dict[str, Any]]) -> Tuple[int, int, int]:
    """Pair one blob's subtitles/categories to their listing ids into ``into``.

    Returns ``(ids, subtitles, categories)`` counts so the caller can log drift:
    a subtitle count above the id count means the id regex missed listings, and
    values are being dropped by the distance cap rather than paired.
    """
    ids = _listing_id_positions(blob)
    subs = _pair_by_position(blob, _JSON_SUBTITLE_RX, ids)
    cats = _pair_by_position(blob, _JSON_CATEGORY_RX, ids)
    for aid in set(subs) | set(cats):
        rec = into.setdefault(aid, {})
        if aid in subs and not rec.get("mileage_subtitle"):
            rec["mileage_subtitle"] = subs[aid]
        if aid in cats and not rec.get("category_id"):
            rec["category_id"] = cats[aid]
    return len(ids), len(_JSON_SUBTITLE_RX.findall(blob)), len(_JSON_CATEGORY_RX.findall(blob))


def harvest_ssr_card_json(driver, into: Dict[str, Dict[str, Any]]) -> int:
    """Bank mileage + category for the initial, server-rendered cards.

    Call ONCE, before the scroll loop. The SSR blob in ``page_source`` is never
    updated after the initial load — it held 23 pairs while the DOM cycled
    through hundreds — so rescanning a multi-MB string every scroll buys
    nothing. Its ``page_info`` is deliberately ignored: that verdict describes
    page 1 and would be stale the moment scrolling starts.
    """
    before = len(into)
    try:
        blob = driver.page_source
    except Exception:
        return 0
    n_ids, n_subs, n_cats = _merge_card_json(blob, into)
    log(f"  SSR card JSON: {len(into) - before} ids banked "
        f"({n_ids} listing ids, {n_subs} subtitles, {n_cats} categories)")
    return len(into) - before


def harvest_card_json(driver, into: Dict[str, Dict[str, Any]],
                      feed_state: Optional[Dict[str, Any]] = None) -> int:
    """Merge mileage + category for freshly-seen cards into ``into``.

    Drains ``window.__fbcap`` — the GraphQL responses the hook teed since the
    last call — and nothing else. Call once per scroll iteration: the buffer is
    consumed, and FB unmounts off-screen cards, so anything not harvested now is
    gone. Returns the number of ids added.

    The SSR blob is NOT read here (see ``harvest_ssr_card_json``). It never
    changes after load, so rescanning it per scroll is waste — and worse, its
    ``page_info`` is a page-1 verdict: a stale ``has_next_page: false`` from it
    would reach the "Facebook says the feed is over" branch on any scroll where
    no GraphQL response arrived. Feed verdicts come only from live responses.
    """
    before = len(into)
    try:
        blobs = driver.execute_script("return (window.__fbcap||[]).splice(0)") or []
    except Exception:
        blobs = []
    ids_total = subs_total = cats_total = 0
    for blob in blobs:
        if feed_state is not None:
            verdict, pg = _feed_page_info(blob)
            if verdict is not None:
                where = f" at pg={pg}" if pg is not None else ""
                if (feed_state.get("has_next_page") != verdict
                        or feed_state.get("pg") != pg):
                    log(f"  feed verdict: has_next_page={verdict}{where}")
                feed_state["has_next_page"] = verdict
                feed_state["pg"] = pg
        n_ids, n_subs, n_cats = _merge_card_json(blob, into)
        ids_total += n_ids
        subs_total += n_subs
        cats_total += n_cats
    # Drift check: more subtitles than listing ids means _JSON_LISTING_ID_RX
    # missed listings, and _pair_by_position is dropping (not misattributing)
    # their values. Silent until it happens.
    if subs_total > ids_total or cats_total > ids_total:
        log(f"  WARNING: card JSON counts diverge — {ids_total} listing ids vs "
            f"{subs_total} subtitles / {cats_total} categories; some values "
            f"could not be paired to an id.")
    return len(into) - before


def _parse_mileage_km(subtitle: Optional[str]) -> Optional[int]:
    """Parse ``'237K km'`` or ``'148,000 km'`` into integer kilometres."""
    if not subtitle:
        return None
    s = subtitle.replace(",", "").replace(" ", "").strip().lower()
    # The unit must be matched to the END of the string. The old pattern accepted
    # "km" anywhere after the number and ignored the tail, so "156K miles"
    # ("156kmiles") matched on the "km" inside "kmiles" and returned 156 instead
    # of ~251000 — a 2008 RAV4 landed in the CSV at 172 km.
    m = re.match(r"^(\d+(?:\.\d+)?)(k|m)?(km|miles|milles|mile|mille|mi)$", s)
    if not m:
        return None
    num = float(m.group(1))
    magnitude = m.group(2)
    if magnitude == "k":
        num *= 1000
    elif magnitude == "m":
        num *= 1_000_000
    if m.group(3) != "km":
        num *= 1.609344   # FB serves some listings in miles
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
    # Category and mileage come from the JSON side-channel, which can miss a card
    # (FB unmounts fast scrollers). Unknown means "no opinion" — never a reject,
    # or a capture gap would silently delete real listings. A known-wrong value
    # is a real reject: measured 2026-08-06, all 146 Vehicles-category cards had
    # mileage and all 22 non-Vehicles cards (engines, boats, Pokémon cards) had
    # none, so category alone cleanly separates cars from junk.
    cat = card.get("category_id")
    if cat is not None and cat != FB_VEHICLES_CATEGORY_ID:
        return f"not-vehicle-{cat}"
    if title_is_non_car(card.get("title")):
        return "not-a-car"
    if card.get("is_sold") or card.get("is_pending"):
        return "sold-or-pending"
    if card.get("is_live") is False:
        return "not-live"
    # Facebook stopped RENDERING the mileage subtitle into cards on 2026-08-06
    # (0/24 carry it, identically on /search, /vehicles and search+category_id),
    # but it still SENDS it — harvest_card_json() recovers it from the JSON and
    # folds it back onto the card before this runs. Requiring it outright once
    # rejected 100% of cards, so a missing subtitle is "no opinion". A subtitle
    # that is present and cannot be parsed is a genuine reject.
    if card.get("mileage_subtitle") and _parse_mileage_km(card.get("mileage_subtitle")) is None:
        return "unparseable-mileage"
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
                              max_days: int,
                              buffer_days: int = 2) -> int:
    """Compute daysSinceListed window with overlap buffer, capped at ``max_days``.

    ``max_days`` is the profile's configured ceiling (e.g. 90). When no prior
    timestamp exists (cold start) we open the window to the full cap.
    """
    if not last_timestamp:
        return max_days
    try:
        last = datetime.fromisoformat(last_timestamp)
        delta = (datetime.now() - last).days + buffer_days
        return max(1, min(max_days, delta))
    except Exception:
        return max_days


# ── Search page navigation + card collection ──────────────────────────────

def _is_logged_in(driver) -> bool:
    """True only when FB has issued a real session cookie.

    ``c_user`` (the account id) is set only once the full login flow completes —
    including 2FA and any security checkpoint. URL/DOM heuristics are not enough:
    the 2FA page has no ``/login`` in its URL and no ``input[name=email]``, so a
    heuristic check reports "logged in" mid-challenge and we reload the search
    page out from under the user.
    """
    try:
        return any(c.get("name") == "c_user" for c in driver.get_cookies())
    except Exception:
        return False


def _maybe_login_wait(driver, target_url: str, max_wait_secs: int = 600) -> bool:
    """Poll until a real FB session exists (user logs in manually if needed).

    Returns True once ``c_user`` is present. Returns False after
    ``max_wait_secs`` without a completed login.
    """
    if _is_logged_in(driver):
        return True
    log("FB login required — log in manually in the browser window "
        "(including 2FA / security check). "
        f"Waiting up to {max_wait_secs}s...")
    waited = 0
    poll = 5
    last_stage = ""
    while waited < max_wait_secs:
        time.sleep(poll)
        waited += poll
        if _is_logged_in(driver):
            log(f"  Session cookie found after {waited}s. Reloading search page...")
            _driver_get_with_retry(driver, target_url)
            time.sleep(3)
            return True
        # Narrate which challenge the user is sitting on, once per stage change.
        url = ""
        try:
            url = driver.current_url or ""
        except Exception:
            pass
        stage = ("2FA code" if "two_step_verification" in url
                 else "security checkpoint" if "/checkpoint" in url
                 else "login form" if "/login" in url
                 else "")
        if stage and stage != last_stage:
            log(f"  Waiting on: {stage} ({waited}s elapsed)")
            last_stage = stage
    log(f"  Login did not complete within {max_wait_secs}s — aborting FB scrape.")
    log("  Tip: log in once out-of-band, then re-run. See README / CLAUDE.md.")
    return False


def get_fb_listing_cards(driver, query: str, days_since_listed: int,
                         max_listings: Optional[int],
                         model_regex: "re.Pattern[str]", seen_ad_ids: Set[str],
                         year_range: Optional[Tuple[int, int]] = None,
                         max_price: int = 15000,
                         allowed_provinces: Optional[Set[str]] = None,
                         min_price: int = 6000,
                         ) -> Tuple[List[Dict[str, Any]], Set[str]]:
    """Scroll the search page collecting passing cards until a stop condition.

    Stop conditions, each logged as a ``STOP_REASON`` tag on the way out
    (whichever hits first):
      * ``max-listings`` — ``max_listings`` passing cards collected
      * ``fb-verdict-end`` — the feed's own ``has_next_page`` came back false
      * ``consecutive-rejects`` — ``FB_MAX_CONSECUTIVE_REJECTS`` cards rejected
        in a row without a single keep; the feed has turned to padding
      * ``feed-quiet`` — every rung of ``FB_NO_NEW_BACKOFF_SECS`` elapsed with
        no unseen card id. Never decided on ``scrollHeight``: FB's next batch
        often lands after the scroll pause, and two still heights once ended a
        run at 24 cards on a feed holding 456.
      * ``scroll-budget`` — the derived budget ran out (runaway guard only)

    Already-known ad_ids are skipped (no detail fetch) but do NOT stop the
    scroll — FB's feed is non-deterministic, so a known ad can appear before
    fresh unseen ones further down.

    Returns ``(kept_cards, known_ids_re-observed)``, including on every bail-out
    path — the caller unpacks two values.
    """
    url = build_search_url(query, max_price=max_price, min_price=min_price,
                           days_since_listed=days_since_listed)
    log(f"FB search URL: {url}")
    if not _driver_get_with_retry(driver, url):
        log(f"  Bailing out of FB query after repeated navigation failures.")
        log("STOP_REASON: navigation-failed (kept=0, scrolls=0)")
        return [], set()
    time.sleep(4)
    if not _maybe_login_wait(driver, target_url=url):
        log("STOP_REASON: login-timeout (kept=0, scrolls=0)")
        return [], set()

    passing: List[Dict[str, Any]] = []
    known_seen: Set[str] = set()     # already-in-CSV ids re-observed this run
    passing_ids: Set[str] = set()
    rejected_ids: Set[str] = set()   # cards rejected by reject_reason
    seen_card_ids: Set[str] = set()  # every card we've observed on the page
    traced_ids: Set[str] = set()     # dedup for trace-file writes
    consecutive_rejects = 0  # cards rejected back-to-back, reset by any keep
    dry_scrolls = 0          # scrolls in a row that surfaced no unseen card id
    quiet_secs = 0.0         # time accumulated across those dry scrolls
    prev_seen_count = -1
    # id → {mileage_subtitle, category_id} accumulated from the JSON side-channel.
    # Persists across iterations: FB unmounts off-screen cards, so a card's JSON
    # must be banked when first seen.
    card_json: Dict[str, Dict[str, Any]] = {}
    # Facebook's own "is there more?" verdict, refreshed from every feed response.
    feed_state: Dict[str, Any] = {"has_next_page": None, "pg": None}
    stop_reason = "scroll-budget"
    scroll_i = 0

    # The server-rendered cards, banked once. page_source never changes after
    # this, so the scroll loop only drains the live GraphQL buffer.
    harvest_ssr_card_json(driver, card_json)

    # None (limits.<source>.max_new_listings: null) means "take everything the
    # feed will serve"; the safety valves below are what actually end the run.
    target = max_listings if max_listings and max_listings > 0 else math.inf
    scroll_budget = _scroll_budget(max_listings)
    for scroll_i in range(scroll_budget):
        harvest_card_json(driver, card_json, feed_state)
        cards = extract_dom_listings(driver)
        for card in cards:
            aid = card["id"]
            seen_card_ids.add(aid)
            # Fold in whatever the JSON knew about this card before filtering.
            extra = card_json.get(aid)
            if extra:
                if extra.get("mileage_subtitle"):
                    card["mileage_subtitle"] = extra["mileage_subtitle"]
                if extra.get("category_id"):
                    card["category_id"] = extra["category_id"]
            if aid in seen_ad_ids:
                known_seen.add(aid)
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
                consecutive_rejects = 0
                passing.append(card)
                passing_ids.add(aid)
                km = card.get("mileage_subtitle") or "km?"
                log(f"  + card #{len(passing)}: {card['title']} | "
                    f"CA${card.get('price_str') or '?'} | {km} | {card['city']}")
                if len(passing) >= target:
                    break
            else:
                rejected_ids.add(aid)
                consecutive_rejects += 1
                if consecutive_rejects >= FB_MAX_CONSECUTIVE_REJECTS:
                    break
        if len(passing) >= target:
            stop_reason = "max-listings"
            break

        # Junk guard: consecutive rejects, NOT cumulative. A cumulative cap is a
        # total-work limit wearing a "feed exhausted" costume — at a steady junk
        # ratio it always fires eventually no matter how good the yield is. Once
        # the category gate started working (2026-08-09) the feed's ~50% junk
        # rate burned the old 400 budget in 780 cards, cutting a 1000-listing run
        # off at 348. A run of rejects with no keep in between is what actually
        # means the feed has turned to padding.
        if consecutive_rejects >= FB_MAX_CONSECUTIVE_REJECTS:
            log(f"  {consecutive_rejects} rejects in a row "
                f"(cap={FB_MAX_CONSECUTIVE_REJECTS}) — feed looks exhausted; "
                f"stopping with {len(passing)} passing "
                f"({len(rejected_ids)} rejected overall)")
            stop_reason = "consecutive-rejects"
            break

        # Stop on "no new cards", NOT on scrollHeight. FB's next batch regularly
        # takes longer to arrive than the scroll pause, so the height sits still
        # for a beat while more results are already in flight — and two such
        # beats used to end the scrape. Measured 2026-08-09: a run bailed at 24
        # cards while that exact feed held 456+ (438 of them unseen), because
        # height stalled twice early on. Card ids are the signal that actually
        # means "the feed is exhausted".
        if len(seen_card_ids) == prev_seen_count:
            dry_scrolls += 1
            has_next = feed_state.get("has_next_page")
            # Facebook says the feed is over: believe it immediately, no ladder.
            if has_next is False:
                pg = feed_state.get("pg")
                log(f"  FEED ENDED — Facebook reports has_next_page=false"
                    f"{f' at pg={pg}' if pg is not None else ''}.")
                log(f"    This is Facebook's own verdict, not a timeout: there are "
                    f"no further pages to fetch.")
                log(f"    Ending with {len(passing)} listings kept out of "
                    f"{len(seen_card_ids)} cards seen.")
                stop_reason = "fb-verdict-end"
                break
            if dry_scrolls > len(FB_NO_NEW_BACKOFF_SECS):
                waits = ", ".join(f"{w:.0f}s" for w in FB_NO_NEW_BACKOFF_SECS)
                log(f"  STOPPING — {dry_scrolls} consecutive scrolls returned no "
                    f"new listing.")
                log(f"    Waits between attempts: {waits} "
                    f"(the last one a full 5 minutes).")
                if has_next is True:
                    log(f"    NOTE: Facebook still reports has_next_page=TRUE — the "
                        f"feed is NOT finished, it simply stopped delivering to this "
                        f"session. More listings exist; re-run to collect them.")
                else:
                    log(f"    Facebook never reported a pagination flag, so whether "
                        f"the feed is finished is unknown.")
                log(f"    Ending with {len(passing)} listings kept out of "
                    f"{len(seen_card_ids)} cards seen.")
                stop_reason = "feed-quiet"
                break
        else:
            if dry_scrolls:
                log(f"  Feed resumed after {quiet_secs:.0f}s quiet "
                    f"({dry_scrolls} empty scrolls) — continuing")
            if scroll_i == 0 or scroll_i % 25 == 0:
                log(f"  [scroll {scroll_i}] {len(seen_card_ids)} cards seen, "
                    f"{len(passing)} kept, has_next_page="
                    f"{feed_state.get('has_next_page')}")
            dry_scrolls = 0
            quiet_secs = 0.0
        prev_seen_count = len(seen_card_ids)

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        if dry_scrolls:
            # Nothing came back last time: give FB progressively longer to answer
            # before concluding anything.
            try:
                vis = driver.execute_script(
                    "return document.visibilityState + '/' + "
                    "(document.hasFocus() ? 'focused' : 'unfocused')")
            except Exception:
                vis = "?"
            if vis.split("/")[0] != "visible":
                log(f"  WARNING: page is '{vis}' — Chrome throttles hidden windows, "
                    f"which stops the infinite scroll from requesting more. "
                    f"Bring the Chrome window to the foreground.")
            wait = FB_NO_NEW_BACKOFF_SECS[dry_scrolls - 1]
            if wait >= 60:
                log(f"  Nothing new after {dry_scrolls} scrolls "
                    f"({quiet_secs:.0f}s quiet, has_next_page="
                    f"{feed_state.get('has_next_page')}) — final attempt, waiting "
                    f"{wait / 60:.0f} min")
            quiet_secs += wait
            time.sleep(wait)
        else:
            time.sleep(FB_SCROLL_PAUSE_SECS + random.uniform(0, 1.5))

    kept = passing if target is math.inf else passing[:max_listings]
    if stop_reason == "scroll-budget":
        log(f"  Scroll budget of {scroll_budget} exhausted — this is a runaway "
            f"guard, not a verdict on the feed (has_next_page="
            f"{feed_state.get('has_next_page')}).")
    log(f"STOP_REASON: {stop_reason} (kept={len(kept)}, scrolls={scroll_i + 1})")
    # Second return: ids we saw on the page but skipped because they are already
    # in the CSV. The caller MUST fold these into its "still alive" set — they are
    # live listings, and treating their absence as a disappearance is what marked
    # 178 of Camille's 189 FB rows deleted.
    return kept, known_seen


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


def extract_fb_details(driver, listing_id: str) -> Optional[VehicleListing]:
    """Navigate to a listing and extract a populated ``VehicleListing``.

    Returns None when the browser never lands on this listing's URL — the same
    positive-evidence rule ``_listing_is_gone`` follows. Waiting only for
    ``[role=main]`` to hold some text is not enough: the PREVIOUS listing's page
    already satisfies that, so a slow navigation would silently write one car's
    mileage, price and description under the next car's ad_id.
    """
    url = f"https://www.facebook.com/marketplace/item/{listing_id}/"
    listing = VehicleListing(url=url, source="facebook", ad_id=listing_id)
    if not _driver_get_with_retry(driver, url):
        return None

    try:
        WebDriverWait(driver, 15).until(
            lambda d: f"/marketplace/item/{listing_id}" in (d.current_url or "")
        )
    except Exception:
        try:
            where = (driver.current_url or "?")[:80]
        except Exception:
            where = "?"
        log(f"    browser never landed on item/{listing_id} "
            f"(still at {where}) — skipping")
        return None

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


# ── Delete sold/expired listings ──────────────────────────────────────────

# A dead listing redirects away from /marketplace/item/<id>/ to
# ".../marketplace/<city>/?unavailable_product=1" and renders "This listing
# isn't available anymore". Verified 2026-08-10 on three May listings vs a live
# one. Note the JSON `is_sold` / `is_live` flags on that page are USELESS — they
# describe the "Today's picks" sidebar items, not the listing being viewed.
_FB_GONE_URL_MARKER = "unavailable_product"
_FB_GONE_TEXT_MARKER = "isn't available anymore"


# Positive proof of life on a vehicle detail page. Absence-of-death is NOT a
# usable test: the redirect to the "unavailable" page takes a beat, and a fixed
# sleep read a still-redirecting page as live (observed on the first listing of
# a sweep, where the browser is coldest).
_FB_ALIVE_MARKERS = ("About this vehicle", "Driven ", "Seller's description")


def _listing_is_gone(driver, ad_id: str,
                     max_wait_secs: float = 30.0) -> Optional[bool]:
    """True = sold/expired, False = still live, None = could not tell.

    Polls until one side shows positive evidence, rather than trusting a single
    snapshot. ``None`` is deliberate and load-bearing: an inconclusive read must
    leave the row active, or a slow page or flaky network would silently retire
    real listings.
    """
    url = f"https://www.facebook.com/marketplace/item/{ad_id}/"
    if not _driver_get_with_retry(driver, url, attempts=2):
        return None
    deadline = time.time() + max_wait_secs
    while time.time() < deadline:
        time.sleep(1.0)
        try:
            current = driver.current_url or ""
            text = driver.execute_script(_DOM_INNERTEXT_JS) or ""
        except Exception:
            continue
        if _FB_GONE_URL_MARKER in current:
            return True
        if _FB_GONE_TEXT_MARKER.lower() in text.lower():
            return True
        if (f"/marketplace/item/{ad_id}" in current
                and any(m in text for m in _FB_ALIVE_MARKERS)):
            return False
    # Window exhausted with neither marker: indeterminate, per this function's
    # own contract. Sitting on the item URL with a rendered page is NOT proof of
    # life — it is the absence of proof of death, which is exactly the test that
    # produced silent false negatives when the redirect had not landed yet. The
    # caller leaves the row active and does not stamp it as re-seen.
    return None


def delete_sold_listings(driver, csv_file: str, scrape_time: str,
                         stale_after_days: int = 0,
                         save_every: int = 20) -> int:
    """Visit EVERY active Facebook listing and retire the ones that are gone.

    Facebook's own feed cannot retire a listing for us: a search only ever shows
    what is currently for sale, so a sold car simply stops appearing — and the
    deletion-detection health guard (rightly) refuses to infer a disappearance
    from a partial scrape. Re-opening each listing is the only reliable way.

    Every candidate row is checked individually; nothing is sampled.
    ``save_every`` is only how often the CSV is flushed to disk, so an
    interrupted hour-long pass keeps the work it already did — it does NOT skip
    listings. ``stale_after_days`` optionally narrows the set to rows not
    re-seen in that many days (0 = every active row, the default).

    Rows are FLAGGED via ``is_deleted``, never dropped: the CSV is append-only
    by design and the scatter already hides flagged rows.
    """
    if not os.path.exists(csv_file):
        return 0
    df = pd.read_csv(csv_file, dtype={"ad_id": str})
    if not {"source", "ad_id", "is_deleted"} <= set(df.columns):
        return 0
    # is_deleted is a TIMESTAMP, not a boolean. On a CSV where every row is
    # still active pandas types the all-NaN column as float64, and assigning an
    # ISO string into it warns today and raises under pandas 3.
    df["is_deleted"] = df["is_deleted"].astype(object)

    candidates = df["is_deleted"].isna() & (df["source"] == "facebook") & df["ad_id"].notna()
    if stale_after_days > 0 and "last_scrape_timestamp" in df.columns:
        seen = pd.to_datetime(df["last_scrape_timestamp"], errors="coerce", format="mixed")
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=stale_after_days)
        candidates &= seen.isna() | (seen < cutoff)

    idx = list(df.index[candidates])
    if not idx:
        log("Delete-sold: nothing to check.")
        return 0
    scope = "all active" if stale_after_days <= 0 else f"not re-seen in {stale_after_days}d"
    log(f"Delete-sold: checking {len(idx)} Facebook listings ({scope}), "
        f"one by one — "
        f"roughly {len(idx) * FB_SWEEP_SECS_PER_LISTING / 60:.0f} min")

    gone = unknown = 0
    for n, row in enumerate(idx, start=1):
        ad_id = _ad_id_str(df.at[row, "ad_id"])
        if not ad_id:
            continue
        verdict = _listing_is_gone(driver, ad_id)
        raw_title = df.at[row, "title"]
        title = "" if pd.isna(raw_title) else str(raw_title)[:45]
        if verdict is True:
            df.at[row, "is_deleted"] = scrape_time
            gone += 1
            log(f"  [{n}/{len(idx)}] SOLD/GONE — {title}")
        elif verdict is None:
            unknown += 1
            log(f"  [{n}/{len(idx)}] unreadable, left active — {title}")
        else:
            df.at[row, "last_scrape_timestamp"] = scrape_time
        if n % save_every == 0:
            df.to_csv(csv_file, index=False)
            log(f"  … {n}/{len(idx)} checked ({gone} gone so far, progress saved)")
        if n < len(idx):
            time.sleep(FB_LISTING_PAUSE_SECS + random.uniform(0, FB_LISTING_PAUSE_JITTER))

    df.to_csv(csv_file, index=False)
    log(f"Delete-sold: {gone} retired as sold/expired, "
        f"{len(idx) - gone - unknown} still live, {unknown} unreadable (left active)")
    return gone


# ── End-to-end orchestrator (mirrors scrape_vehicle) ──────────────────────

def scrape_vehicle_facebook(driver, query: str,
                            model_regex_src: str,
                            model_canonical: Optional[str],
                            output_file: str, scrape_num: int, scrape_time: str,
                            max_days_since_listed: int,
                            make: Optional[str] = None,
                            max_listings: Optional[int] = 10,
                            year_range: Optional[Tuple[int, int]] = None,
                            max_price: int = 15000,
                            allowed_provinces: Optional[Set[str]] = None,
                            min_price: int = 6000,
                            session_last_scrape_timestamp: Optional[str] = None,
                            days_override: Optional[int] = None) -> None:
    """Scrape FB for one query and merge into ``output_file`` CSV.

    Mirrors ``scrape_vehicle()`` for AutoTrader: loads the existing CSV,
    collects fresh listings, dedups, refreshes ``last_scrape_timestamp`` on the
    rows this scrape re-observed, and writes back.

    It does NOT mark absent rows as deleted, and must not: a Marketplace search
    is bounded by ``daysSinceListed``, so a listing posted outside the window
    cannot appear however alive it is. Inferring disappearance from absence
    flagged 564 rows on 2026-08-11, of which a 16-row sample found 15 still for
    sale. ``delete_sold_listings()`` is the only path that retires a listing,
    and it does so by re-opening it.

    ``model_canonical`` and ``make`` may be ``None`` for generic searches
    (e.g. body-type or drivetrain queries that don't pin a make/model). The
    CSV's ``make``/``model`` columns are left blank for those rows.
    """
    log_id = model_canonical or query
    # Load existing CSV
    existing_df = None
    if os.path.exists(output_file):
        try:
            # ad_id as str: float64 mangles 17-digit ids (see _ad_id_str).
            existing_df = pd.read_csv(output_file, dtype={"ad_id": str})
        except pd.errors.EmptyDataError:
            existing_df = None

    if existing_df is not None and "source" not in existing_df.columns:
        existing_df["source"] = "autotrader"

    # Every FB ad_id already stored, whatever query or model it came in under.
    # An ad_id identifies a listing, not a model, so a row already in the CSV is
    # already in the CSV — re-fetching it appends a duplicate. Scoping this set
    # by `model == model_canonical` existed to protect the deletion inference
    # that no longer exists; all it does now is re-fetch and re-append rows a
    # different query happened to store first.
    seen_ad_ids: Set[str] = set()
    if existing_df is not None and "source" in existing_df.columns and "ad_id" in existing_df.columns:
        mask = (existing_df["source"] == "facebook") & existing_df["ad_id"].notna()
        seen_ad_ids = {a for a in (_ad_id_str(v) for v in existing_df.loc[mask, "ad_id"]) if a}

    # Compute daysSinceListed. The orchestrator passes
    # ``session_last_scrape_timestamp`` once per run (or None on cold start);
    # we never re-load a default state file here, since that would silently
    # cross profile boundaries.
    if days_override is not None:
        days = max(1, min(365, int(days_override)))
    else:
        days = compute_days_since_listed(
            session_last_scrape_timestamp, max_days=max_days_since_listed,
        )
    log(f"FB scrape: {log_id} (query={query!r}) max={max_listings} days={days} "
        f"seen_ad_ids={len(seen_ad_ids)}")
    trace_section(f"{log_id} query={query!r} days={days}")

    # Collect cards
    model_regex = re.compile(model_regex_src, re.IGNORECASE)
    cards, known_seen_ids = get_fb_listing_cards(
        driver, query=query, days_since_listed=days,
        max_listings=max_listings, model_regex=model_regex,
        seen_ad_ids=seen_ad_ids, year_range=year_range, max_price=max_price,
        # No `or {"QC"}` default: filters.province: null in the profile means the
        # user disabled the filter, and reject_reason() reads None as "no opinion".
        allowed_provinces=allowed_provinces, min_price=min_price,
    )
    log(f"Collected {len(cards)} cards passing filter; fetching details...")

    # Ids proven alive this run: freshly-collected cards PLUS the already-known
    # ones we skipped. Omitting the second group makes every listing the
    # incremental skip works on look like it vanished.
    scraped_ad_ids: Set[str] = {c["id"] for c in cards} | known_seen_ids

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
        if listing is None:
            # Never landed on this listing's page — anything read there would
            # belong to the previous one.
            trace_card("DROP", card, "detail-page-not-reached")
            if idx < len(cards):
                time.sleep(FB_LISTING_PAUSE_SECS + random.uniform(0, FB_LISTING_PAUSE_JITTER))
            continue

        # Mileage gate — moved here from the card filter now that FB strips the
        # subtitle from search previews. The detail page still renders
        # "Driven N km", so this is the first point we can confirm the listing is
        # a vehicle at all; it also keeps every FB row plottable, since the
        # scatter puts mileage on the x axis.
        if listing.mileage_km is None:
            # The card's JSON subtitle ("149K km") is a fine fallback when the
            # detail page fails to render its "Driven N km" line.
            listing.mileage_km = _parse_mileage_km(card.get("mileage_subtitle"))
        if listing.mileage_km is None:
            log(f"    dropped (no mileage on detail page) — {listing.title or card['title']}")
            trace_card("DROP", card, "detail-no-mileage")
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
        if not listing.make:
            # Generic FB queries (e.g. Émile's "awd") leave make blank in the
            # query config. Fall back to parsing the title so the row passes the
            # scatter's make filter.
            listing.make = make_from_title(listing.title or card.get("title"))
        if not listing.model and listing.make:
            listing.model = model_from_title(
                listing.title or card.get("title"), listing.make,
            )
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
        fb_mask = existing_df["source"] == "facebook"
        if model_canonical is not None:
            # Same pandas trap as seen_ad_ids: `== None` is False everywhere, so
            # scoping a generic query by model silently disables this whole block.
            fb_mask &= (existing_df["model"] == model_canonical)

        # Normalise both sides: the CSV column round-trips through float64
        # elsewhere in the pipeline, so a raw astype(str) yields "123.0" and
        # never matches a card id.
        existing_ids = existing_df["ad_id"].map(_ad_id_str)

        still_present = fb_mask & existing_ids.isin(scraped_ad_ids)
        existing_df.loc[still_present, "last_scrape_timestamp"] = scrape_time

        # Health guard, mirroring the AutoTrader path: only trust a
        # disappearance when the scrape itself looks complete. A partial scrape
        # (login wall, feed truncated, network blip) must never mass-delete.
        existing_active = int((fb_mask & existing_df["is_deleted"].isna()).sum())
        seen_count = int(still_present.sum())
        healthy = (len(scraped_ad_ids) >= FB_MIN_IDS_FOR_DELETION
                   and (existing_active < FB_MIN_ACTIVE_FOR_DELETION
                        or seen_count / max(1, existing_active) >= FB_MIN_SEEN_RATIO))
        if not scraped_ad_ids:
            log("Skipping FB deletion marking — no ad_ids scraped")
        elif not healthy:
            log(f"Skipping FB deletion marking — scrape looks partial "
                f"({len(scraped_ad_ids)} ids seen, {seen_count}/{existing_active} "
                f"existing rows re-found)")
        else:
            revived = fb_mask & still_present & existing_df["is_deleted"].notna()
            if revived.any():
                log(f"Resurrecting {int(revived.sum())} FB listings that came back")
                existing_df.loc[revived, "is_deleted"] = pd.NA
            # DELIBERATELY NOT marking disappearances from a search scrape.
            #
            # A Marketplace search is windowed by daysSinceListed: a listing
            # posted before that window CANNOT appear, however alive it is. So
            # "absent from this scrape" does not mean "gone" — it usually means
            # "outside the window", or simply not served, since the feed is
            # non-deterministic and returns a different slice each session.
            #
            # Measured 2026-08-11: a days=7 run marked 564 rows deleted; re-opening
            # a 16-row sample found 15 still for sale. The ratio health guard did
            # not help — 1633 cards seen cleared the 50% threshold easily. The
            # guard checks whether the scrape was BIG, not whether the feed could
            # have testified about those rows at all.
            #
            # delete_sold_listings() is the reliable path: it re-opens each stored
            # listing and reads Facebook's own answer. It runs by default before
            # every scrape (--no-delete-sold skips it).
            missing = int((fb_mask & ~existing_ids.isin(scraped_ad_ids)
                           & existing_df["is_deleted"].isna()).sum())
            if missing:
                log(f"{missing} stored FB listings were not in this scrape — NOT "
                    f"marking them deleted (a windowed search cannot prove absence; "
                    f"the delete-sold pass is what retires listings)")

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
