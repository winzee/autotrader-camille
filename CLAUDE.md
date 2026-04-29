# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Used-SUV scraper for the Montréal area, built to help shop for a sub-$15K SUV. Despite the legacy filename (`bmw_x3_scraper.py`), the scraper now covers seven make/models from AutoTrader.ca **and** Facebook Marketplace, writes to `used_suv_listings.csv`, and regenerates an interactive scatter plot (`suv_scatter.html`) that is pushed to GitHub Pages on every run (https://winzee.github.io/autotrader-camille/suv_scatter.html).

Vehicles tracked (defined in `VEHICLES` list): Subaru Forester / Outback / Crosstrek, Toyota RAV4, Honda HR-V / CR-V, Hyundai Kona.

## Commands

```bash
source venv/bin/activate

# Install dependencies (no requirements.txt — manual install)
pip install selenium webdriver-manager beautifulsoup4 pandas

# Full scrape: AutoTrader (7 vehicles) + Facebook + scatter HTML + git push
python bmw_x3_scraper.py

# Single-vehicle dev runs
python bmw_x3_scraper.py --make-model honda/cr-v
python bmw_x3_scraper.py --generate-html-only   # rebuild plot only
```

There are no tests, linters, or build steps. Run logs go to `logs/run_YYYY-MM-DD_HHMMSS.log` (per run, gitignored).

## Architecture

Two-file scraper:
- `bmw_x3_scraper.py` (~1700 lines) — AutoTrader scrape, CSV merge, scatter generation, GitHub push, FB orchestration.
- `fb_scraper.py` — Facebook Marketplace scraping (separate browser session, persistent profile under `fb_profile/`).

### AutoTrader path (must handle BOTH apps)

AT A/B-routes between two apps; both must be supported:
- **Next.js**: `/offers/<make>-<model>-…` URLs, exposes `window.__NEXT_DATA__.props.pageProps.listingDetails`. Parsed by `parse_next_data()`.
- **Angular (legacy)**: `/a/<make>/<model>/…` URLs, exposes `window.ngVdpModel`. Parsed by `parse_ngvdp_model()`.

Pipeline:
1. `_collect_page_links()` — collects both `a[href*='/offers/']` and `a[href*='/a/']` anchors, then `scrape_vehicle()` post-filters by make/model slug via `_make_model_url_patterns()`.
2. `extract_listing_details()` is a tiered dispatcher: Tier 1a `__NEXT_DATA__`, Tier 1b `ngVdpModel`, Tier 2 BeautifulSoup on embedded `__NEXT_DATA__`. **All tiers must stay wired up** — recent breakage occurred when the `ngVdpModel` tier was accidentally dropped from the dispatcher.
3. URL slug matching uses `_make_model_url_patterns()` which generates both `/a/<make>/<model>/` and `/offers/<make>-<model>-` plus an alt spelling that inserts a hyphen between letter+digit (so `rav4` also matches `rav-4` URLs).

### CSV lifecycle (`used_suv_listings.csv`)

Listings are never deleted from the CSV — they're flagged. Three relevant columns:
- `scrape_timestamp` — when first scraped (immutable).
- `last_scrape_timestamp` — updated to the current run when the URL is re-found.
- `is_deleted` — NaN when active; ISO timestamp of the run that flagged it disappeared. **The column stores a timestamp, not a boolean** — check `notna()` / `isna()`, never `== 'true'`.

Each `scrape_vehicle()` call:
1. Updates `last_scrape_timestamp` for URLs still in the search results.
2. **Resurrects** any matching URL whose `is_deleted` is set (clears it back to NaN).
3. Marks newly-disappeared URLs as deleted **only if the scrape looks healthy**: `≥5 URLs scraped` AND (`existing_active < 3` OR `seen_count / existing_active ≥ 0.5`). Otherwise treated as a partial failure and skipped — protects against false-positive deletions when a single scrape misses listings.

A final `province == "QC"` filter drops out-of-province listings; AT search radius leaks Ontario dealers, so this filter is load-bearing.

`collapse_cross_source_duplicates()` runs after all sources to drop same-car-different-source duplicates by `(make, model, year, mileage_km, price_cad)`, preferring AutoTrader rows.

### Facebook path

`fb_scraper.py` uses a **persistent Chrome profile** (`fb_profile/`) so login cookies survive between runs. Scrolls Marketplace search, applies regex+year filters per `FB_QUERIES`, hydrates each card, and merges into the same `used_suv_listings.csv`.

## Data Reference

- `gu.json` — sample `ngVdpModel` (Angular detail page).
- `next_data_sample.json` — sample `listingDetails` (Next.js detail page). Province lives at `seller.dealer.region`.

## Configuration

Hardcoded in `main()`/`COMMON_PARAMS`/`VEHICLES`/`FB_QUERIES`:
- Year ≥ 2016, max price $15K, radius 300 km from H1X 3J1, used only.
- Chrome binary: `/Applications/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing`. Chrome for Testing must be installed locally.
- `LISTING_PAUSE_SECS = 5` between detail extractions.

## Known issues / gotchas

- **AT search returns Ontario dealers despite `prx=300`.** Listing URLs don't contain `/ontario/` so the URL-level filter misses them; the QC province filter at write time catches them but a lot of work is wasted parsing them. See `investigation_notes.md`.
- **`rcp=100` is silently ignored.** Each search page returns ~20 URLs per vehicle regardless. Pagination via `&page=N` is unreliable and frequently breaks back to Angular.
- **`is_deleted` is a timestamp, not a boolean.** Any analysis script must use `notna()` / `isna()`.
- **Run logs are best-effort.** `setup_run_log()` tees stdout/stderr to `logs/`; failures there are swallowed so logging never breaks a scrape.
