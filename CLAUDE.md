# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

AutoTrader BMW X3 web scraper — a Python script that scrapes used BMW X3 listings (2021-2023) from AutoTrader.ca for automotive market research. Extracted data feeds into a Tableau workbook (`BMW analyse.twb`) for visualization.

## Commands

```bash
# Activate virtual environment
source venv/bin/activate

# Install dependencies (no requirements.txt — manual install)
pip install selenium webdriver-manager beautifulsoup4 pandas

# Run the scraper
python bmw_x3_scraper.py
```

There are no tests, linters, or build steps configured.

## Architecture

Single-file scraper (`bmw_x3_scraper.py`, ~640 lines) with this flow:

1. **Search page** — Builds AutoTrader URL with filters, opens headless Chrome via Selenium, scrolls to trigger lazy-loaded listings
2. **Link extraction** — Collects all `/a/` listing URLs from the search results page
3. **Detail scraping** — For each listing, extracts data from the `window.ngVdpModel` JavaScript object (primary) or falls back to BeautifulSoup HTML parsing
4. **Deduplication** — Removes duplicates by `(ad_id, dealer_co_id)` tuple
5. **CSV output** — Appends unique listings to `bmw_x3_used_2021_2023.csv` with incremental `scrape_number` and `scrape_timestamp`

Key components:
- `VehicleListing` dataclass — 28-field container for vehicle data
- `create_driver()` — Headless Chrome setup with anti-detection options
- `get_listing_urls()` — Search page navigation, cookie handling, infinite scroll
- `extract_listing_details()` / `parse_ngvdp_model()` — Data extraction with JS-first, HTML-fallback strategy
- `deduplicate_listings()` — ID-based dedup before CSV append

## Data Reference

`gu.json` is a sample of AutoTrader's `ngVdpModel` schema — use it to understand the nested object structure (adBasicInfo, hero, seller, priceAnalysis, conditionAnalysis, dealerTrust, vehicle, specifications, etc.).

## Configuration

Search parameters (make, model, year range, radius, postal code) are hardcoded in the search URL construction in `main()`. Chrome binary path is set to `/Applications/Google Chrome for Testing.app/...`. The scraper requires Chrome for Testing to be installed locally.
