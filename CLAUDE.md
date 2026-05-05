# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Used-vehicle scraper for the Montréal area. Despite the legacy filename (`bmw_x3_scraper.py`), the scraper supports multiple **profiles** (one YAML per user) and pulls from AutoTrader.ca **and** Facebook Marketplace. Two profiles ship today:

- **`camille.yaml`** — sub-$15K SUV search, 7 specific Japanese/Korean make/model pairs, year ≥ 2016, 300 km radius from H1X 3J1. Writes `used_suv_listings.csv` and `suv_scatter.html`. Pushed to https://winzee.github.io/autotrader-camille/suv_scatter.html.
- **`emile.yaml`** — sub-$9K AWD search (no body-type/model filter; `dtrain=A` URL param), Japanese + Korean brands, year ≥ 2007, 200 km radius from H1X 3J1. Writes `emile_suv_listings.csv` and `emile_scatter.html`. Pushed to https://winzee.github.io/autotrader-camille/emile_scatter.html (same repo as Camille).

Per-profile knobs (postal code, radius, price min/max, year min, province filter, vehicle list, FB queries, output paths, GitHub Pages target, HTML title/heading, optional scatter price caps) all live in the YAML file. See `config.py` for the full schema.

## Commands

```bash
source venv/bin/activate

# Install dependencies (no requirements.txt — manual install)
pip install selenium webdriver-manager beautifulsoup4 pandas pyyaml

# Full scrape with the default profile (camille.yaml)
python bmw_x3_scraper.py

# Run with another profile
python bmw_x3_scraper.py --config emile.yaml

# Single-vehicle dev runs (matches against the active profile's search_units;
# `make/model` for model-specific units or just `make` for make-only units)
python bmw_x3_scraper.py --config camille.yaml --make-model honda/cr-v
python bmw_x3_scraper.py --config emile.yaml   --make-model toyota

# Rebuild the plot only (no scrape)
python bmw_x3_scraper.py --config camille.yaml --generate-html-only

# Skip the GitHub Pages push for either profile
python bmw_x3_scraper.py --config emile.yaml --no-publish
```

There are no tests, linters, or build steps. Run logs go to `logs/run_YYYY-MM-DD_HHMMSS.log` (per run, gitignored).

## Architecture

Three-file scraper:
- `config.py` — loads a YAML profile and exposes a typed `Config` dataclass.
- `bmw_x3_scraper.py` (~1700 lines) — AutoTrader scrape, CSV merge, scatter generation, GitHub push, FB orchestration.
- `fb_scraper.py` — Facebook Marketplace scraping (separate browser session, persistent profile under `fb_profile/`).

### AutoTrader path (must handle BOTH apps)

AT A/B-routes between two apps; both must be supported:
- **Next.js**: `/offers/<make>-<model>-…` URLs, exposes `window.__NEXT_DATA__.props.pageProps.listingDetails`. Parsed by `parse_next_data()`.
- **Angular (legacy)**: `/a/<make>/<model>/…` URLs, exposes `window.ngVdpModel`. Parsed by `parse_ngvdp_model()`.

Pipeline:
1. `_collect_page_links()` — collects both `a[href*='/offers/']` and `a[href*='/a/']` anchors, then `scrape_vehicle()` post-filters by `SearchUnit` (make + optional model) via `_make_model_url_patterns()`.
2. `extract_listing_details()` is a tiered dispatcher: Tier 1a `__NEXT_DATA__`, Tier 1b `ngVdpModel`, Tier 2 BeautifulSoup on embedded `__NEXT_DATA__`. **All tiers must stay wired up** — recent breakage occurred when the `ngVdpModel` tier was accidentally dropped from the dispatcher.
3. URL slug matching uses `_make_model_url_patterns()`. For model-specific units (Camille) it generates `/a/<make>/<model>/` and `/offers/<make>-<model>-` plus the rav4↔rav-4 alt spelling. For make-only units (Émile) it matches any `/a/<make>/` or `/offers/<make>-` URL — model filtering is replaced by URL params like `dtrain=A`.
4. The AT search URL is built by `build_at_search_url(unit, cfg.autotrader_search)`. Search params come from `cfg.autotrader_search` (year, price min/max, radius, postal code) plus everything in `extra_params` (e.g. `{dtrain: A}`). When a unit has no model, the URL drops to `/cars/<make>/qc/montréal/`. AT 200s on this and applies `dtrain=A` correctly; the location/radius is honored via the browser session, not the URL params (AT redirects strip them).

### CSV lifecycle (per-profile, e.g. `used_suv_listings.csv` / `emile_suv_listings.csv`)

Listings are never deleted from the CSV — they're flagged. Three relevant columns:
- `scrape_timestamp` — when first scraped (immutable).
- `last_scrape_timestamp` — updated to the current run when the URL is re-found.
- `is_deleted` — NaN when active; ISO timestamp of the run that flagged it disappeared. **The column stores a timestamp, not a boolean** — check `notna()` / `isna()`, never `== 'true'`.

Each `scrape_vehicle()` call:
1. Updates `last_scrape_timestamp` for URLs still in the search results.
2. **Resurrects** any matching URL whose `is_deleted` is set (clears it back to NaN).
3. Marks newly-disappeared URLs as deleted **only if the scrape looks healthy**: `≥5 URLs scraped` AND (`existing_active < 3` OR `seen_count / existing_active ≥ 0.5`). Otherwise treated as a partial failure and skipped — protects against false-positive deletions when a single scrape misses listings.

A final `province == cfg.filters.province` filter drops out-of-province listings; AT search radius leaks Ontario dealers, so this filter is load-bearing. Set `filters.province: null` in the YAML to disable.

`collapse_cross_source_duplicates()` runs after all sources to drop same-car-different-source duplicates by `(make, model, year, mileage_km, price_cad)`, preferring AutoTrader rows.

### Facebook path

`fb_scraper.py` uses a **persistent Chrome profile** (`fb_profile/`) so login cookies survive between runs. Scrolls Marketplace search, applies regex+year filters per `cfg.fb_queries`, hydrates each card, and merges into the profile's CSV. Each FB query has `query` (free-text), `regex` (title filter), `model_canonical` (CSV `model` value, may be null for generic searches like Émile's "awd"), `year_range`, and optional `make` (CSV `make` value; if null, the row's `make` is left blank).

### Scatter chart (`generate_scatter_html`)

Profile-agnostic — adapts to whatever models the active profile's CSV contains. The pipeline:

1. **Profile filter** — keep rows whose `make` is in `{u.make.capitalize() for u in cfg.search_units}` and (if set) `province == cfg.filters.province`.
2. **Outlier filter** — `_iqr_fences()` computes Tukey 3×IQR fences on `price_cad` and `mileage_km`. Rows outside both fences are dropped. **Use 3× (extreme outlier), not the textbook 1.5×** — on small samples (~50 listings) the 1.5× fence cuts into legitimate budget-end deals (e.g. a $4999 2011 Mazda Tribute is *not* an outlier in a $3-9k market). 3× catches only true junk like a $5 broken-car listing or a $32k Subaru that slipped past the URL filter.
3. **Axis bounds** — `_axis_bounds()` sizes both axes to `[data_min - 4% pad, data_max + 4% pad]` of the *filtered* data. `cfg.html.chart_price_floor` and `cfg.html.chart_price_max` are **optional** clamps applied on top — set them only when you want a hard ceiling regardless of data (typical case: leave both unset).
4. **Dynamic per-model datasets** — every unique `model` in the surviving data gets its own Chart.js dataset, ordered by frequency (most-common gets `circle`, etc.). Shapes cycle through `_SCATTER_SHAPE_POOL` (10 styles); model→shape is recomputed each render. Legend checkboxes and the SVG icons next to them are generated server-side in Python by `_shape_svg()` so they always match what's drawn on the canvas.
5. **Line-shape rendering** — Chart.js draws `cross`, `crossRot`, `dash`, `line`, and `star` as stroke-only (no fill); they render at 0px without `pointBorderWidth`. The list lives in `_SCATTER_LINE_SHAPES` and is shipped to the JS as a `LINE_SHAPES` Set. Any model assigned a stroked shape gets `pointBorderWidth: 2.5` and `pointRadius + 1` so its glyph reads at the same visual weight as a filled shape. **If a Chart.js update adds a new stroke-only point style, append it to `_SCATTER_LINE_SHAPES` — that's the only thing to change.**

Color encodes scrape freshness (gold = latest run, green = today/yesterday, gray = older). Shape encodes model. The two axes are price (y) vs mileage (x).

## Data Reference

- `gu.json` — sample `ngVdpModel` (Angular detail page).
- `next_data_sample.json` — sample `listingDetails` (Next.js detail page). Province lives at `seller.dealer.region`.

## Configuration

All per-user knobs live in YAML profiles (`camille.yaml`, `emile.yaml`). See `config.py` for the full schema. Top-level sections:
- `output` — CSV / scatter HTML / log directory paths.
- `html` — page title, H1 heading, optional public URL link. `chart_price_max` / `chart_price_floor` are **optional** hard caps applied on top of the auto-derived axis bounds (leave them out for pure auto-scale).
- `github_pages` — `enabled: bool`; `repo: <user/repo>` when enabled. Multiple profiles can share one repo (each commits its own scatter HTML file).
- `filters.province` — single-province filter (e.g. `QC`); `null` disables.
- `autotrader.search` — `year_min`, `price_min`, `price_max`, `radius_km`, `postal_code`, `extra_params` (free-form dict appended to the AT URL — e.g. `{dtrain: A}` for AWD-only).
- `autotrader.search_units` — list of `{make, model?}`. Omitting `model` searches all of that make's listings (combine with `extra_params` to filter further).
- `facebook.defaults` and `facebook.queries` — FB-specific overrides; each query supports `make`, `model_canonical`, `query`, `regex`, `year_range`.

Environmental constants still hardcoded:
- Chrome binary: `/Applications/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing`. Chrome for Testing must be installed locally.
- `LISTING_PAUSE_SECS = 5` between detail extractions.

## Known issues / gotchas

- **AT search returns Ontario dealers despite `prx=300`.** Listing URLs don't contain `/ontario/` so the URL-level filter misses them; the QC province filter at write time catches them but a lot of work is wasted parsing them. See `investigation_notes.md`.
- **`rcp=100` is silently ignored.** Each search page returns ~20 URLs regardless. We paginate via `&page=N` instead — `get_listing_urls()` reads `__NEXT_DATA__.props.pageProps.numberOfPages` on page 1 and loops until it hits the last page. Pages stay on the Next.js app (the long-standing "page=N flips back to Angular" claim was either transient or fixed upstream — verified across pages 1, 2, 3, 5, 10, 11 of an Émile Subaru search on 2026-05-04). When the metadata is missing (Angular session), the loop falls back to "stop when a page returns no new URLs".
- **`is_deleted` is a timestamp, not a boolean.** Any analysis script must use `notna()` / `isna()`.
- **Run logs are best-effort.** `setup_run_log()` tees stdout/stderr to `logs/`; failures there are swallowed so logging never breaks a scrape.
- **Chart.js stroke-only point styles render at 0px without `pointBorderWidth`.** `cross`, `crossRot`, `dash`, `line`, and `star` are line-only — they have no fill. If you add a new shape to `_SCATTER_SHAPE_POOL` and forget to also add it to `_SCATTER_LINE_SHAPES` (when applicable), points using it will appear as a label hovering above empty space. We've hit this multiple times — the canonical list lives in `bmw_x3_scraper.py` and ships into the inline JS as `LINE_SHAPES`.
- **Tukey 1.5×IQR is too aggressive on small samples.** The scatter outlier filter uses 3×IQR ("extreme outlier") instead. With ~50 listings, 1.5× cuts into legitimate budget-end deals and high-mileage real cars. 3× catches only true junk (a $5 broken-car listing, a $32k Subaru in Camille's $15k search). See `_iqr_fences()` in `bmw_x3_scraper.py`.
