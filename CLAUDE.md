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
3. Marks newly-disappeared URLs as deleted only when **both** health questions pass. They ask different things and both are load-bearing:
   - **Could the search testify?** `get_listing_urls()` returns `(urls, search_healthy)`; `search_healthy` is False when `_collect_page_links()` hit the CAPTCHA branch on any page (solved or not) or the whole search yielded zero links. A blocked page returns an empty/partial URL set, and the difference against the CSV then reads as disappearance. **Size checks cannot see this** — a blocked search is small, not wrong, which is exactly the "guard that measures the wrong thing" failure from P13 in `POSTMORTEM.md`.
   - **Was the scrape big enough?** `≥5 URLs scraped` AND (`existing_active < 3` OR `seen_count / existing_active ≥ 0.5`).

   When either fails, deletion marking is skipped and logged; resurrection and `last_scrape_timestamp` updates still run.

The `province == cfg.filters.province` filter applies **only to the newly-scraped rows being merged in**, never to rows already stored — the CSV contract is flag-don't-delete, and filtering the combined frame dropped every stored row whose `province` was blank (all FB rows whose city parse failed), once per search unit. A null province is *no opinion* and is kept. The read-time filter in `generate_scatter_html` is the authority on what actually charts. Set `filters.province: null` in the YAML to disable both.

`collapse_cross_source_duplicates()` runs after all sources to drop same-car-different-source duplicates by `(make, model, year, mileage_km, price_cad)`, preferring AutoTrader rows.

### Facebook path

`fb_scraper.py` uses a **persistent Chrome profile** (`fb_profile/`) so login cookies survive between runs. Scrolls Marketplace search, applies regex+year filters per `cfg.fb_queries`, hydrates each card, and merges into the profile's CSV. Each FB query has `query` (free-text), `regex` (title filter), `model_canonical` (CSV `model` value, may be null for generic searches like Émile's "awd"), `year_range`, and optional `make` (CSV `make` value; if null, the row's `make` is left blank).

**`create_fb_driver()` attaches to Chrome — it does not let chromedriver launch it.** It starts Chrome for Testing as an ordinary process with `--remote-debugging-port=9222` on `fb_profile/`, then connects Selenium via `debuggerAddress`. This is load-bearing, not a style choice: when chromedriver spawns the browser it sets `--enable-automation` / `navigator.webdriver = true` and runs an ad-hoc re-signed binary clone, and FB reads that as an untrusted device — it re-challenges with 2FA every run and then **deletes `c_user`/`xs` server-side**, so no amount of profile persistence helps. Attached, `navigator.webdriver` is naturally false and the session sticks (verified 2026-08-06 across a cold restart). Corollary: no other Chrome may hold `fb_profile/` while the scraper runs, or the debug port never opens.

**Mileage and category come from a JSON side-channel, not the card markup.** `harvest_card_json()` builds an `id → {mileage_subtitle, category_id}` map from two sources: the SSR blob in `page_source` (initial ~24 cards) and the GraphQL XHR responses teed by `_FB_GRAPHQL_HOOK_JS` (every later scroll batch). Three things are load-bearing:
- The hook is installed via `Page.addScriptToEvaluateOnNewDocument` in `create_fb_driver()`. Injected after navigation it captures **nothing** — FB's bundle has already grabbed its own `window.fetch` reference (measured: 0 responses late vs 17 early).
- `harvest_card_json()` must run **once per scroll iteration**. It drains `window.__fbcap`, and FB unmounts off-screen cards, so unharvested JSON is gone for good (after 6 scrolls the SSR blob still held only the original 23 cards while the DOM had cycled to 56 entirely different ones).
- Ids are paired to values **by position** (`_pair_by_position`), anchored on `"id":"…","primary_listing_photo"`. A naive combined regex grabs the nested photo or `city_page` id sitting between the listing id and its subtitle.

`reject_reason()` treats a missing value as *no opinion*, never a reject — a capture gap must not delete real listings. A known-wrong category **is** a reject: measured 2026-08-06, all 146 Vehicles-category cards had mileage and all 22 non-Vehicles ones (engines, boats, Pokémon cards) had none. On a full sweep this filters 32% of the feed (248/766) with zero detail fetches, and rejects 0 cards for missing mileage.

Login state is detected by the **`c_user` cookie**, never by URL or DOM heuristics — the 2FA page has no `/login` in its URL and no `input[name="email"]`, so heuristics report "logged in" mid-challenge and reload the search page out from under the user. When a login is needed the scraper waits up to 600 s for `c_user`; just log in (2FA included) in the window it opened and the run continues.

### Scatter chart (`generate_scatter_html`)

Profile-agnostic — adapts to whatever models the active profile's CSV contains. The pipeline:

This function is the **read-time authority on what charts**. Nothing it filters is removed from the CSV; every exclusion below is a display decision.

1. **Profile filter** — keep rows whose `make` is in `{u.make.capitalize() for u in cfg.search_units}` and (if set) `province == cfg.filters.province`.
2. **Non-car filter** — drop rows whose `title` satisfies `title_is_non_car()` (imported from `fb_scraper`), and log the count. **Call the helper, never `_FB_NON_CAR_TITLE_RX` directly** — it consults a second, make-scoped pattern for tokens that are real car names on their own (`Rebel` is a Ram trim, `Shadow` a Dodge, a bare `GSX` the AWD Mitsubishi Eclipse), and the bare regex silently misses those. It guards its input with `isinstance`, so a `title` column maps as-is; **that guard is load-bearing, not defensive noise** — a blank title arrives from `read_csv` as a float `nan`, which is *truthy*, so the earlier `title or ""` idiom passed it straight to `re.search` and raised `TypeError`. The scrape-time filter only rejects new cards; it cannot reach the motorcycles collected before it existed. Measured 2026-08-11: 13 such rows in Émile's CSV survived the make filter — all Suzuki or Honda, the two makes that sell cars and bikes under one name, so no make-level rule can catch them.
3. **Model-name normalisation** — `model` is upper-cased and stripped **once**, before anything groups by it. Source casing varies (`SANTA FE`/`Santa Fe`, `CR-V`/`Cr-v`, `OUTBACK`/`Outback`), which used to split one model across two datasets, two legend rows and two shapes — 17 measured collisions. Datasets, legend entries, datalabels and shape assignment all key off the normalised value, so the legend reads in caps.
4. **Outlier filter** — `_iqr_fences()` computes Tukey 3×IQR fences on `price_cad` and `mileage_km`. Rows outside both fences are dropped. **Use 3× (extreme outlier), not the textbook 1.5×** — on small samples (~50 listings) the 1.5× fence cuts into legitimate budget-end deals (e.g. a $4999 2011 Mazda Tribute is *not* an outlier in a $3-9k market). 3× catches only true junk like a $5 broken-car listing or a $32k Subaru that slipped past the URL filter.
5. **Axis bounds are computed in the browser**, by `computeAxisBounds()` in the inline JS: `[data_min - 4% pad, data_max + 4% pad]` over the points currently passing the user's filters, recomputed on every filter change. `cfg.html.chart_price_floor` / `chart_price_max` ship into the page as `PRICE_FLOOR` / `PRICE_MAX` and clamp the **price** axis there; `null` (the default in both profiles) means pure auto-scale. **They have to travel to the JS to do anything** — computing clamped bounds in Python left them affecting nothing, since the rendered chart never read those values.
6. **Dynamic per-model datasets** — every unique normalised `model` gets its own Chart.js dataset, ordered by frequency. Legend checkboxes and the SVG icons next to them are generated server-side in Python by `_shape_svg()` so they always match what's drawn on the canvas.
7. **Shape encodes MAKE, not model.** Makes are ordered by frequency and assigned `_SCATTER_SHAPE_POOL[i % 10]`; every model of a make inherits its make's glyph. Émile's chart carries ~124 models against a 10-glyph pool, so a per-model shape repeated ~12 times over and encoded nothing; ~12-13 makes fit the pool nearly one-to-one. Datasets and legend rows stay per-model — only the glyph is shared.
8. **Line-shape rendering** — Chart.js draws `cross`, `crossRot`, `dash`, `line`, and `star` as stroke-only (no fill); they render at 0px without `pointBorderWidth`. The list lives in `_SCATTER_LINE_SHAPES` and is shipped to the JS as a `LINE_SHAPES` Set. Any point whose (make-derived) shape is stroked gets `pointBorderWidth: 2.5` and `pointRadius + 1` so its glyph reads at the same visual weight as a filled shape. **If a Chart.js update adds a new stroke-only point style, append it to `_SCATTER_LINE_SHAPES` — that's the only thing to change.**

Color encodes scrape freshness (gold = latest run, green = today/yesterday, gray = older). Shape encodes make. The two axes are price (y) vs mileage (x).

## Stop conditions

**One knob expresses intent: `limits.<source>.max_new_listings` in the profile, overridable by `--limit` (which applies to BOTH sources).** `null` means "everything the source serves". Everything else is an anti-runaway safety valve, kept out of the YAML on purpose — a hardcoded 40-scroll cap once silently turned an explicit `--limit 1000` into ~435 listings, and that must not be able to happen again.

### Facebook

*Window (before any scrolling):* `daysSinceListed`, from `compute_days_since_listed()` — time since last scrape + 2 days overlap, capped at `facebook.defaults.days_since_listed`. Cold start opens the full window. `--days` overrides. A small `days=` in the log after a recent scrape is correct, not a bug.

**The window only advances when the run actually saw the feed.** `_scrape_facebook()` writes `.fb_scrape_state.<profile>.json` only if at least one query traced at least one card; otherwise it logs and leaves the timestamp alone. Stamping it on a graceful early exit narrows the *next* run's window for a scrape that never happened, and the loss compounds silently across runs. `scrape_vehicle_facebook()` returns nothing, so the signal is the card-trace line count (`_fb_cards_traced()`) — if that function's tracing ever changes, this check needs rechecking.

Evaluated in this order inside the scroll loop:

| # | condition | value | kind |
|---|---|---|---|
| 1 | `max_listings` reached | `--limit` / YAML | **intent** |
| 2 | consecutive rejects | `FB_MAX_CONSECUTIVE_REJECTS` = 150 | valve |
| 3 | feed quiet (no unseen card) | `FB_NO_NEW_BACKOFF_SECS` ladder 4→8→15→25→40→**300**s | valve |
| 4 | scroll budget exhausted | `_scroll_budget(max_listings)` | valve |

1 and 2 are checked both per-card and per-batch; 3 and 4 only between scrolls. **Valve 3 measures TIME, not iterations** — counting fast scrolls cannot tell "the feed ended" from "FB has not answered yet". A 4-iteration version (~16s of patience) ended a run at 24 cards on a feed that held 360+ when re-measured next morning. **Why that batch was slow is unproven** — the delete-sold pass had just run, but at 13.3s per listing that is a gentle rate, so throttling is a hypothesis, not a finding; `FB_POST_SWEEP_COOLDOWN_SECS` is precautionary. The patience fix stands regardless of the cause. Any counter of "tries" is suspect here: four fast looks in 16s are one moment observed four times, not four times the information. The run ends only after the **final 5-minute rung** also comes back empty (~6.5 min of total quiet, paid once at the very end), and the log says so explicitly — listing every wait and stating this is the end of the feed, not a timeout. `_scroll_budget()` **derives** from the target (`20 + ceil(target / 4)`, capped at `FB_MAX_SCROLLS_ABSOLUTE` = 500) at a deliberately pessimistic 4 keeps/scroll against ~11 measured — so it always keeps ~3x headroom and never binds before the real conditions. The detail loop has no stop of its own; it only drops individual listings.

### AutoTrader

*Window:* the search URL (`year_min`, `price_min/max`, `radius_km`, `postal_code`, `extra_params`) — enforced server-side.

Pagination: stop at `numberOfPages` from `__NEXT_DATA__` (normal path); otherwise after `AT_MAX_PAGES_WITHOUT_NEW` = 3 consecutive pages adding no unseen URL, with `AT_MAX_PAGES_ABSOLUTE` = 200 as a hard ceiling. **The old fallback broke only when a page had neither new links nor any links, so a search re-serving the same page looped forever** — reachable whenever the metadata is missing.

Detail: capped by `limits.autotrader.max_new_listings` / `--limit` (`None` = all). Incremental skip is by URL already in the CSV.

*Cost:* `LISTING_PAUSE_SECS` = 20 per listing (4x Facebook's 5) and `VEHICLE_PAUSE_SECS` = 45 between makes — ~10 min of pure pauses across 13 makes. This, not the scraping, is why a full AutoTrader pass takes ~22 min.

### Retiring sold listings

Facebook's feed cannot retire a listing for us: a search only shows what is currently for sale, so a sold car just stops appearing — and the deletion health guard rightly refuses to infer that from a partial scrape (with ~680 active rows and a 2-day window, the 50%-re-found threshold is never met). `delete_sold_listings()` re-opens each active FB listing instead. **It runs by default AFTER every Facebook scrape** (since 2026-08-12), so the scroll pass's `last_scrape_timestamp` stamps count as free proofs of life, and it only re-opens rows with no sign of life in `facebook.defaults.sweep_recheck_days` days (default 3; 0 = every active row). Skipping can only delay a flag by at most that window, never create a false one. `--no-delete-sold` skips the sweep entirely; `--full-delete-sweep` forces a complete pass over every active row. It flags `is_deleted` and never drops rows. `save_every` is a disk-flush interval, not a sampling rate: every candidate is checked, and the periodic write only means an interrupted hour-long pass keeps its work.

**Detection, verified 2026-08-10:** a dead listing redirects to `.../marketplace/<city>/?unavailable_product=1` and renders "This listing isn't available anymore". Two traps:
- The `is_sold` / `is_live` JSON flags on that page describe the **"Today's picks" sidebar**, not the listing. They read `false`/`true` on a listing that is definitively gone.
- **Absence of death markers is not proof of life.** The redirect takes a beat, and a fixed sleep read a still-redirecting page as live — a silent false negative on the first listing of a sweep, when the browser is coldest. `_listing_is_gone()` polls for positive evidence on *either* side and returns `None` (leave active) when neither appears.

**AutoTrader does not need this.** Its per-unit search re-scan already retires vanished listings via `scraped_url_set` plus `MIN_URLS_FOR_DELETION` — measured 2026-08-10: 254 of 260 active AT rows refreshed on the last run, 57 already flagged, versus 679 FB rows with zero ever flagged.

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
- `limits` — `autotrader.max_new_listings` / `facebook.max_new_listings`: the only stop knob (see **Stop conditions**). `null` = uncapped; `--limit` overrides both.
- `facebook.defaults` and `facebook.queries` — FB-specific overrides; each query supports `make`, `model_canonical`, `query`, `regex`, `year_range`.

Environmental constants still hardcoded:
- Chrome binary: `/Applications/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing`. Chrome for Testing must be installed locally.
- `LISTING_PAUSE_SECS = 20` between AutoTrader detail extractions (jittered ±5s), `VEHICLE_PAUSE_SECS = 45` between makes. See **Stop conditions → AutoTrader → Cost**.

## Known issues / gotchas

- **AT search returns Ontario dealers despite `prx=300`.** Listing URLs don't contain `/ontario/` so the URL-level filter misses them; the QC province filter catches them as they are merged in (it only screens *new* rows — see **CSV lifecycle**) but a lot of work is wasted parsing them. See `investigation_notes.md`.
- **`rcp=100` is silently ignored.** Each search page returns ~20 URLs regardless. We paginate via `&page=N` instead — `get_listing_urls()` reads `__NEXT_DATA__.props.pageProps.numberOfPages` on page 1 and loops until it hits the last page. Pages stay on the Next.js app (the long-standing "page=N flips back to Angular" claim was either transient or fixed upstream — verified across pages 1, 2, 3, 5, 10, 11 of an Émile Subaru search on 2026-05-04). When the metadata is missing (Angular session), the loop falls back to "stop when a page returns no new URLs".
- **`is_deleted` is a timestamp, not a boolean.** Any analysis script must use `notna()` / `isna()`.
- **Run logs are best-effort.** `setup_run_log()` tees stdout/stderr to `logs/`; failures there are swallowed so logging never breaks a scrape.
- **Chart.js stroke-only point styles render at 0px without `pointBorderWidth`.** `cross`, `crossRot`, `dash`, `line`, and `star` are line-only — they have no fill. If you add a new shape to `_SCATTER_SHAPE_POOL` and forget to also add it to `_SCATTER_LINE_SHAPES` (when applicable), points using it will appear as a label hovering above empty space. We've hit this multiple times — the canonical list lives in `bmw_x3_scraper.py` and ships into the inline JS as `LINE_SHAPES`.
- **Tukey 1.5×IQR is too aggressive on small samples.** The scatter outlier filter uses 3×IQR ("extreme outlier") instead. With ~50 listings, 1.5× cuts into legitimate budget-end deals and high-mileage real cars. 3× catches only true junk (a $5 broken-car listing, a $32k Subaru in Camille's $15k search). See `_iqr_fences()` in `bmw_x3_scraper.py`.
- **FB stopped *rendering* mileage and category on search cards** (2026-08-06) — but still *sends* both. Card `innerText` is now only badge / price / title / location, which silently made `reject_reason()`'s mileage gate reject 100% of cards (`421 non-matching, 0 passing`). The data is alive in the JSON: `custom_sub_titles_with_rendering_flags:[{"subtitle":"149K km"}]` and `marketplace_listing_category_id`. `harvest_card_json()` recovers both — see the Facebook path section. **The lesson: when FB "removes" a field, check the JSON before rewriting parsers.** `innerText` only sees rendered text; `page_source` and the GraphQL responses see everything.
- **`ad_id` round-trips through float64 and gets corrupted.** pandas reads the column as float, so `astype(str)` yields `'1588888658875172.0'`, and past 2^53 it degrades to `'3.56e+16'` with trailing digits **genuinely lost** (12 of 337 rows were wrong by 1-2 digits). Card ids are plain strings, so `seen_ad_ids` never matched and every run re-fetched the same listings. Read with `dtype={"ad_id": str}` and normalise via `_ad_id_str()`. Corrupted rows are recoverable from the `url` column, which carries the true id.
- **Per-model `seen_ad_ids` scoping breaks generic FB queries.** The mask compared `existing_df["model"] == model_canonical`; for a generic query (`model_canonical: null`, e.g. Émile's `awd`) that is `== None`, which pandas evaluates False everywhere. Worse, generic queries *do* fill `model` from the title, so even `isna()` would not match. Scope by `source == "facebook"` alone when `model_canonical is None`.
- **Motorcycles and powersports are indistinguishable from cars in FB's data.** They share the Vehicles category id, carry mileage and a year, and `vehicle_specifications` is all-null for a Ruckus and a car alike — there is no `vehicle_type` field (verified 2026-08-09). Only a title filter works: `_FB_NON_CAR_TITLE_RX`. **Every term must be word-anchored and validated against the CSV before adding** — bare keywords are traps: `sport` hits Kia Sportage, `cross` hits Subaru Crosstrek, `quad` hits a Dodge Ram Quad Cab, and `spyder` would hit a Mitsubishi Eclipse Spyder. Makes that also sell cars (Honda, Suzuki) must be matched by model name, never by make. **The entry point is `title_is_non_car()`, not `_FB_NON_CAR_TITLE_RX`** — a second pattern (`_FB_NON_CAR_SCOPED_RX`) carries the tokens that need a make to disambiguate, and only the helper reads both. The filter runs **twice**: at scrape time (rejects new cards) and at read time in `generate_scatter_html` (keeps already-stored bikes off the chart without deleting them), so widening it retroactively cleans the chart with no re-scrape.
- **A `\b` at the end of an alternation kills every prefix-only branch.** `gsx-?[rs]` matched "GSX-S" inside "GSX-S1000F" and then died on the `1`, so three Suzuki motorcycles charted as cars. The same trap silently disabled `crf`, `fz`, `sv`, `dr`, `yzf` and `cbr` against CRF250L, FZ6R, SV650S, DR650SE, YZF-R6 and CBR600RR. Fix is `\w*` before the closing boundary. **None of those six were in the CSVs, so no measurement against stored titles would have found them** — when a term is a model-name prefix, test it against the longer variants by hand.
- **`collapse_cross_source_duplicates()` keys on year+mileage+price; make/model may be null.** Requiring make/model exempted exactly the rows most likely to be reposts — a title like "2009 Pontiac Vibe" yields no make for a Japanese/Korean profile — so one BMW sat in the CSV three times. An exact odometer match at the same year and price is the same car.
- **THE BIG ONE: Chrome pauses background windows, which silently kills the infinite scroll.** Hidden or occluded windows get timers throttled to ~1 tick/minute, `requestAnimationFrame` halted and `IntersectionObserver` callbacks stopped — and Marketplace's infinite scroll depends on exactly those to request the next batch. `scrollTo()` still runs; nothing is listening. The feed then looks *identical* to an exhausted one. Measured 2026-08-11: a run sat dead for 92s at 2 kept listings, then **resumed the instant the window was brought to the foreground**, reaching 50 within a minute. This one behaviour caused every "feed exhausted at 24/264 cards" mystery in this codebase, and sent us chasing bot-detection and rate-limiting theories that were all wrong. `_launch_real_chrome()` now passes `--disable-background-timer-throttling`, `--disable-backgrounding-occluded-windows` and `--disable-renderer-backgrounding`, and the loop logs `document.visibilityState` whenever a scroll comes back dry. **Caveat: `--keep-browser` makes the next run ATTACH to the existing Chrome, so a browser launched without these flags keeps the bug alive — close it between runs.**
- **Never stop scrolling on `document.body.scrollHeight`.** FB's next batch often lands later than the scroll pause, so the height sits still for a beat while results are already in flight; the old "2 identical heights = feed exhausted" rule ended a run at **24 cards when that feed held 456+ (438 unseen)**. The loop now stops only after 4 consecutive scrolls surface no unseen card id — the signal that actually means exhausted. Symptom to watch for in a log: a low card count plus `KNOWN_SKIP: 0` despite a populated `seen_ad_ids`, since a premature stop never reaches the already-known listings deeper in the feed.
- **The FB junk guard counts CONSECUTIVE rejects, not cumulative ones** (`FB_MAX_CONSECUTIVE_REJECTS`, internal).
- **A page carries MANY GraphQL connections — never read a flag without scoping it.** `has_next_page` appears in comments, suggestions, notification and feed connections alike. Taking the last match in `page_source` read a stranger's flag and ended a `--limit 10000` run after **17 listings** (2026-08-11): the SSR HTML held exactly 2 flags, the last was `false`, and **neither belonged to the Marketplace feed**. Only the feed's `end_cursor` carries a `pg` field, so `_feed_has_next_page()` keys on that (`_FEED_CURSOR_MARKER`). The same trap already bit us once with `is_sold`/`is_live` on detail pages, which describe the "Today's picks" sidebar rather than the listing being viewed. **When a JSON field could plausibly belong to more than one object on the page, find its discriminator before trusting it.**
- **`has_next_page=None` means "no opinion", never "stop".** A missing verdict falls through to the patience ladder; only an explicit `false` from the feed ends a run immediately. Getting this backwards turns any parsing miss into silent data loss.
- **`fb_card_trace.log` names the gate that ate everything.** One `grep` on its reason tags localised the 100%-rejection bug immediately (421 cards, all rejected by the mileage gate — the tag is now `FILTER:unparseable-mileage`). Read it first when FB output goes empty. Caveat: `trace_card()` binds its path as a default argument, so monkey-patching `FB_CARD_TRACE_FILE` at runtime does **not** redirect it — separate runs by timestamp instead. The file is also load-bearing beyond debugging: `_fb_cards_traced()` in `bmw_x3_scraper.py` counts its `[HH:MM:SS] …` card lines to decide whether the feed served anything at all (see the FB window note above).
- **`migrate_add_columns.py` was deleted** (2026-08-11) — an already-applied one-off that rewrote the CSV in place. Don't go looking for it, and don't recreate it: schema changes belong in a script that is written, run once, and removed, not left lying next to the data it can destroy.
- **`GitHub push failed (non-fatal)` on an empty scrape is benign.** `git commit` is run with `check=True`, and git exits 1 when nothing is staged — which is what happens when the scatter HTML is regenerated byte-identical. It resolves on its own as soon as a run actually changes data.
