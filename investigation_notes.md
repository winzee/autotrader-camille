# AutoTrader A/B Routing & prx Param Investigation

## Context

AutoTrader.ca A/B-routes sessions between two apps:
- **Next.js** (post AutoScout24 acquisition) — uses `/offers/` URLs, exposes `window.__NEXT_DATA__`. This is what the scraper needs.
- **Angular (legacy)** — uses `/a/` URLs. Scraper's `/offers/` selector returns 0 links.

Open questions:
1. Does the `prx` (radius) param correlate with Angular vs Next.js routing?
2. Is `prx` actually honored by the Next.js platform?
3. Why did page 1 only return 10 URLs this morning despite `rcp=100`?
4. Why does `&page=2` fail to load any listings?

## Environment

- Chrome for Testing 130.0.6723.69
- Fresh temp profile per attempt (no `--user-data-dir`) → no cookie carryover
- Hardcoded User-Agent `Chrome/130.0.0.0`
- Same client IP across all runs
- Postal code H1X 3J1 (Montréal)

## Experiments

### Exp 1 — This morning's scraper run (prx=300)

- Next.js detected on attempt 1
- Page 1: **10 URLs** found (expected ~100 with `rcp=100`)
- Page 2 (`&page=2`): 0 listings, 2 retries failed
- **Finding:** Next.js likely ignores `rcp=100`; `&page=2` pagination either fails or flips back to Angular

### Exp 2 — investigation_script.py with prx=200, 10 attempts

- 10:58–10:02 AM
- **10/10 attempts landed on Angular app**
- Script exited before reaching the comparison logic
- **Finding:** High probability of Angular routing with prx=200 at this time

### Exp 3 — Multi-variant test (no_prx, prx_200, prx_100, prx_50)

- 10:06 AM — first attempt crashed on page-load `TimeoutException`, hardened script

### Exp 4 — Hardened multi-variant rerun, 4 attempts each

10:08–10:13 AM, 16 attempts total, 0 errors.

| Variant  | Next.js hits | /a/ links (Angular) |
|----------|:---:|---|
| no_prx   | 0/4 | 200 × 4 |
| prx=200  | 0/4 | 218 × 4 |
| prx=100  | 0/4 | 206 × 4 |
| prx=50   | 0/4 | 200 × 4 |

**Findings:**
- **All 16 attempts → Angular.** Zero Next.js. A/B routing has completely shifted toward Angular since this morning (when attempt 1 hit Next.js with prx=300).
- **prx IS honored by Angular.** Different values yield different result counts: prx=200 → 218, prx=100 → 206, no_prx → 200, prx=50 → 200. (prx=50 ≤ default radius, so matches no_prx.)
- **prx does NOT affect A/B routing.** no_prx also lands on Angular — so prx presence isn't pushing us away from Next.js.
- **`/a/` counts are perfectly deterministic per variant** (218×4, 206×4, etc.) — no randomness in Angular response.

## Cumulative tally

| Variant  | Next.js hits | Attempts | Rate |
|----------|:---:|:---:|---|
| prx=300  | 1 | 1  | one morning hit |
| prx=200  | 0 | 15 | — |
| no_prx   | 0 | 4  | — |
| prx=100  | 0 | 4  | — |
| prx=50   | 0 | 4  | — |

## Hypotheses (updated)

- **H1 — Time-windowed A/B rollout.** Best explanation: morning = Next.js possible, by ~10 AM ET = 100% Angular. Try again later today or tomorrow morning.
- **H2 — prx pushes to Angular.** **Falsified** — no_prx also lands on Angular.
- **H3 — IP pinning.** **Falsified** — got Next.js once, Angular 15× from same IP.
- **H4 — UA / fingerprint.** Not yet tested. Would need UA randomization.
- **H5 — AutoTrader killed the Next.js A/B entirely.** Can't rule out; would explain the step-change.

## Next steps

1. Wait an hour or two, re-run the sweep — tests H1.
2. Try tomorrow morning ~same time as this morning's successful scrape — tests H1 more precisely.
3. Try from phone hotspot (different IP) — sanity check, though H3 is already falsified.
4. Add UA randomization to the script — tests H4.
5. If Next.js reappears, probe `&page=2` to confirm pagination flips back to Angular.

### Exp 5 — Real browser test via Claude-in-Chrome (10:45 AM)

Loaded search page in user's actual Chrome (real cookies, real history, real fingerprint, same IP):
- Search page: **Angular** — `hasNextData: false`, `/a/` links = 218, title "156 used & certified ..."
- **Falsifies H4** (UA/fingerprint) — real Chrome still gets Angular.
- **Strengthens H5** — this looks like AutoTrader is serving Angular to everyone for search right now.

Then clicked into a `/a/` Forester detail page in same real Chrome:
- URL: `/a/subaru/forester/saint-hubert/quebec/5_69810816_ct2004322104822253/`
- `hasNextData: false`, `hasNgVdpModel: true`
- `ngVdpModel` keys include all the fields the scraper needs:
  `adBasicInfo, hero, conditionAnalysis, priceAnalysis, specifications, dealerTrust, description, fuelEconomy, gallery, carfax, highlights, ...`

## 🎯 Key realization

**We do NOT need the Next.js app.** Angular `/a/` pages are fully scrapeable via `window.ngVdpModel` — the exact data structure `gu.json` documents and that the scraper used before commit `e97c882` ("Migrate scraper from ngVdpModel to __NEXT_DATA__").

The scraper's comment claiming "Angular can no longer be scraped" is incorrect/outdated.

### Recommended fix

Instead of retrying until Next.js, the scraper should:
1. Accept whichever app it lands on.
2. For Angular: collect `/a/` URLs on search, parse `ngVdpModel` on detail pages.
3. For Next.js: collect `/offers/` URLs on search, parse `__NEXT_DATA__` on detail pages.
4. Merge both paths into the same `VehicleListing` output.

The `parse_ngvdp_model()` code path likely still exists in git history (pre-`e97c882`) and can be resurrected.

---

# 2026-04-29 — Undercoverage diagnosis & dual-path fix

## Symptom

User reported "scrape is returning less and less data" over the prior days. Spot-check of `used_suv_listings.csv` showed:

- Today's scrape touched ~48 of the 211 non-deleted AT rows; CR-V alone had 65 live listings on AT but the CSV held only 26.
- Toyota RAV4 had been getting 0 results for 5 of the last 6 scrapes despite 27 live listings on AT.
- **233 of 260 rows had `is_deleted` set** — almost certainly mostly false positives. (Earlier analysis missed this because the column is checked as a timestamp, not a boolean — see CLAUDE.md gotchas.)

## Root causes (3 compounding bugs)

1. **URL collector hard-coded to Next.js URLs.** `_collect_page_links()` used a single selector `a[href*='/offers/']`. AT has been routing 100% to Angular since at least 04-15 (see Exp 4 / Exp 5 above). On Angular sessions the selector found 0 listing URLs, so the scrape did nothing. The rare Next.js sessions kept the CSV from going completely stale.
2. **Detail-extraction dispatcher never called the Angular parser.** `extract_listing_details()` only tried `__NEXT_DATA__`, even though `parse_ngvdp_model()` was fully implemented and the docstring claimed both tiers existed. Even if Angular URLs had been collected, detail scraping would have failed.
3. **Slug matcher couldn't handle digit-suffixed models.** `vehicle_mask` and the URL post-filter compared against `toyota-rav4`, but real RAV4 URLs are spelled `toyota-rav-4-…`. RAV4 listings were therefore never matched by the deletion logic, never updated by `last_scrape_timestamp`, and never resurrectable.

## Deletion-logic gaps

- Existing safeguard refused to mark deletions only when the scrape returned **literally zero** URLs. A scrape returning 1 URL out of 25 expected would still mark the other 24 as deleted.
- No resurrection path: once `is_deleted` was set, it never cleared, even if the same URL reappeared in a later scrape.

## Fixes applied (`bmw_x3_scraper.py`)

- `LISTING_SELECTORS` now collects both `a[href*='/offers/']` and `a[href*='/a/']`.
- `extract_listing_details()` gained Tier 1b: `window.ngVdpModel` → `parse_ngvdp_model()`.
- New `_make_model_url_patterns(make_model)` generates both URL flavours plus the letter-digit hyphenated alt spelling (`rav4` ↔ `rav-4`). Used by both the URL post-filter and the `vehicle_mask` regex.
- Deletion now requires a **healthy scrape**: ≥5 URLs scraped AND (≥50% of expected re-found OR fewer than 3 baseline rows).
- **Resurrection**: any URL with `is_deleted` set that reappears in a healthy scrape gets cleared back to `NaN`.
- Per-run logs added under `logs/` via `setup_run_log()` (tees stdout+stderr).

## End-to-end verification

Full scrape run 12:59–13:15 (logged at `logs/run_2026-04-29_125936.log`):

- All 7 AT vehicles completed cleanly. URL counts per vehicle: 19–20 each.
- **RAV4: 20 URLs (was 0)**. Slug fix confirmed.
- **38 false-positive deletions resurrected** across the 7 vehicles. Distribution: Forester 5, Outback 6, Crosstrek 7, RAV4 0, HR-V 6, CR-V 4, Kona 10.
- Health-based safeguard correctly fired on RAV4 (re-found 2 of 6 expected) and skipped deletion marking.
- Active row count: 27 → 65. Total rows: 260 → 261.

## Newly visible / still-open issues

These were masked by the bugs above and showed up clearly once the scraper was finding URLs again:

1. **AT search leaks Ontario dealers.** Of 89 new listings extracted in this run, only 2 had `province == "QC"`; the rest were Toronto-area dealers (Vaughan, North York, Burlington, Brampton). They're correctly dropped by the QC filter at write time, but a lot of detail-page work is wasted. URL-level filter on `/ontario/` doesn't catch them because slugs don't contain provinces. **Fix idea:** check `data.location.zip` or `seller.dealer.region` *before* writing, and skip non-QC earlier; or add a city/zip allowlist.
2. **`rcp=100` ignored, pagination unreliable.** Every vehicle returned ~20 URLs regardless of `rcp`. Live AT shows 27–65 listings per vehicle (CR-V 65, RAV4 27). So even on the corrected scraper we're capturing only the first ~20–30%. Combined with #1, the QC subset is even smaller. **Investigation needed:** does Angular respect `rcp`? Does `&page=2` work mid-session? Can we issue separate searches per QC city to bypass the radius/pagination trap?

## Operational notes

- Pre-run CSV backups land at `used_suv_listings.pre_scrape_<timestamp>.csv` when a session-runner makes one — useful for diff-based audits.
- Memory note: `is_deleted` column holds an ISO timestamp string, not a boolean. Any quick `csv` analysis must use `notna()`/`isna()` semantics, not equality with `'true'`.
