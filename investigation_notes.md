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
