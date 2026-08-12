"""Profile-based configuration for the SUV scraper.

Each profile lives in a YAML file (e.g. ``camille.yaml``, ``emile.yaml``) and
is loaded once at startup. Pass ``--config <path>`` to ``bmw_x3_scraper.py``
to pick a profile; default is ``camille.yaml``.

Schema (see ``camille.yaml`` and ``emile.yaml`` for fully-populated examples):

    profile_name: <str>
    output:
        csv: <path>
        scatter_html: <path>
        log_dir: <path>
    html:
        page_title: <str>
        heading: <str>
        public_url: <str | null>
        chart_price_max: <int>      # optional hard ceiling on the price axis
        chart_price_floor: <int>    # optional hard floor on the price axis
    github_pages:
        enabled: <bool>
        repo: <str>                 # required when enabled is true
    filters:
        province: <str | null>      # e.g. "QC"; null disables province filter
    autotrader:
        enabled: <bool>
        search:
            year_min: <int>
            price_min: <int>        # 0 disables
            price_max: <int>
            radius_km: <int>
            postal_code: <str>
            extra_params: <dict>    # e.g. {dtrain: A}; appended verbatim to URL
        search_units:
            - {make: <str>, model: <str | omitted>}
    facebook:
        enabled: <bool>
        defaults:
            price_min: <int>
            price_max: <int>
            days_since_listed: <int>
        queries:
            - query: <str>
              regex: <str>
              model_canonical: <str | null>
              year_range: [<int>, <int>]
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import yaml


@dataclass
class SearchUnit:
    make: str
    model: Optional[str] = None

    @property
    def slug(self) -> str:
        """Identifier used for URL filtering and FB lookup keys."""
        return f"{self.make}/{self.model}" if self.model else self.make


@dataclass
class AutotraderSearch:
    year_min: int
    price_max: int
    radius_km: int
    postal_code: str
    price_min: int = 0
    extra_params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FbQuery:
    query: str
    regex: str
    model_canonical: Optional[str]
    year_range: Tuple[int, int]
    make: Optional[str] = None


@dataclass
class FbDefaults:
    price_min: int
    price_max: int
    days_since_listed: int
    # Delete-sold sweep: only re-open listings with no sign of life in this
    # many days (feed re-finds and past sweep verdicts both count as life).
    # 0 = re-check every active row on every run.
    sweep_recheck_days: int = 3


@dataclass
class HtmlConfig:
    page_title: str
    heading: str
    public_url: Optional[str] = None
    # Optional clamps on the price axis, applied on top of the bounds the
    # chart derives from the plotted points. None / omitted => pure
    # auto-scaling (typical case). Set when you want a hard ceiling/floor
    # regardless of data (e.g. budget cap).
    chart_price_max: Optional[int] = None
    chart_price_floor: Optional[int] = None


@dataclass
class OutputConfig:
    csv: str
    scatter_html: str
    log_dir: str


@dataclass
class GithubPagesConfig:
    enabled: bool
    repo: Optional[str] = None


@dataclass
class FiltersConfig:
    province: Optional[str]
    # Postal-code prefix allowlist applied at search-results time (before
    # fetching detail pages). Each entry matches by .startswith() on the
    # uppercased, space-stripped postal code (e.g. "H1X 3J1" → "H1X3J1").
    # When None or empty, no postal filter is applied.
    allowed_postal_prefixes: Optional[List[str]] = None


@dataclass
class LimitsConfig:
    """How many NEW listings to take per source, per search unit / FB query.

    This is the only stop knob a user should ever set. Everything else in the
    scrape loops (consecutive-reject guards, no-new-page/card detectors, absolute
    iteration caps) is a runaway safety valve, deliberately kept out of the YAML
    so a limit the user asked for can never be silently overridden by one they
    did not — which is exactly what a hardcoded 40-scroll cap did to `--limit`.

    ``None`` means "no cap: take everything the source will serve".
    """
    autotrader_max_new_listings: Optional[int] = None
    facebook_max_new_listings: Optional[int] = None


@dataclass
class Config:
    profile_name: str
    output: OutputConfig
    html: HtmlConfig
    github_pages: GithubPagesConfig
    filters: FiltersConfig
    autotrader_enabled: bool
    autotrader_search: AutotraderSearch
    search_units: List[SearchUnit]
    facebook_enabled: bool
    fb_defaults: FbDefaults
    fb_queries: List[FbQuery]
    limits: LimitsConfig = field(default_factory=LimitsConfig)


def _load_limits(raw: Dict[str, Any]) -> LimitsConfig:
    """Parse the optional top-level ``limits`` block.

    A missing block, a missing source, or an explicit ``null`` all mean "no cap".
    """
    def _one(section: str) -> Optional[int]:
        block = raw.get(section) or {}
        value = block.get("max_new_listings")
        return None if value is None else int(value)

    return LimitsConfig(
        autotrader_max_new_listings=_one("autotrader"),
        facebook_max_new_listings=_one("facebook"),
    )


def load_config(path: str) -> Config:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    out = raw["output"]
    html = raw["html"]
    gh = raw["github_pages"]
    filt = raw.get("filters") or {}
    at = raw["autotrader"]
    at_search = at["search"]
    fb = raw["facebook"]

    if gh.get("enabled") and not gh.get("repo"):
        raise ValueError(f"{path}: github_pages.enabled=true requires github_pages.repo")

    units = [SearchUnit(make=u["make"], model=u.get("model")) for u in at["search_units"]]

    fb_queries = [
        FbQuery(
            query=q["query"],
            regex=q["regex"],
            model_canonical=q.get("model_canonical"),
            year_range=tuple(q["year_range"]),
            make=q.get("make"),
        )
        for q in fb.get("queries", [])
    ]

    return Config(
        profile_name=raw["profile_name"],
        output=OutputConfig(
            csv=out["csv"],
            scatter_html=out["scatter_html"],
            log_dir=out.get("log_dir", "logs"),
        ),
        html=HtmlConfig(
            page_title=html["page_title"],
            heading=html["heading"],
            public_url=html.get("public_url"),
            chart_price_max=int(html["chart_price_max"]) if html.get("chart_price_max") is not None else None,
            chart_price_floor=int(html["chart_price_floor"]) if html.get("chart_price_floor") is not None else None,
        ),
        github_pages=GithubPagesConfig(
            enabled=bool(gh.get("enabled", False)),
            repo=gh.get("repo"),
        ),
        filters=FiltersConfig(
            province=filt.get("province"),
            allowed_postal_prefixes=[
                str(p).upper().replace(" ", "")
                for p in (filt.get("allowed_postal_prefixes") or [])
            ] or None,
        ),
        autotrader_enabled=bool(at.get("enabled", True)),
        autotrader_search=AutotraderSearch(
            year_min=int(at_search["year_min"]),
            price_min=int(at_search.get("price_min", 0) or 0),
            price_max=int(at_search["price_max"]),
            radius_km=int(at_search["radius_km"]),
            postal_code=at_search["postal_code"],
            extra_params=dict(at_search.get("extra_params") or {}),
        ),
        search_units=units,
        facebook_enabled=bool(fb.get("enabled", True)),
        fb_defaults=FbDefaults(
            price_min=int(fb["defaults"]["price_min"]),
            price_max=int(fb["defaults"]["price_max"]),
            days_since_listed=int(fb["defaults"]["days_since_listed"]),
            sweep_recheck_days=int(fb["defaults"].get("sweep_recheck_days", 3)),
        ),
        fb_queries=fb_queries,
        limits=_load_limits(raw.get("limits") or {}),
    )
