"""World Bank CPI-based cumulative inflation calculator.

Used by the price benchmark module to compute real-world inflation between
the date of a historical reference quote and the current PR's date.

World Bank indicator: FP.CPI.TOTL.ZG  (Inflation, consumer prices — annual %)
API docs: https://datahelpdesk.worldbank.org/knowledgebase/articles/898590

Returns None (gracefully) when:
  - Country string cannot be resolved to an ISO code
  - World Bank API returns no data for the date range
  - start_year >= end_year (no period to measure)
  - Network or parse error
"""

from __future__ import annotations

from typing import Optional

import requests
from loguru import logger

try:
    import pycountry
    from rapidfuzz import process as fuzz_process
    _DEPS_OK = True
except ImportError:
    _DEPS_OK = False
    logger.warning(
        "pycountry or rapidfuzz not installed — CPI inflation will not be computed. "
        "Run: pip install pycountry rapidfuzz"
    )

_WB_URL = "https://api.worldbank.org/v2/country/{code}/indicator/FP.CPI.TOTL.ZG"
_REQUEST_TIMEOUT = 10   # seconds


# ── Country resolution ─────────────────────────────────────────────────────────

def resolve_country_code(country_str: Optional[str]) -> Optional[str]:
    """Resolve a country name / alpha-2 / alpha-3 string to an ISO 3166-1 alpha-2 code.

    Uses exact match first, then rapidfuzz fuzzy match (threshold > 75).
    Returns None if unresolvable or dependencies are missing.
    """
    if not _DEPS_OK or not country_str:
        return None

    text = country_str.strip().lower()

    for country in pycountry.countries:
        if text in (
            country.name.lower(),
            getattr(country, "official_name", "").lower(),
            country.alpha_2.lower(),
            country.alpha_3.lower(),
        ):
            return country.alpha_2

    names = [c.name for c in pycountry.countries]
    result = fuzz_process.extractOne(text, names)
    if result and result[1] > 75:
        matched = pycountry.countries.get(name=result[0])
        if matched:
            return matched.alpha_2

    return None


# ── World Bank data fetch ──────────────────────────────────────────────────────

def _fetch_cpi_rates(country_code: str, start_year: int, end_year: int) -> dict[int, float]:
    """Fetch annual CPI inflation rates from World Bank API.

    Returns {year: inflation_rate_pct} dict, empty on any failure.
    """
    url = _WB_URL.format(code=country_code)
    try:
        resp = requests.get(
            url,
            params={"date": f"{start_year}:{end_year}", "format": "json", "per_page": 100},
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if len(data) < 2 or not data[1]:
            return {}
        return {
            int(entry["date"]): float(entry["value"])
            for entry in data[1]
            if entry.get("value") is not None
        }
    except Exception as exc:
        logger.warning("CPI fetch failed for country={!r} {}-{}: {}", country_code, start_year, end_year, exc)
        return {}


# ── Main entry point ───────────────────────────────────────────────────────────

def compute_cpi_inflation_pct(
    country_str: Optional[str],
    start_year: int,
    end_year: int,
) -> Optional[float]:
    """Return cumulative CPI inflation % from start_year to end_year for a country.

    Parameters
    ----------
    country_str:
        Country name or ISO code (e.g. "India", "IN", "IND").
    start_year:
        Year of the historical reference quote (low_hist PR's C_DATETIME year).
    end_year:
        Year of the current PR being benchmarked (current PR's C_DATETIME year).

    Returns
    -------
    float or None
        Cumulative percentage change, e.g. 12.34 means prices rose 12.34%.
        None when country is unresolvable, no WB data exists, or start >= end.
    """
    if start_year >= end_year:
        return None

    code = resolve_country_code(country_str)
    if not code:
        logger.debug("CPI: could not resolve country={!r} — skipping", country_str)
        return None

    rates = _fetch_cpi_rates(code, start_year, end_year)
    if not rates:
        return None

    factor = 1.0
    for year in range(start_year + 1, end_year + 1):
        if year in rates:
            factor *= (1 + rates[year] / 100)

    return round((factor - 1) * 100, 4)
