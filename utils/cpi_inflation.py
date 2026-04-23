"""World Bank CPI-based cumulative inflation calculator.

Used by the price benchmark module to compute real-world inflation between
the date of a historical reference quote and the current PR's date.

World Bank indicator: FP.CPI.TOTL.ZG  (Inflation, consumer prices — annual %)
API docs: https://datahelpdesk.worldbank.org/knowledgebase/articles/898590

CPI rates are cached in [ras_procurement].[cpi_rates] (Azure SQL).
Run `python fetch_cpi_rates.py` once from a machine with internet access to
pre-populate the cache.  If the cache has all needed years the World Bank API
is never called.  On a cache miss the API is tried and, if it succeeds, the
new rates are written back to the cache automatically.

Returns None (gracefully) when:
  - Country string cannot be resolved to an ISO code
  - No data in cache and World Bank API unreachable / returns no data
  - start_year > end_year (invalid range)
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

_WB_URL          = "https://api.worldbank.org/v2/country/{code}/indicator/FP.CPI.TOTL.ZG"
_REQUEST_TIMEOUT = 30   # seconds
_CPI_RATES_TABLE = "[ras_procurement].[cpi_rates]"


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


# ── DB cache helpers ───────────────────────────────────────────────────────────

def _read_db_cache(conn_str: str, country_code: str, start_year: int, end_year: int) -> dict[int, float]:
    """Read CPI rates from the Azure SQL cache table."""
    try:
        import pyodbc
        conn = pyodbc.connect(conn_str, autocommit=True)
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT [year], [rate_pct] FROM {_CPI_RATES_TABLE} "
                f"WHERE [country_code] = ? AND [year] BETWEEN ? AND ?",
                country_code, start_year + 1, end_year,
            )
            return {int(row[0]): float(row[1]) for row in cursor.fetchall()}
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("CPI DB cache read failed: {}", exc)
        return {}


def _write_db_cache(conn_str: str, country_code: str, rates: dict[int, float]) -> None:
    """Persist API-fetched CPI rates back to the Azure SQL cache."""
    if not rates:
        return
    try:
        import pyodbc
        conn = pyodbc.connect(conn_str, autocommit=False)
        try:
            cursor = conn.cursor()
            for year, rate in rates.items():
                cursor.execute(
                    f"MERGE {_CPI_RATES_TABLE} AS t "
                    f"USING (SELECT ? AS c, ? AS y) AS s ON t.[country_code]=s.c AND t.[year]=s.y "
                    f"WHEN MATCHED THEN UPDATE SET [rate_pct]=?, [fetched_at]=SYSUTCDATETIME() "
                    f"WHEN NOT MATCHED THEN INSERT ([country_code],[year],[rate_pct]) VALUES (?,?,?);",
                    country_code, year, rate, country_code, year, rate,
                )
            conn.commit()
            logger.debug("CPI cache: saved {} rate(s) for {}", len(rates), country_code)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("CPI DB cache write failed: {}", exc)


# ── World Bank API fetch ───────────────────────────────────────────────────────

def _fetch_api_rates(country_code: str, start_year: int, end_year: int) -> dict[int, float]:
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
        logger.warning("CPI API fetch failed for country={!r} {}-{}: {}", country_code, start_year, end_year, exc)
        return {}


def _fetch_cpi_rates(
    country_code: str,
    start_year: int,
    end_year: int,
    conn_str: Optional[str] = None,
) -> dict[int, float]:
    """Return {year: rate_pct} for start_year+1 … end_year.

    Checks DB cache first.  Falls back to World Bank API and writes results
    back to the cache if conn_str is provided.
    """
    years_needed = set(range(start_year + 1, end_year + 1))

    # 1. DB cache
    if conn_str:
        cached = _read_db_cache(conn_str, country_code, start_year, end_year)
        if years_needed.issubset(cached.keys()):
            logger.debug("CPI cache hit for {} {}-{}", country_code, start_year, end_year)
            return cached
    else:
        cached = {}

    # 2. World Bank API
    api_rates = _fetch_api_rates(country_code, start_year, end_year)

    # 3. Write new rates back to cache
    if api_rates and conn_str:
        _write_db_cache(conn_str, country_code, api_rates)

    # Merge: API results take precedence, fill any gaps from cache
    return {**cached, **api_rates}


# ── Main entry point ───────────────────────────────────────────────────────────

def compute_cpi_inflation_pct(
    country_str: Optional[str],
    start_year: int,
    end_year: int,
    conn_str: Optional[str] = None,
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
    conn_str:
        Optional Azure SQL connection string for the DB cache.  When provided,
        rates are read from [cpi_rates] first and the API is only called on a
        cache miss.  API results are written back automatically.

    Returns
    -------
    float or None
        Cumulative percentage change, e.g. 12.34 means prices rose 12.34%.
        0.0 when start_year == end_year.
        None when country is unresolvable, no data available, or start > end.
    """
    if start_year > end_year:
        return None
    if start_year == end_year:
        return 0.0

    if not _DEPS_OK:
        logger.warning("CPI: pycountry/rapidfuzz not installed — run: pip install pycountry rapidfuzz")
        return None

    if not country_str:
        logger.info("CPI: country_str is None/empty — returning None")
        return None

    code = resolve_country_code(country_str)
    if not code:
        logger.info("CPI: could not resolve country={!r} — returning None", country_str)
        return None

    rates = _fetch_cpi_rates(code, start_year, end_year, conn_str=conn_str)
    if not rates:
        logger.info("CPI: no data for country={!r} (code={}) {}-{}",
                    country_str, code, start_year, end_year)
        return None

    factor = 1.0
    for year in range(start_year + 1, end_year + 1):
        if year in rates:
            factor *= (1 + rates[year] / 100)

    return round((factor - 1) * 100, 4)
