"""Pre-fetch World Bank CPI rates and store them in [ras_procurement].[cpi_rates].

Run this ONCE from a machine that has internet access to api.worldbank.org.
The server running the benchmark does not need internet after this.

Usage:
    python fetch_cpi_rates.py                        # default: 2015-2026, all countries in DB
    python fetch_cpi_rates.py --start 2010 --end 2026
    python fetch_cpi_rates.py --countries IN,US,DE,GB --start 2015 --end 2026
"""

from __future__ import annotations

import argparse
import sys
from dotenv import load_dotenv

load_dotenv()

from utils.config import AppConfig
from utils.cpi_inflation import resolve_country_code, _fetch_api_rates, _write_db_cache
from db.crud import BaseRepository
from db.tables import AzureTables

import pyodbc
from loguru import logger


def _get_countries_from_db(conn_str: str) -> list[str]:
    """Return distinct ISO alpha-2 codes from supplier_country values in quotation_extracted_items."""
    rows = BaseRepository(conn_str)._fetch(
        f"SELECT DISTINCT [supplier_country] "
        f"FROM {AzureTables.QUOTATION_EXTRACTED_ITEMS} "
        f"WHERE [supplier_country] IS NOT NULL AND LEN([supplier_country]) > 0"
    )
    codes = []
    seen = set()
    for (raw,) in rows:
        code = resolve_country_code(raw)
        if code and code not in seen:
            codes.append(code)
            seen.add(code)
            logger.info("  {} → {}", raw, code)
    return codes


def main() -> None:
    parser = argparse.ArgumentParser(description="Pre-fetch World Bank CPI rates into Azure SQL.")
    parser.add_argument("--start",     type=int, default=2015, help="Start year (default: 2015)")
    parser.add_argument("--end",       type=int, default=2026, help="End year   (default: 2026)")
    parser.add_argument("--countries", type=str, default=None,
                        help="Comma-separated ISO alpha-2 codes, e.g. IN,US,DE  "
                             "(default: all countries found in quotation_extracted_items)")
    args = parser.parse_args()

    config   = AppConfig()
    conn_str = config.get_azure_conn_str()

    if args.countries:
        codes = [c.strip().upper() for c in args.countries.split(",") if c.strip()]
        logger.info("Using {} country code(s) from --countries flag", len(codes))
    else:
        logger.info("Resolving countries from quotation_extracted_items ...")
        codes = _get_countries_from_db(conn_str)
        logger.info("Found {} distinct country code(s) in DB", len(codes))

    if not codes:
        logger.warning("No countries to fetch — exiting.")
        sys.exit(0)

    total_saved = 0
    for code in codes:
        logger.info("Fetching CPI for {} ({}-{}) ...", code, args.start, args.end)
        rates = _fetch_api_rates(code, args.start - 1, args.end)
        if not rates:
            logger.warning("  No data returned for {}", code)
            continue
        _write_db_cache(conn_str, code, rates)
        logger.info("  Saved {} year(s) for {}", len(rates), code)
        total_saved += len(rates)

    logger.info("Done — {} rate(s) stored in [cpi_rates] for {} country/countries.",
                total_saved, len(codes))


if __name__ == "__main__":
    main()
