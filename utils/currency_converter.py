"""Currency conversion utility — converts prices to EUR using EXCHANGE_RATE table.

Usage
-----
    from utils.currency_converter import CurrencyConverter

    converter = CurrencyConverter(conn_str)
    eur_price = converter.to_eur(Decimal("100"), "USD", date(2026, 1, 15))
    # → Decimal("91.743...")  or None if rate not found

The converter caches currency-ID lookups and exchange rates in memory for the
lifetime of the instance, so a single instance should be shared across all
items in one processing run to avoid redundant DB round-trips.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Optional

from loguru import logger

from db.crud import BaseRepository
from db.tables import AzureTables

# EUR's CUR_ID in currency_mst — confirmed by user.
_EUR_CUR_ID = 3

# STATUS_ID value that means "active/live" in EXCHANGE_RATE.
# Confirm against live DB: SELECT DISTINCT STATUS_ID FROM [ras_procurement].[EXCHANGE_RATE]
_ACTIVE_STATUS_ID = 1

# Conversion direction — MUST be confirmed against the live DB before deploy.
# Run: SELECT cm.CURRENCY, er.CONVERSION_RATE FROM EXCHANGE_RATE er
#        JOIN currency_mst cm ON cm.CUR_ID = er.CUR_ID
#       WHERE er.BASE_CUR_ID = 3 ORDER BY FROM_DATE DESC
#
# If CONVERSION_RATE for USD is ~0.92  → True  (eur = amount * rate)
# If CONVERSION_RATE for USD is ~1.09  → False (eur = amount / rate)
_RATE_IS_MULTIPLY = True

_LOOKUP_CUR_ID_SQL = f"""
    SELECT [CUR_ID]
    FROM   {AzureTables.CURRENCY_MST}
    WHERE  [CURRENCY] = ?
"""

_LOOKUP_RATE_SQL = f"""
    SELECT TOP 1 [CONVERSION_RATE]
    FROM   {AzureTables.EXCHANGE_RATE}
    WHERE  [CUR_ID]      = ?
      AND  [BASE_CUR_ID] = {_EUR_CUR_ID}
      AND  [STATUS_ID]   = {_ACTIVE_STATUS_ID}
      AND  [FROM_DATE]  <= ?
      AND  [TO_DATE]    >= ?
    ORDER BY [FROM_DATE] DESC
"""


def _apply_rate(amount: Decimal, rate: Decimal) -> Decimal:
    return (amount * rate) if _RATE_IS_MULTIPLY else (amount / rate)


class CurrencyConverter(BaseRepository):
    """Converts amounts to EUR using monthly EXCHANGE_RATE data.

    Parameters
    ----------
    conn_str:
        pyodbc connection string for Azure SQL (ras_procurement schema).
    """

    def __init__(self, conn_str: str) -> None:
        super().__init__(conn_str)
        self._cur_id_cache: dict[str, Optional[int]] = {}
        self._rate_cache: dict[tuple[int, int, int], Optional[Decimal]] = {}

    def to_eur(
        self,
        amount: Optional[Decimal],
        currency_code: Optional[str],
        ref_date: Optional[date],
    ) -> Optional[Decimal]:
        """Convert *amount* in *currency_code* to EUR using the rate on *ref_date*.

        Returns None when:
        - amount is None
        - ref_date is None (no date → cannot select a rate)
        - currency code is unknown in currency_mst
        - no matching rate row found in EXCHANGE_RATE
        - any DB or arithmetic error

        EUR amounts pass through unchanged.
        """
        if amount is None:
            return None
        if ref_date is None:
            return None

        code = (currency_code or "").strip().upper()
        if not code:
            return None
        if code == "EUR":
            return amount

        cur_id = self._get_cur_id(code)
        if cur_id is None:
            logger.warning("CurrencyConverter: unknown currency code={!r}", code)
            return None

        rate = self._get_rate(cur_id, ref_date)
        if rate is None:
            logger.warning(
                "CurrencyConverter: no EUR rate found for currency={!r} on {}",
                code, ref_date,
            )
            return None

        try:
            converted = _apply_rate(amount, rate)
            return converted.quantize(Decimal("0.000001"))
        except Exception as exc:
            logger.warning("CurrencyConverter: conversion error currency={!r}: {}", code, exc)
            return None

    # ── Private helpers ────────────────────────────────────────────────────

    def _get_cur_id(self, currency_code: str) -> Optional[int]:
        if currency_code in self._cur_id_cache:
            return self._cur_id_cache[currency_code]
        rows = self._fetch(_LOOKUP_CUR_ID_SQL, currency_code)
        cur_id = int(rows[0][0]) if rows else None
        self._cur_id_cache[currency_code] = cur_id
        return cur_id

    def _get_rate(self, cur_id: int, ref_date: date) -> Optional[Decimal]:
        cache_key = (cur_id, ref_date.year, ref_date.month)
        if cache_key in self._rate_cache:
            return self._rate_cache[cache_key]
        rows = self._fetch(_LOOKUP_RATE_SQL, cur_id, ref_date, ref_date)
        rate = Decimal(str(rows[0][0])) if rows else None
        self._rate_cache[cache_key] = rate
        return rate
