"""LLM client for price benchmark analysis using Azure OpenAI."""

from __future__ import annotations

import json
import re
from decimal import Decimal
from pathlib import Path
from typing import Optional

from loguru import logger

from quotation_extraction.llm_client import ExtractionLLMClient
from utils.config import AppConfig

from .models import HistoricalItem

_PROMPTS_DIR = Path(__file__).parent / "prompts"


def _load_prompt(name: str) -> str:
    return (_PROMPTS_DIR / name).read_text(encoding="utf-8")


_INFLATION_SYSTEM = (
    "You are a procurement economics expert. "
    "Answer only with a JSON object — no extra text, no markdown fences."
)


class BenchmarkLLMClient:
    """Calls Azure OpenAI to produce a price benchmark for one line item.

    Wraps ExtractionLLMClient (text-only query) so retry logic and Azure
    OpenAI configuration are shared with the rest of the repo.
    """

    def __init__(self, config: AppConfig) -> None:
        self._llm           = ExtractionLLMClient(config)
        self._system_prompt = _load_prompt("system.txt")
        self._analysis_tmpl = _load_prompt("analysis.txt")

    def analyze(
        self,
        row: dict,
        historical_items: list[HistoricalItem],
        curr_unit_eur: Optional[Decimal] = None,
    ) -> dict:
        """Return LLM benchmark analysis (bp_unit_price + summary) for one item.

        Returns
        -------
        dict
            Keys: ``bp_unit_price`` (float|None), ``summary`` (str).
            Empty dict on parse failure.
        """
        current_block    = _format_current_item(row, curr_unit_eur)
        historical_block = _format_historical_table(historical_items)

        user_prompt = self._analysis_tmpl.format(
            current_item     = current_block,
            historical_table = historical_block,
        )

        try:
            raw = self._llm.query(self._system_prompt, user_prompt)
        except Exception as exc:
            logger.error(
                "BenchmarkLLMClient.analyze: LLM call failed for purchase_dtl_id={}: {}",
                row.get("purchase_dtl_id"), exc,
            )
            return {}

        return _parse_response(raw, row.get("purchase_dtl_id"))

    def estimate_inflation(
        self,
        item_name: Optional[str],
        item_category: Optional[str],
        supplier_country: Optional[str],
        ref_year: Optional[int],
        current_year: Optional[int],
        historical_items: Optional[list[HistoricalItem]] = None,
    ) -> Optional[float]:
        """Ask the LLM: buying this item from this country — what is the inflation rate
        from ref_year to current_year?

        Historical items are passed as supplementary context only; the LLM is
        explicitly instructed not to derive inflation from the price delta.

        Returns the estimated inflation % as a float, or None on failure.
        """
        if not supplier_country or not ref_year or not current_year:
            return None
        if current_year <= ref_year:
            return None

        years = current_year - ref_year

        hist_lines: list[str] = []
        if historical_items:
            for it in historical_items:
                price = it.unit_price_eur if it.unit_price_eur is not None else it.unit_price
                if price is None:
                    continue  # skip items with no usable price
                currency = "EUR" if it.unit_price_eur is not None else (it.currency or "")
                hist_lines.append(
                    f"  - {it.quotation_date or 'N/A'} | {it.supplier_name or 'N/A'} "
                    f"| {price} {currency} / {it.unit or 'unit'}"
                )

        if hist_lines:
            hist_block = (
                "\n\nFor additional context, here are historical purchase prices "
                "for this item (provided for reference only — do NOT derive the "
                "inflation rate directly from the price delta; use your knowledge "
                "of macroeconomic inflation in that country and category instead):\n"
                + "\n".join(hist_lines)
                + "\n"
            )
            base_instruction = (
                f"Base your estimate on the typical inflation trend for this item category "
                f"in that country over that period — not on the historical prices above. "
            )
        else:
            hist_block = ""
            base_instruction = (
                f"Base your estimate on the typical inflation trend for this item category "
                f"in that country over that period. "
            )

        user_prompt = (
            f"I am buying the following item from {supplier_country}:\n"
            f"  Item     : {item_name or 'N/A'}\n"
            f"  Category : {item_category or 'N/A'}\n"
            f"{hist_block}\n"
            f"What is the estimated cumulative inflation rate (%) for this type of item "
            f"in {supplier_country} from {ref_year} to {current_year} ({years} year(s))?\n\n"
            f"{base_instruction}"
            f"Respond ONLY with a JSON object:\n"
            f'{{ "inflation_pct": <number, positive if prices rose, negative if fell, null if unknown> }}'
        )

        try:
            raw = self._llm.query(_INFLATION_SYSTEM, user_prompt)
        except Exception as exc:
            logger.warning(
                "BenchmarkLLMClient.estimate_inflation: LLM call failed "
                "country={!r} {}-{}: {}",
                supplier_country, ref_year, current_year, exc,
            )
            return None

        clean = re.sub(r"```(?:json)?\s*", "", raw).strip().rstrip("`").strip()
        try:
            data = json.loads(clean)
            val = data.get("inflation_pct")
            return float(val) if val is not None else None
        except Exception:
            logger.warning(
                "BenchmarkLLMClient.estimate_inflation: failed to parse response: {}",
                raw[:200],
            )
            return None


# ── Formatting helpers ─────────────────────────────────────────────────────────

def _format_current_item(row: dict, curr_unit_eur: Optional[Decimal] = None) -> str:
    raw_price = row.get("unit_price")
    currency  = row.get("currency") or ""
    if curr_unit_eur is not None and currency.upper() != "EUR":
        price_str = f"{curr_unit_eur} EUR  (original: {raw_price} {currency})"
    elif curr_unit_eur is not None:
        price_str = f"{curr_unit_eur} EUR"
    else:
        price_str = f"{raw_price} {currency}"

    lines = [
        f"- Item name        : {row.get('item_name') or 'N/A'}",
        f"- Description      : {row.get('item_description') or 'N/A'}",
        f"- Category         : {' > '.join(filter(None, [row.get(f'item_level_{i}') for i in range(1, 9)]))}",
        f"- Commodity tag    : {row.get('commodity_tag') or 'N/A'}",
        f"- Quantity         : {row.get('quantity')} {row.get('unit') or ''}",
        f"- Quoted unit price: {price_str}",
        f"- Supplier         : {row.get('supplier_name') or 'N/A'}",
        f"- Quotation date   : {row.get('quotation_date') or 'N/A'}",
    ]
    return "\n".join(lines)


def _format_historical_table(items: list[HistoricalItem]) -> str:
    if not items:
        return "No historical data available."

    header = "| # | Supplier | Date | Qty | Unit | Unit Price (EUR) | Orig. Currency |"
    sep    = "|---|----------|------|-----|------|-----------------|----------------|"
    rows: list[str] = []
    has_unconverted = False

    for i, it in enumerate(items, 1):
        if it.unit_price_eur is not None:
            eur_label = str(it.unit_price_eur)
        elif it.unit_price is not None:
            eur_label = f"{it.unit_price} *"
            has_unconverted = True
        else:
            eur_label = "N/A"

        rows.append(
            f"| {i} "
            f"| {it.supplier_name or 'N/A'} "
            f"| {it.quotation_date or 'N/A'} "
            f"| {it.quantity or 'N/A'} "
            f"| {it.unit or 'N/A'} "
            f"| {eur_label} "
            f"| {it.currency or 'N/A'} |"
        )

    result = "\n".join([header, sep] + rows)
    if has_unconverted:
        result += "\n* price shown in original currency — no EUR rate available"
    return result


def _parse_response(raw: str, dtl_id: Optional[int]) -> dict:
    """Extract JSON from LLM response, stripping any markdown fences."""
    clean = re.sub(r"```(?:json)?\s*", "", raw).strip().rstrip("`").strip()
    try:
        data = json.loads(clean)
        return {
            "bp_unit_price": data.get("bp_unit_price"),
            "summary":       str(data.get("summary") or ""),
        }
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning(
            "BenchmarkLLMClient: failed to parse JSON for purchase_dtl_id={}: {} | raw={}",
            dtl_id, exc, raw[:300],
        )
        return {}
