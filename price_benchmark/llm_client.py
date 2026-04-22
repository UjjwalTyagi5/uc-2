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


class BenchmarkLLMClient:
    """Calls Azure OpenAI to produce a price benchmark for one line item.

    Wraps ExtractionLLMClient (text-only query) so retry logic and Azure
    OpenAI configuration are shared with the rest of the repo.
    """

    def __init__(self, config: AppConfig) -> None:
        self._llm          = ExtractionLLMClient(config)
        self._system_prompt = _load_prompt("system.txt")
        self._analysis_tmpl = _load_prompt("analysis.txt")

    def analyze(
        self,
        row: dict,
        historical_items: list[HistoricalItem],
        curr_unit_eur: Optional[Decimal] = None,
    ) -> dict:
        """Return LLM benchmark analysis for one selected line item.

        Parameters
        ----------
        row:
            Dict from BenchmarkItemReader (current item with pricing cols).
        historical_items:
            Similar historical items from Pinecone + DB lookup.

        Returns
        -------
        dict
            Keys: ``bp_unit_price`` (float|None), ``inflation_pct`` (float|None),
            ``summary`` (str).  Empty dict on parse failure.
        """
        current_block    = _format_current_item(row, curr_unit_eur)
        historical_block = _format_historical_table(historical_items)

        user_prompt = self._analysis_tmpl.format(
            current_item    = current_block,
            historical_table = historical_block,
        )

        try:
            raw = self._llm.query(self._system_prompt, user_prompt)
        except Exception as exc:
            logger.error(
                "BenchmarkLLMClient: LLM call failed for purchase_dtl_id={}: {}",
                row.get("purchase_dtl_id"), exc,
            )
            return {}

        return _parse_response(raw, row.get("purchase_dtl_id"))


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
    # Strip ```json ... ``` fences if present
    clean = re.sub(r"```(?:json)?\s*", "", raw).strip().rstrip("`").strip()
    try:
        data = json.loads(clean)
        return {
            "bp_unit_price": data.get("bp_unit_price"),
            "inflation_pct": data.get("inflation_pct"),
            "summary":       str(data.get("summary") or ""),
        }
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning(
            "BenchmarkLLMClient: failed to parse JSON for purchase_dtl_id={}: {} | raw={}",
            dtl_id, exc, raw[:300],
        )
        return {}
