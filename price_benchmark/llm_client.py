"""LLM client for price benchmark analysis using Azure OpenAI."""

from __future__ import annotations

import json
import re
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
        current_block    = _format_current_item(row)
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

def _format_current_item(row: dict) -> str:
    lines = [
        f"- Item name       : {row.get('item_name') or 'N/A'}",
        f"- Description     : {row.get('item_description') or 'N/A'}",
        f"- Category        : {' > '.join(filter(None, [row.get(f'item_level_{i}') for i in range(1, 9)]))}",
        f"- Commodity tag   : {row.get('commodity_tag') or 'N/A'}",
        f"- Quantity        : {row.get('quantity')} {row.get('unit') or ''}",
        f"- Quoted unit price: {row.get('unit_price')} {row.get('currency') or ''}",
        f"- Supplier        : {row.get('supplier_name') or 'N/A'}",
        f"- Quotation date  : {row.get('quotation_date') or 'N/A'}",
    ]
    return "\n".join(lines)


def _format_historical_table(items: list[HistoricalItem]) -> str:
    if not items:
        return "No historical data available."

    header = "| # | Supplier | Date | Qty | Unit | Unit Price | Currency |"
    sep    = "|---|----------|------|-----|------|------------|----------|"
    rows   = []
    for i, it in enumerate(items, 1):
        rows.append(
            f"| {i} "
            f"| {it.supplier_name or 'N/A'} "
            f"| {it.quotation_date or 'N/A'} "
            f"| {it.quantity or 'N/A'} "
            f"| {it.unit or 'N/A'} "
            f"| {it.unit_price or 'N/A'} "
            f"| {it.currency or 'N/A'} |"
        )
    return "\n".join([header, sep] + rows)


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
