"""Orchestrator for price benchmarking one PR or all pending PRs.

Typical usage:
    from price_benchmark.run import run_benchmark, run_all_pending

    # Single PR
    count = run_benchmark("R_260647/2026", config)

    # All pending (older purchase orders first)
    processed = run_all_pending(config, limit=50)
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Optional

from loguru import logger

from pipeline.tracker import PipelineTracker
from utils.config import AppConfig

from utils.cpi_inflation import compute_cpi_inflation_pct

from .historical_fetcher import HistoricalFetcher
from .llm_client import BenchmarkLLMClient
from .models import BenchmarkResult, HistoricalItem
from .pending_reader import PendingPRReader
from .reader import BenchmarkItemReader
from .writer import BenchmarkWriter

_STAGE_ID = 7   # must match pipeline_stages.STAGE_ID for PRICE_BENCHMARK


def run_benchmark(
    purchase_req_no: str,
    config: Optional[AppConfig] = None,
) -> int:
    """Benchmark all is_selected_quote=1 items for one PR.

    For each selected line item:
      1. Query Pinecone for similar historical items (excluding this PR).
      2. Fetch their pricing from quotation_extracted_items.
      3. Call LLM for a benchmark price recommendation and summary.
      4. Compute low/last historical prices programmatically.
      5. Upsert to benchmark_result (MERGE on purchase_dtl_id).

    Parameters
    ----------
    purchase_req_no:
        The PURCHASE_REQ_NO value (e.g. "R_260647/2026").
    config:
        Optional AppConfig override; loaded from env if not supplied.

    Returns
    -------
    int
        Number of benchmark rows upserted.
    """
    if config is None:
        config = AppConfig()

    conn_str = config.get_azure_conn_str()
    tracker  = PipelineTracker(conn_str)

    rows = BenchmarkItemReader(conn_str).fetch(purchase_req_no)
    if not rows:
        logger.warning(
            "No is_selected_quote=1 items found for {} — skipping benchmark",
            purchase_req_no,
        )
        return 0

    logger.info(
        "Benchmarking {} item(s) for PR={}",
        len(rows), purchase_req_no,
    )

    fetcher = HistoricalFetcher(config, conn_str)
    llm     = BenchmarkLLMClient(config)
    writer  = BenchmarkWriter(conn_str)

    results: list[BenchmarkResult] = []

    for row in rows:
        dtl_id = row["purchase_dtl_id"]
        uuid_fk = str(row["extracted_item_uuid_pk"])

        historical = fetcher.fetch(row, exclude_pr=purchase_req_no)

        low_item, last_item = _compute_low_last(historical)

        low_uuid  = low_item.extracted_item_uuid_pk  if low_item  else None
        last_uuid = last_item.extracted_item_uuid_pk if last_item else None

        # CPI inflation: from low_hist PR's C_DATETIME to current PR's C_DATETIME
        cpi_pct: Optional[Decimal] = None
        if not low_item:
            logger.debug("dtl_id={}: CPI skipped — no low_hist item", dtl_id)
        elif not low_item.pr_c_datetime:
            logger.debug("dtl_id={}: CPI skipped — low_hist pr_c_datetime is None (PR={})",
                         dtl_id, low_item.purchase_req_no)
        else:
            current_pr_dt = row.get("pr_c_datetime")
            if not current_pr_dt:
                logger.debug("dtl_id={}: CPI skipped — current pr_c_datetime is None", dtl_id)
            else:
                start_year = low_item.pr_c_datetime.year
                end_year   = current_pr_dt.year if hasattr(current_pr_dt, "year") else None
                if not end_year:
                    logger.debug("dtl_id={}: CPI skipped — could not read end_year from {!r}",
                                 dtl_id, current_pr_dt)
                elif start_year == end_year:
                    logger.debug("dtl_id={}: CPI skipped — same year ({}) for both PRs",
                                 dtl_id, start_year)
                else:
                    logger.debug(
                        "dtl_id={}: CPI calc start={} end={} country={!r}",
                        dtl_id, start_year, end_year, low_item.supplier_country,
                    )
                    raw_cpi = compute_cpi_inflation_pct(
                        low_item.supplier_country, start_year, end_year
                    )
                    if raw_cpi is not None:
                        cpi_pct = _to_decimal(raw_cpi)
                    else:
                        logger.debug(
                            "dtl_id={}: CPI returned None for country={!r} {}-{}",
                            dtl_id, low_item.supplier_country, start_year, end_year,
                        )

        curr_unit_eur = _to_decimal(row.get("unit_price_eur"))
        llm_out = llm.analyze(row, historical, curr_unit_eur=curr_unit_eur)

        bp_unit = _to_decimal(llm_out.get("bp_unit_price"))
        infl    = _to_decimal(llm_out.get("inflation_pct"))
        summary = llm_out.get("summary") or ""

        quantity = _to_decimal(row.get("quantity")) or Decimal("1")
        bp_total = _round2(bp_unit * quantity) if bp_unit is not None else None

        similar_ids = json.dumps([h.purchase_dtl_id for h in historical]) if historical else None

        results.append(BenchmarkResult(
            extracted_item_uuid_fk = uuid_fk,
            purchase_dtl_id        = int(dtl_id),
            bp_unit_price          = bp_unit,
            bp_total_price         = bp_total,
            low_hist_item_fk       = low_uuid,
            last_hist_item_fk      = last_uuid,
            inflation_pct          = infl,
            cpi_inflation_pct      = cpi_pct,
            similar_dtl_ids        = similar_ids,
            summary                = summary or None,
        ))

        logger.debug(
            "purchase_dtl_id={}: bp_unit={}, hist_count={}, infl={}%",
            dtl_id, bp_unit, len(historical), infl,
        )

    count = writer.write(results)

    # Advance ras_tracker so the PR is recorded at the PRICE_BENCHMARK stage.
    # Called only after all rows are written — if the write raises, the tracker
    # is not advanced and the PR stays pending for the next run.
    tracker.advance_stage(purchase_req_no, _STAGE_ID)

    logger.info("Benchmark complete for PR={}: {} row(s) upserted", purchase_req_no, count)
    return count


def run_all_pending(
    config: Optional[AppConfig] = None,
    limit: Optional[int] = None,
) -> list[str]:
    """Benchmark all PRs that have selected items but no benchmark rows yet.

    PRs are processed one at a time in purchase-date order (oldest first),
    so all items for one PR are fully benchmarked before moving to the next.

    Parameters
    ----------
    config:
        Optional AppConfig override; loaded from env if not supplied.
    limit:
        Cap the number of PRs processed.  None = process everything pending.

    Returns
    -------
    list[str]
        purchase_req_no values of PRs that were successfully processed.
    """
    if config is None:
        config = AppConfig()

    conn_str = config.get_azure_conn_str()
    pr_nos   = PendingPRReader(conn_str).fetch(limit=limit)

    if not pr_nos:
        logger.info("No pending PRs to benchmark")
        return []

    logger.info("Found {} pending PR(s) to benchmark", len(pr_nos))

    done: list[str] = []
    for idx, pr_no in enumerate(pr_nos, 1):
        logger.info("[{}/{}] Benchmarking PR={!r}", idx, len(pr_nos), pr_no)
        try:
            count = run_benchmark(pr_no, config)
            logger.info("[{}/{}] PR={!r} → {} row(s)", idx, len(pr_nos), pr_no, count)
            done.append(pr_no)
        except Exception as exc:
            logger.opt(exception=True).error(
                "[{}/{}] PR={!r} failed: {}", idx, len(pr_nos), pr_no, exc,
            )

    logger.info(
        "Benchmark run complete: {}/{} PR(s) succeeded",
        len(done), len(pr_nos),
    )
    return done


# ── Pure helpers ───────────────────────────────────────────────────────────────

def _compute_low_last(
    items: list[HistoricalItem],
) -> tuple[Optional[HistoricalItem], Optional[HistoricalItem]]:
    """Return (low_item, last_item) — cheapest and most-recent historical items.

    low  = item with the minimum EUR unit price (falls back to raw when EUR is None)
    last = item with the most recent quotation_date

    Returns (None, None) when no usable items are available.
    """
    def _eur_unit(it: HistoricalItem) -> Optional[Decimal]:
        return it.unit_price_eur if it.unit_price_eur is not None else it.unit_price

    priced = [it for it in items if _eur_unit(it) is not None]
    dated  = [it for it in items if it.quotation_date is not None]

    low_item  = min(priced, key=_eur_unit) if priced else None  # type: ignore[arg-type]
    last_item = max(dated,  key=lambda it: it.quotation_date) if dated else None  # type: ignore[arg-type]

    return low_item, last_item


def _to_decimal(value: object) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _round2(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"))
