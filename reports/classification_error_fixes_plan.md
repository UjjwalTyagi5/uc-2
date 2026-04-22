# Plan: Fix Classification Errors from 1000-RAS Production Run

**Date:** 19 April 2026  
**Log file:** `logs/pipeline_2026-04-19.log`  
**Total errors:** 105 out of 19,666 files (0.53%)  
**Status:** All errors are non-fatal — pipeline continued, files marked as "Others" with 0.0 confidence

---

## Error Summary

| # | Error Type | Count | % of Errors | Files Affected |
|---|-----------|-------|-------------|----------------|
| 1 | Corrupt/unreadable PNG images | 45 | 42.9% | `image_extractor.py` |
| 2 | Unidentified image files | 28 | 26.7% | `image_extractor.py` |
| 3 | Broken PNG data stream | 9 | 8.6% | `image_extractor.py` |
| 4 | Azure content filter — jailbreak false positive | 6 | 5.7% | `llm_client.py` |
| 5 | Corrupt Excel files (.xls) | 7 | 6.7% | `excel_extractor.py` |
| 6 | Azure content filter — sexual false positive | 3 | 2.9% | `llm_client.py` |
| 7 | Empty/corrupt PDF (0 pages) | 3 | 2.9% | `pdf_extractor.py` |
| 8 | Truncated JPEG files | 1 | 1.0% | `image_extractor.py` |
| 9 | Azure content filter — jailbreak (additional) | 2 | 1.9% | `llm_client.py` |
| 10 | Other | 1 | 1.0% | — |

---

## Fix 1: Corrupt/Truncated Image Handling (Errors 1, 2, 3, 8)

**Priority:** HIGH  
**Impact:** 83 of 105 errors (79%)  
**File:** `file_classifier/extractors/image_extractor.py`

### Root Cause

Embedded images extracted from Excel/Word/MSG files are sometimes truncated or corrupt. PIL (Python Imaging Library) raises errors at different stages:
- `OSError: unrecognized data stream contents when reading image file` — corrupt PNG, fails during `.convert("RGB")` (45 occurrences)
- `PIL.UnidentifiedImageError: cannot identify image file` — PIL can't even detect the format, file is garbage (28 occurrences)
- `OSError: broken data stream when reading image file` — corrupt PNG, fails during `.convert("RGB")` (9 occurrences)
- `OSError: Truncated File Read` — JPEG truncated/incomplete download from DB (1 occurrence)

These are typically small logos, signature images, or inline graphics embedded in Office documents — not procurement documents that need classification.

### Fix

1. Set `PIL.ImageFile.LOAD_TRUNCATED_IMAGES = True` at module level — tells PIL to decode what it can instead of raising on truncated data
2. Wrap the entire `extract()` method body in try/except for `OSError`, `PIL.UnidentifiedImageError`, and `Exception`
3. On failure: return an `ExtractionResult` with `text_content="[Corrupt/unreadable image file — cannot extract content]"` and no `image_base64` — this lets the classifier still return "Others" cleanly without a stack trace

### Code Change

```python
# image_extractor.py

import io
import base64
import structlog
from PIL import Image, ImageFile, UnidentifiedImageError
from file_classifier.extractors.base import BaseExtractor, ExtractionResult

logger = structlog.get_logger()

MAX_IMAGE_DIMENSION = 2048

# Allow PIL to process truncated images instead of raising
ImageFile.LOAD_TRUNCATED_IMAGES = True


class ImageExtractor(BaseExtractor):
    def extract(self, file_bytes: bytes, filename: str) -> ExtractionResult:
        logger.info("Extracting image content", filename=filename)

        try:
            img = Image.open(io.BytesIO(file_bytes))

            if img.mode not in ("RGB", "L"):
                img = img.convert("RGB")

            if max(img.size) > MAX_IMAGE_DIMENSION:
                img.thumbnail((MAX_IMAGE_DIMENSION, MAX_IMAGE_DIMENSION), Image.LANCZOS)

            buf = io.BytesIO()
            img.save(buf, format="PNG")
            img_base64 = base64.b64encode(buf.getvalue()).decode("utf-8")

            return ExtractionResult(
                text_content="[Image file - content sent as image for visual analysis]",
                metadata={
                    "original_size": f"{img.size[0]}x{img.size[1]}",
                    "format": img.format or "unknown",
                },
                is_image_based=True,
                image_base64=img_base64,
            )

        except (OSError, UnidentifiedImageError, Exception) as e:
            logger.warning(
                "Corrupt or unreadable image — returning fallback",
                filename=filename,
                error=str(e),
            )
            return ExtractionResult(
                text_content="[Corrupt/unreadable image file — cannot extract content for classification]",
                metadata={"extraction_method": "failed", "error": str(e)},
            )
```

---

## Fix 2: Azure Content Filter Handling (Errors 4, 6, 9)

**Priority:** LOW (cosmetic — pipeline already handles correctly)  
**Impact:** 11 of 105 errors (10.5%)  
**File:** `file_classifier/classifier/llm_client.py`

### Root Cause

Some documents trigger Azure OpenAI's content management policy:
- **Jailbreak false positive (8 occurrences):** PDFs with reversed/garbled text (e.g., `"epyT ygolonhceT lpS kcolbonoM detresnI"`) look like obfuscation attempts to Azure's jailbreak detector
- **Sexual content filter (3 occurrences):** Documents with medical/industrial terminology that triggers the sexual content filter

These are `BadRequestError` with `code: content_filter`. They are **permanent** for that specific content — retrying won't help.

### Current Behavior

The `BadRequestError` handler in `_create_completion()` only checks for "temperature" in the error message, then re-raises. The error propagates to `_classify_single_file()`'s catch-all which returns `("Others", 0.0)`. **This is functionally correct** — the pipeline doesn't halt. The only issue is the noisy full stack trace in logs.

### Fix

In the `BadRequestError` handler inside `_create_completion()`, add a check for `content_filter` in the error message. Log a concise warning and re-raise (the catch-all upstream will still handle it).

### Code Change

```python
# In _create_completion(), replace the BadRequestError handler:

except BadRequestError as e:
    if "temperature" in str(e):
        logger.info("Model does not support temperature=0, caching for future calls", model=model)
        _models_without_temperature.add(model)
        extra.pop("temperature", None)
        continue
    if "content_filter" in str(e):
        logger.warning(
            "Azure content filter triggered — document will be classified as Others",
            model=model,
            error_code=getattr(e, 'code', 'unknown'),
        )
    raise  # all BadRequestErrors are permanent
```

---

## Fix 3: Empty PDF Guard (Error 7)

**Priority:** MEDIUM  
**Impact:** 3 of 105 errors (2.9%)  
**File:** `file_classifier/extractors/pdf_extractor.py`

### Root Cause

`_extract_as_image()` is called when text extraction yields < 50 characters, but the PDF has `total_pages = 0` (corrupt/empty PDF). The method unconditionally accesses `pdf.pages[0]`, causing `IndexError: list index out of range`.

### Fix

1. In the main `extract()` method: if `total_pages == 0`, return a fallback `ExtractionResult` immediately
2. In `_extract_as_image()`: add a guard check `if not pdf.pages:` before accessing `pdf.pages[0]`

### Code Change

```python
# In PdfExtractor.extract(), add after opening the PDF:

with pdfplumber.open(buf) as pdf:
    total_pages = len(pdf.pages)

    if total_pages == 0:
        logger.warning("PDF has no pages", filename=filename)
        return ExtractionResult(
            text_content="[Empty or corrupt PDF — no pages found]",
            metadata={"total_pages": 0, "extraction_method": "failed"},
        )

    pages_to_process = min(total_pages, MAX_PAGES)
    # ... rest of existing code ...


# In PdfExtractor._extract_as_image(), add guard:

def _extract_as_image(self, file_bytes, filename, total_pages):
    buf = io.BytesIO(file_bytes)

    with pdfplumber.open(buf) as pdf:
        if not pdf.pages:
            return ExtractionResult(
                text_content="[Empty or corrupt PDF — no pages to render as image]",
                metadata={"total_pages": total_pages, "extraction_method": "failed"},
            )

        page = pdf.pages[0]
        # ... rest of existing code ...
```

---

## Fix 4: Corrupt Excel Error Logging (Error 5)

**Priority:** LOW (cosmetic — already works correctly)  
**Impact:** 7 of 105 errors (6.7%)  
**File:** `file_classifier/extractors/excel_extractor.py`

### Root Cause

Some `.xls` files are password-protected, corrupt, or use a format that neither openpyxl nor xlrd can parse. The `_open_excel_file()` function silently swallows per-engine errors and raises a generic `ValueError`.

### Current Behavior

Already works correctly — the `ValueError` propagates to `_classify_single_file()`'s catch-all and returns `("Others", 0.0)`. But the stack trace is noisy and the per-engine error details are lost.

### Fix

Log the per-engine errors for debugging instead of silently swallowing them.

### Code Change

```python
# In _open_excel_file():

def _open_excel_file(buf: io.BytesIO) -> pd.ExcelFile:
    """Try openpyxl first, fall back to xlrd."""
    errors = []
    for engine in ("openpyxl", "xlrd"):
        try:
            buf.seek(0)
            return pd.ExcelFile(buf, engine=engine)
        except Exception as e:
            errors.append(f"{engine}: {e}")
            continue
    raise ValueError(
        f"Failed to open Excel file with both engines: {'; '.join(errors)}"
    )
```

---

## Files to Modify (Summary)

| File | Fix | Priority | Impact |
|------|-----|----------|--------|
| `file_classifier/extractors/image_extractor.py` | Add `LOAD_TRUNCATED_IMAGES`, wrap extract in try/except | HIGH | 83 errors (79%) |
| `file_classifier/extractors/pdf_extractor.py` | Guard `pdf.pages[0]` when page count is 0 | MEDIUM | 3 errors (2.9%) |
| `file_classifier/classifier/llm_client.py` | Detect `content_filter` BadRequestError, log concisely | LOW | 11 errors (10.5%) |
| `file_classifier/extractors/excel_extractor.py` | Log per-engine errors for debugging | LOW | 7 errors (6.7%) |

---

## Verification

After applying fixes, re-run with `--limit 1` on a PR that previously had errors. Expected:
- Clean `WARNING` log lines (no stack traces for expected conditions like corrupt images)
- `doc_type = "Others"`, `classification_conf = 0.0` in the DB for unprocessable files
- Pipeline continues without interruption
- All 105 error types should produce single-line warnings instead of multi-line stack traces
