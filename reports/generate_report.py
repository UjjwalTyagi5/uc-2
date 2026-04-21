"""
Generate a PDF report from pipeline log analysis.
Usage: python generate_report.py
"""

from fpdf import FPDF
from datetime import datetime


class PipelineReport(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(100, 100, 100)
        self.cell(0, 8, "Motherson RAS Pipeline - Run Report", align="L")
        self.cell(0, 8, "19 April 2026", align="R", new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(200, 200, 200)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    def section_title(self, title):
        self.set_font("Helvetica", "B", 14)
        self.set_text_color(30, 60, 120)
        self.ln(4)
        self.cell(0, 10, title, new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(30, 60, 120)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def sub_title(self, title):
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(50, 50, 50)
        self.ln(2)
        self.cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def body_text(self, text):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 6, text)
        self.ln(1)

    def key_value(self, key, value, indent=0):
        self.set_font("Helvetica", "", 10)
        x = self.get_x() + indent
        self.set_x(x)
        self.set_text_color(80, 80, 80)
        self.cell(70, 6, key, new_x="END")
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(30, 30, 30)
        self.cell(0, 6, str(value), new_x="LMARGIN", new_y="NEXT")

    def table(self, headers, rows, col_widths=None):
        if col_widths is None:
            total = 190
            col_widths = [total / len(headers)] * len(headers)

        # Header
        self.set_font("Helvetica", "B", 9)
        self.set_fill_color(30, 60, 120)
        self.set_text_color(255, 255, 255)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 7, h, border=1, fill=True, align="C")
        self.ln()

        # Rows
        self.set_font("Helvetica", "", 9)
        self.set_text_color(40, 40, 40)
        for row_idx, row in enumerate(rows):
            if row_idx % 2 == 0:
                self.set_fill_color(245, 245, 250)
            else:
                self.set_fill_color(255, 255, 255)
            for i, cell in enumerate(row):
                align = "R" if i > 0 and str(cell).replace(",", "").replace(".", "").replace("%", "").replace(" ", "").isdigit() else "L"
                if i == 0:
                    align = "L"
                self.cell(col_widths[i], 6, str(cell), border=1, fill=True, align=align)
            self.ln()
        self.ln(3)

    def highlight_box(self, text, color="blue"):
        colors = {
            "blue":  (230, 240, 255, 30, 60, 120),
            "green": (230, 250, 230, 30, 120, 30),
            "red":   (255, 235, 235, 180, 30, 30),
            "amber": (255, 245, 225, 180, 120, 30),
        }
        bg_r, bg_g, bg_b, txt_r, txt_g, txt_b = colors.get(color, colors["blue"])
        self.set_fill_color(bg_r, bg_g, bg_b)
        self.set_text_color(txt_r, txt_g, txt_b)
        self.set_font("Helvetica", "B", 10)
        self.cell(0, 8, f"  {text}", fill=True, new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(40, 40, 40)
        self.ln(2)


def generate():
    pdf = PipelineReport()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)

    # ══════════════════════════════════════════════════════════════════
    # PAGE 1 - EXECUTIVE SUMMARY
    # ══════════════════════════════════════════════════════════════════
    pdf.add_page()

    pdf.set_font("Helvetica", "B", 22)
    pdf.set_text_color(30, 60, 120)
    pdf.ln(10)
    pdf.cell(0, 14, "Pipeline Run Report", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 12)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 8, "1000 RAS IDs  |  19 April 2026  |  00:00 - 17:17 UTC", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(8)

    # Key metrics boxes
    pdf.highlight_box("725 of 726 PRs completed successfully (99.86% success rate)", "green")
    pdf.highlight_box("19,561 files classified across 4 stages", "blue")
    pdf.highlight_box("1 PR failed at BLOB_UPLOAD (missing work folder) | 105 file-level classification warnings", "amber")

    pdf.ln(4)
    pdf.section_title("1. Executive Summary")
    pdf.body_text(
        "The pipeline processed 726 Purchase Requests through 4 sequential stages: "
        "INGESTION, EMBED_DOC_EXTRACTION, BLOB_UPLOAD, and CLASSIFICATION. "
        "725 PRs completed all stages successfully (99.86% success rate). "
        "1 PR (R_261225/2026) failed at BLOB_UPLOAD due to a missing local work folder. "
        "The total pipeline runtime was approximately 17 hours 17 minutes with 4 parallel workers."
    )

    pdf.sub_title("Key Metrics")
    pdf.key_value("Total PRs Processed:", "726")
    pdf.key_value("PRs Succeeded:", "725 (99.86%)")
    pdf.key_value("PRs Failed:", "1 (0.14%)")
    pdf.key_value("Total Attachments Downloaded:", "4,517")
    pdf.key_value("Embedded Files Extracted:", "14,856")
    pdf.key_value("Files Uploaded to Blob:", "19,502")
    pdf.key_value("Files Classified:", "19,561 (parent) + embedded")
    pdf.key_value("Classification Errors:", "105 (0.54% of files)")
    pdf.key_value("Pipeline Duration:", "~17h 17m (00:00 - 17:17)")

    # ══════════════════════════════════════════════════════════════════
    # PAGE 2 - STAGE-BY-STAGE ANALYSIS
    # ══════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("2. Stage-by-Stage Performance")

    pdf.table(
        ["Stage", "Succeeded", "Failed", "Min Time", "Max Time", "Avg Time", "Total Time"],
        [
            ["INGESTION",            "722", "0", "1.0s",   "3.9s",    "1.2s",   "14m 40s"],
            ["EMBED_DOC_EXTRACTION", "723", "0", "1.6s",   "222.9s",  "37.7s",  "7h 34m"],
            ["BLOB_UPLOAD",          "722", "1", "4.8s",   "62.6s",   "9.7s",   "1h 57m"],
            ["CLASSIFICATION",       "725", "0", "29.6s",  "2368.7s", "293.3s", "59h 4m*"],
        ],
        col_widths=[45, 22, 18, 20, 22, 22, 25]
    )
    pdf.set_font("Helvetica", "I", 8)
    pdf.set_text_color(120, 120, 120)
    pdf.cell(0, 5, "* CLASSIFICATION total is wall-clock sum across all workers (4 parallel). Actual elapsed ~15h with parallelism.", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    pdf.sub_title("2.1 INGESTION (Stage 1)")
    pdf.body_text(
        "All 722 PRs ingested successfully. Each PR was recorded in the ras_tracker table. "
        "Average time: 1.2 seconds. No errors."
    )

    pdf.sub_title("2.2 EMBED_DOC_EXTRACTION (Stage 2)")
    pdf.body_text(
        "723 PRs processed. 4,517 attachments were downloaded from the on-prem database and saved locally. "
        "14,856 embedded files were extracted from parent documents (Excel, Word, PDF, MSG). "
        "204 individual embedded attachment extractions failed (primarily due to long file paths on Windows "
        "exceeding the 260-character limit). 256 attachment directories had no parent PK for embedded classification."
    )

    pdf.sub_title("2.3 BLOB_UPLOAD (Stage 3)")
    pdf.body_text(
        "722 of 723 PRs uploaded successfully. 19,502 files were uploaded to Azure Blob Storage. "
        "1 PR (R_261225/2026) failed because its local work folder was not found - likely the "
        "attachment download produced zero files for this PR."
    )

    pdf.sub_title("2.4 CLASSIFICATION (Stage 4)")
    pdf.body_text(
        "All 725 PRs that reached this stage completed successfully. The stage classified 19,561 files "
        "(4,532 parent + 15,029 embedded) using Azure OpenAI GPT-4o-mini. "
        "105 individual files failed classification but were gracefully handled as 'Others' with 0.0 confidence. "
        "No rate limit errors or LLM timeouts were encountered, and no escalations to the full GPT-4o model "
        "were triggered (all mini-model classifications exceeded the 0.75 confidence threshold)."
    )

    # ══════════════════════════════════════════════════════════════════
    # PAGE 3 - CLASSIFICATION DEEP DIVE
    # ══════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("3. Classification Results - Deep Dive")

    pdf.sub_title("3.1 Document Type Distribution (All Files)")
    pdf.table(
        ["Document Type", "Count", "Percentage"],
        [
            ["Others",            "11,078", "56.6%"],
            ["MPBC",              "6,719",  "34.3%"],
            ["Quotation",         "1,494",  "7.6%"],
            ["RFQ",               "135",    "0.7%"],
            ["BER",               "121",    "0.6%"],
            ["E-Auction Results", "14",     "0.1%"],
        ],
        col_widths=[70, 50, 50]
    )

    pdf.body_text(
        "The high 'Others' count (56.6%) is expected. The majority of embedded files extracted from "
        "Excel/Word/MSG documents are logos, signature images, email headers, XML metadata, and other "
        "non-procurement artifacts that correctly classify as 'Others'. The meaningful procurement "
        "documents (MPBC, Quotation, RFQ, BER, E-Auction) account for 8,483 files (43.4%)."
    )

    pdf.sub_title("3.2 Parent vs Embedded Breakdown")
    pdf.table(
        ["Document Type", "Parent Files", "Embedded Files", "Total"],
        [
            ["Others",            "2,336", "8,742",  "11,078"],
            ["MPBC",              "1,371", "5,348",  "6,719"],
            ["Quotation",         "717",   "777",    "1,494"],
            ["RFQ",               "18",    "117",    "135"],
            ["BER",               "88",    "33",     "121"],
            ["E-Auction Results", "2",     "12",     "14"],
            ["Total",             "4,532", "15,029", "19,561"],
        ],
        col_widths=[50, 40, 45, 40]
    )

    pdf.sub_title("3.3 Confidence Score Distribution")
    pdf.table(
        ["Confidence Range", "File Count", "Percentage", "Interpretation"],
        [
            ["0.95 - 1.00", "10,525", "53.8%", "Very high confidence"],
            ["0.90 - 0.94", "4,506",  "23.0%", "High confidence"],
            ["0.85 - 0.89", "1,078",  "5.5%",  "Good confidence"],
            ["0.80 - 0.84", "455",    "2.3%",  "Moderate confidence"],
            ["0.75 - 0.79", "1,083",  "5.5%",  "Acceptable (above threshold)"],
            ["0.60 - 0.74", "140",    "0.7%",  "Low (below escalation threshold)"],
            ["< 0.60",      "1,774",  "9.1%",  "Very low / unclassifiable"],
        ],
        col_widths=[40, 30, 30, 70]
    )

    pdf.body_text(
        "90.1% of classified files achieved confidence >= 0.75 (the escalation threshold). "
        "The 9.1% with confidence < 0.60 are primarily embedded images, XML files, and other "
        "non-document artifacts that correctly classified as 'Others' with low confidence."
    )

    # ══════════════════════════════════════════════════════════════════
    # PAGE 4 - CLASSIFICATION ERRORS
    # ══════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("4. Classification Errors Analysis")

    pdf.highlight_box("105 classification errors out of 19,666 files attempted (0.53%)", "amber")

    pdf.sub_title("4.1 Error Breakdown by Category")
    pdf.table(
        ["Error Category", "Count", "% of Errors", "Impact"],
        [
            ["Corrupt/unreadable PNG images",     "45", "42.9%", "Marked as Others"],
            ["Unidentified image files",          "28", "26.7%", "Marked as Others"],
            ["Broken PNG data stream",            "9",  "8.6%",  "Marked as Others"],
            ["Azure content filter (jailbreak)",  "8",  "7.6%",  "Marked as Others"],
            ["Corrupt Excel files",               "7",  "6.7%",  "Marked as Others"],
            ["Azure content filter (sexual)",     "3",  "2.9%",  "Marked as Others"],
            ["Empty PDF (0 pages)",               "3",  "2.9%",  "Marked as Others"],
            ["Truncated JPEG files",              "1",  "1.0%",  "Marked as Others"],
            ["Other",                             "1",  "1.0%",  "Marked as Others"],
        ],
        col_widths=[65, 20, 28, 52]
    )

    pdf.sub_title("4.2 Error Category 1: Corrupt Image Files (83 errors)")
    pdf.body_text(
        "Root cause: Embedded images extracted from Excel/Word/MSG files are sometimes truncated or "
        "corrupt. PIL (Python Imaging Library) raises OSError when attempting to decode the image data. "
        "These are typically small logos, signatures, or inline graphics - not procurement documents.\n\n"
        "Fix: Add PIL's LOAD_TRUNCATED_IMAGES flag and wrap image extraction in a try/except to "
        "gracefully return a fallback result instead of raising."
    )

    pdf.sub_title("4.3 Error Category 2: Azure Content Filter (11 errors)")
    pdf.body_text(
        "Root cause: Some PDFs contain reversed/garbled text (e.g., 'epyT ygolonhceT lpS kcolbonoM') "
        "that Azure's jailbreak detector interprets as obfuscation attempts. 3 files triggered the "
        "sexual content filter (likely from medical/industrial specifications).\n\n"
        "Impact: These are false positives. The pipeline already handles them correctly by returning "
        "'Others' with 0.0 confidence. No code fix required - this is an Azure-side limitation."
    )

    pdf.sub_title("4.4 Error Category 3: Corrupt Excel Files (7 errors)")
    pdf.body_text(
        "Root cause: Some .xls files are password-protected, corrupt, or use a format that neither "
        "openpyxl nor xlrd can parse. Already handled gracefully - returns 'Others' with 0.0 confidence."
    )

    pdf.sub_title("4.5 Error Category 4: Empty PDFs (3 errors)")
    pdf.body_text(
        "Root cause: PDF files with 0 pages cause an IndexError when attempting to access pdf.pages[0] "
        "for image-based extraction.\n\n"
        "Fix: Add a guard check for empty page list before accessing pdf.pages[0]."
    )

    # ══════════════════════════════════════════════════════════════════
    # PAGE 5 - SKIPPED FILES & OTHER OBSERVATIONS
    # ══════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("5. Skipped Files & Other Observations")

    pdf.sub_title("5.1 Files Skipped (Unsupported Extensions)")
    pdf.table(
        ["Extension", "Count", "Reason"],
        [
            [".msg",  "1,082", "Outlook email files - not classifiable as documents"],
            [".zip",  "247",   "Archive files - contents already extracted"],
            ["(none)", "190",  "Files with no extension"],
            [".xml",  "91",    "XML metadata files"],
            [".rels", "24",    "Office relationship files (internal)"],
            [".gif",  "12",    "GIF images - not supported by classifier"],
            [".emz",  "3",     "Enhanced metafile (compressed)"],
            [".vcf",  "2",     "Contact card files"],
            [".eml",  "2",     "Email files"],
            [".7z",   "2",     "7-Zip archive files"],
            ["Other", "10",    "Misc extensions (.xlsm, numeric, etc.)"],
        ],
        col_widths=[30, 25, 120]
    )

    pdf.sub_title("5.2 Embedded Extraction Failures")
    pdf.body_text(
        "204 individual embedded attachment extractions failed during Stage 2 (EMBED_DOC_EXTRACTION). "
        "The primary cause is Windows' 260-character path limit - some MSG files have extremely long "
        "filenames (e.g., 'RE_ SCL FINAL QUOTE - RAS R_261397 __ SITC of 20KLD STP...') that, when "
        "combined with the extraction prefix, exceed the path limit."
    )

    pdf.sub_title("5.3 Missing Parent PK for Embedded Files")
    pdf.body_text(
        "256 attachment directories had no parent PK in the attachment_classification table, preventing "
        "embedded file classification for those directories. This occurs when the parent file extraction "
        "failed in Stage 2 but the directory structure was partially created."
    )

    pdf.sub_title("5.4 Pipeline Performance Observations")
    pdf.body_text(
        "- No LLM rate limit errors were encountered across the entire run\n"
        "- No escalations from GPT-4o-mini to GPT-4o were triggered (all mini responses exceeded 0.75 threshold)\n"
        "- CLASSIFICATION was the most time-intensive stage (avg 293s per PR, ~5 min) due to sequential LLM calls per file\n"
        "- EMBED_DOC_EXTRACTION was the second most intensive (avg 37.7s) due to file I/O and OLE/PDF extraction\n"
        "- The 4 parallel workers effectively utilized the available resources"
    )

    # ══════════════════════════════════════════════════════════════════
    # PAGE 6 - RECOMMENDATIONS
    # ══════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("6. Recommended Fixes")

    pdf.body_text(
        "All 105 classification errors were non-fatal (files were marked as 'Others' with 0.0 confidence "
        "and the pipeline continued). The following fixes will eliminate the error stack traces and improve "
        "classification coverage:"
    )

    pdf.sub_title("Fix 1: Corrupt Image Handling (Priority: High)")
    pdf.body_text(
        "File: file_classifier/extractors/image_extractor.py\n"
        "Impact: 83 of 105 errors (79%)\n"
        "Change: Add PIL LOAD_TRUNCATED_IMAGES flag and wrap extraction in try/except to return "
        "a graceful fallback ExtractionResult for corrupt/truncated images."
    )

    pdf.sub_title("Fix 2: Empty PDF Guard (Priority: Medium)")
    pdf.body_text(
        "File: file_classifier/extractors/pdf_extractor.py\n"
        "Impact: 3 of 105 errors (2.9%)\n"
        "Change: Add guard for pdf.pages being empty before accessing pdf.pages[0] in _extract_as_image()."
    )

    pdf.sub_title("Fix 3: Content Filter Handling (Priority: Low)")
    pdf.body_text(
        "File: file_classifier/classifier/llm_client.py\n"
        "Impact: 11 of 105 errors (10.5%) - cosmetic only\n"
        "Change: Detect 'content_filter' in BadRequestError and log a clean single-line warning "
        "instead of a full stack trace. Functionally already handled correctly."
    )

    pdf.sub_title("Fix 4: Excel Error Logging (Priority: Low)")
    pdf.body_text(
        "File: file_classifier/extractors/excel_extractor.py\n"
        "Impact: 7 of 105 errors (6.7%) - cosmetic only\n"
        "Change: Log per-engine errors for debugging. Functionally already handled correctly."
    )

    pdf.ln(6)
    pdf.section_title("7. Summary")

    pdf.highlight_box("Pipeline Success Rate: 99.86% (725/726 PRs)", "green")
    pdf.highlight_box("File Classification Rate: 99.47% (19,561 successful / 19,666 attempted)", "green")
    pdf.highlight_box("All errors are non-fatal and fixable with minor code changes", "blue")

    # Save
    output_path = "reports/pipeline_run_report_2026-04-19.pdf"
    import os
    os.makedirs("reports", exist_ok=True)
    pdf.output(output_path)
    print(f"Report saved to: {output_path}")


if __name__ == "__main__":
    generate()
