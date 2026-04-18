from file_classifier.extractors.base import BaseExtractor
from file_classifier.extractors.excel_extractor import ExcelExtractor, CsvExtractor
from file_classifier.extractors.pdf_extractor import PdfExtractor
from file_classifier.extractors.word_extractor import WordExtractor
from file_classifier.extractors.text_extractor import TextExtractor
from file_classifier.extractors.image_extractor import ImageExtractor
from file_classifier.extractors.pptx_extractor import PptxExtractor
from file_classifier.extractors.legacy_doc_extractor import LegacyDocExtractor
from file_classifier.extractors.html_extractor import HtmlExtractor

_EXTRACTORS: dict[str, BaseExtractor] = {
    "excel": ExcelExtractor(),
    "csv": CsvExtractor(),
    "pdf": PdfExtractor(),
    "word": WordExtractor(),
    "text": TextExtractor(),
    "image": ImageExtractor(),
    "pptx": PptxExtractor(),
    "legacy_doc": LegacyDocExtractor(),
    "html": HtmlExtractor(),
}


def get_extractor(file_type: str) -> BaseExtractor:
    extractor = _EXTRACTORS.get(file_type)
    if not extractor:
        raise ValueError(f"No extractor found for file type: {file_type}")
    return extractor
