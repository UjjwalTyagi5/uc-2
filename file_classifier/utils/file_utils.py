from pathlib import Path

SUPPORTED_EXTENSIONS = {
    # Excel
    ".xlsx", ".xls", ".csv",
    # PDF
    ".pdf",
    # Word
    ".docx", ".doc",
    # PowerPoint
    ".pptx", ".ppt",
    # Text
    ".txt",
    # HTML
    ".html", ".htm",
    # Images
    ".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp",
}

EXTENSION_TO_TYPE = {
    ".xlsx": "excel",
    ".xls": "excel",
    ".csv": "csv",
    ".pdf": "pdf",
    ".docx": "word",
    ".doc": "legacy_doc",
    ".pptx": "pptx",
    ".ppt": "legacy_doc",
    ".txt": "text",
    ".html": "html",
    ".htm": "html",
    ".png": "image",
    ".jpg": "image",
    ".jpeg": "image",
    ".tiff": "image",
    ".tif": "image",
    ".bmp": "image",
}


def detect_file_type(filename: str) -> str:
    ext = Path(filename).suffix.lower()
    if ext in EXTENSION_TO_TYPE:
        return EXTENSION_TO_TYPE[ext]
    raise ValueError(f"Unsupported file type: {ext}. Supported: {SUPPORTED_EXTENSIONS}")


def get_file_extension(filename: str) -> str:
    return Path(filename).suffix.lower()


def validate_file_size(file_size: int, max_size_mb: int) -> bool:
    return file_size <= max_size_mb * 1024 * 1024
