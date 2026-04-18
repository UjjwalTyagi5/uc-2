from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class ExtractionResult:
    text_content: str
    metadata: dict = field(default_factory=dict)
    is_image_based: bool = False
    image_base64: str | None = None


class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, file_bytes: bytes, filename: str) -> ExtractionResult:
        pass
