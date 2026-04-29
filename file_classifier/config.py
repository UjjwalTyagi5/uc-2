import os

from pydantic_settings import BaseSettings
from pydantic import Field, model_validator


class Settings(BaseSettings):
    # LLM Provider
    llm_provider: str = Field(default="azure", description="LLM provider: 'azure' or 'openai'")

    # Azure OpenAI
    azure_openai_api_key: str = Field(default="")
    azure_openai_endpoint: str = Field(default="")
    azure_openai_api_version: str = Field(default="2024-10-21")
    # Falls back to AZURE_OPENAI_DEPLOYMENT (shared pipeline var) if not set explicitly
    azure_openai_deployment_name: str = Field(default="")

    # OpenAI
    openai_api_key: str = Field(default="")
    openai_model: str = Field(default="gpt-4o")

    # Classification
    confidence_threshold: float = Field(default=0.75)
    max_content_tokens: int = Field(default=6000)
    max_file_size_mb: int = Field(default=50)
    batch_concurrency: int = Field(default=5, description="Max parallel LLM calls for batch endpoint")

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}

    @model_validator(mode="after")
    def _apply_shared_deployment(self) -> "Settings":
        # If AZURE_OPENAI_DEPLOYMENT_NAME not set, fall back to the shared
        # AZURE_OPENAI_DEPLOYMENT env var used by the rest of the pipeline.
        if not self.azure_openai_deployment_name:
            self.azure_openai_deployment_name = (
                os.getenv("AZURE_OPENAI_DEPLOYMENT") or "gpt-4o"
            )
        return self


settings = Settings()
