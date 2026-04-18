from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    # LLM Provider
    llm_provider: str = Field(default="azure", description="LLM provider: 'azure' or 'openai'")

    # Azure OpenAI
    azure_openai_api_key: str = Field(default="")
    azure_openai_endpoint: str = Field(default="")
    azure_openai_api_version: str = Field(default="2024-10-21")
    azure_openai_deployment_name: str = Field(default="gpt-4o")
    azure_openai_mini_deployment_name: str = Field(default="gpt-4o-mini")

    # OpenAI
    openai_api_key: str = Field(default="")
    openai_model: str = Field(default="gpt-4o")
    openai_mini_model: str = Field(default="gpt-4o-mini")

    # Classification
    confidence_threshold: float = Field(default=0.75)
    max_content_tokens: int = Field(default=6000)
    max_file_size_mb: int = Field(default=50)
    batch_concurrency: int = Field(default=5, description="Max parallel LLM calls for batch endpoint")

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


settings = Settings()
