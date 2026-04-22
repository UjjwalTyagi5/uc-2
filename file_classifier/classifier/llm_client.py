import json
import time
import random
import structlog
from openai import (
    AzureOpenAI,
    OpenAI,
    BadRequestError,
    RateLimitError,
    APITimeoutError,
    APIConnectionError,
    InternalServerError,
)
from file_classifier.config import settings

logger = structlog.get_logger()

# Cache: once we learn a model doesn't support temperature=0, remember it
_models_without_temperature: set[str] = set()

_client_cache: tuple | None = None

# ── Retry configuration ──────────────────────────────────────────────────
# These apply to transient LLM errors (rate limits, timeouts, server errors).
# For lakhs of files running over hours, generous retries with long cooldowns
# are essential to avoid losing progress.

MAX_RETRIES   = 6      # total attempts = 1 + 6 retries
BASE_DELAY    = 5.0    # starting back-off in seconds
MAX_DELAY     = 120.0  # cap: never wait more than 2 minutes per retry
JITTER_FACTOR = 0.3    # ±30% random jitter to avoid thundering herd


def _get_client() -> tuple:
    """Returns (client, model_name, mini_model_name) based on config. Cached."""
    global _client_cache
    if _client_cache is not None:
        return _client_cache

    if settings.llm_provider == "azure":
        client = AzureOpenAI(
            api_key=settings.azure_openai_api_key,
            api_version=settings.azure_openai_api_version,
            azure_endpoint=settings.azure_openai_endpoint,
        )
        _client_cache = (client, settings.azure_openai_deployment_name, settings.azure_openai_mini_deployment_name)
    else:
        client = OpenAI(api_key=settings.openai_api_key)
        _client_cache = (client, settings.openai_model, settings.openai_mini_model)

    return _client_cache


def _is_retryable(exc: Exception) -> bool:
    """Return True if this error is transient and safe to retry."""
    if isinstance(exc, RateLimitError):
        return True
    if isinstance(exc, APITimeoutError):
        return True
    if isinstance(exc, APIConnectionError):
        return True
    if isinstance(exc, InternalServerError):
        return True
    # Azure sometimes returns 5xx wrapped in generic errors
    if hasattr(exc, "status_code") and exc.status_code in (429, 500, 502, 503, 504):
        return True
    return False


def _get_retry_after(exc: Exception) -> float | None:
    """Extract Retry-After header from the exception if Azure sent one."""
    if isinstance(exc, RateLimitError) and exc.response is not None:
        retry_after = exc.response.headers.get("Retry-After") or exc.response.headers.get("retry-after")
        if retry_after:
            try:
                return float(retry_after)
            except (ValueError, TypeError):
                pass
    return None


def _compute_delay(attempt: int, retry_after: float | None) -> float:
    """
    Compute the wait time before the next retry.

    Uses the server's Retry-After if provided (with a small jitter),
    otherwise uses exponential back-off: BASE_DELAY * 2^attempt.
    """
    if retry_after is not None and retry_after > 0:
        # Respect the server's requested wait, add small jitter
        delay = retry_after + random.uniform(1.0, 3.0)
    else:
        # Exponential back-off with jitter
        delay = BASE_DELAY * (2 ** attempt)
        jitter = delay * JITTER_FACTOR * (2 * random.random() - 1)  # ±30%
        delay += jitter

    return min(delay, MAX_DELAY)


def _create_completion(client, model: str, **kwargs) -> str:
    """
    Call LLM with automatic retry on transient errors.

    Retries on: RateLimitError (429), APITimeoutError, APIConnectionError,
    InternalServerError (500/502/503/504).

    Does NOT retry on: BadRequestError (400), AuthenticationError (401),
    PermissionDeniedError (403), NotFoundError (404) — these are permanent.
    """
    extra = {}
    if model not in _models_without_temperature:
        extra["temperature"] = 0.0

    last_error: Exception | None = None

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = client.chat.completions.create(model=model, **extra, **kwargs)
            return response.choices[0].message.content

        except BadRequestError as e:
            if "temperature" in str(e):
                logger.info("Model does not support temperature=0, caching for future calls", model=model)
                _models_without_temperature.add(model)
                extra.pop("temperature", None)
                continue  # retry immediately without counting as a failure
            if "content_filter" in str(e):
                logger.warning(
                    "Azure content filter triggered — document will be classified as Others",
                    model=model,
                    error_code=getattr(e, "code", "unknown"),
                )
            raise  # all BadRequestErrors are permanent

        except Exception as e:
            last_error = e

            if not _is_retryable(e) or attempt == MAX_RETRIES:
                if attempt > 0:
                    logger.error(
                        "LLM call failed after retries",
                        model=model,
                        attempt=attempt + 1,
                        max_attempts=MAX_RETRIES + 1,
                        error=str(e),
                    )
                raise

            retry_after = _get_retry_after(e)
            delay = _compute_delay(attempt, retry_after)

            logger.warning(
                "LLM call failed (retryable) — cooling down",
                model=model,
                error_type=type(e).__name__,
                error=str(e)[:200],
                attempt=attempt + 1,
                max_attempts=MAX_RETRIES + 1,
                retry_after_header=retry_after,
                cooldown_seconds=round(delay, 1),
            )
            time.sleep(delay)

    raise last_error  # unreachable — satisfies type checkers


def call_llm_text(
    system_prompt: str,
    user_prompt: str,
    use_mini: bool = True,
) -> dict:
    client, model, mini_model = _get_client()
    selected_model = mini_model if use_mini else model

    logger.info("Calling LLM", model=selected_model, use_mini=use_mini)

    content = _create_completion(
        client,
        selected_model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        response_format={"type": "json_object"},
    )

    return json.loads(content)


def call_llm_vision(
    system_prompt: str,
    user_prompt: str,
    image_base64: str,
    use_mini: bool = False,
) -> dict:
    client, model, mini_model = _get_client()
    selected_model = mini_model if use_mini else model

    logger.info("Calling Vision LLM", model=selected_model)

    content = _create_completion(
        client,
        selected_model,
        messages=[
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_prompt},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{image_base64}",
                            "detail": "high",
                        },
                    },
                ],
            },
        ],
        response_format={"type": "json_object"},
    )

    return json.loads(content)
