import threading

import tiktoken

# tiktoken.encoding_for_model() and get_encoding() load a BPE vocabulary from
# disk on first call. Caching the resulting Encoding object avoids repeating
# that work for every file we classify.
_encoding_cache: dict[str, "tiktoken.Encoding"] = {}
_encoding_cache_lock = threading.Lock()


def _get_encoding(model: str) -> "tiktoken.Encoding":
    cached = _encoding_cache.get(model)
    if cached is not None:
        return cached
    with _encoding_cache_lock:
        cached = _encoding_cache.get(model)
        if cached is not None:
            return cached
        try:
            encoding = tiktoken.encoding_for_model(model)
        except KeyError:
            # Unknown model name (e.g. Azure deployment names like
            # "gpt-5.2-chat") — fall back to the general-purpose encoding.
            encoding = tiktoken.get_encoding("cl100k_base")
        _encoding_cache[model] = encoding
        return encoding


def count_tokens(text: str, model: str = "gpt-4o") -> int:
    return len(_get_encoding(model).encode(text))


def truncate_to_token_limit(text: str, max_tokens: int = 3500, model: str = "gpt-4o") -> str:
    encoding = _get_encoding(model)
    tokens = encoding.encode(text)
    if len(tokens) <= max_tokens:
        return text

    truncated_tokens = tokens[:max_tokens]
    return encoding.decode(truncated_tokens) + "\n\n[... content truncated for classification ...]"
