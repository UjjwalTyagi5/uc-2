"""Azure Blob File Ops — AgentCore custom-code component.

Standalone single-file component that uploads a file to or deletes a file
from Azure Blob Storage at a caller-supplied path. Built around the
existing AgentCore "azure_blob" connector (Settings → Connectors), so the
operator does not have to embed account URLs / container names in the
canvas.

Behaviour
---------
- operation = "upload": writes bytes from the wired File Data input (or
  from File Bytes Path on disk) to <container>/<blob_path>, overwriting
  any existing blob at that path.
- operation = "delete": deletes the single blob at <container>/<blob_path>.
  Missing blobs are treated as a no-op (success).

Auth
----
Uses DefaultAzureCredential so the same code works on dev VMs (az login
→ AzureCliCredential) and AgentCore hosts (Managed Identity). The
credential is cached at module scope and rebuilt on transient
token-refresh failures via _with_az_retry (mirrors pipeline_stage_123_v3).
"""
from __future__ import annotations

import threading
import time
from typing import Optional

from loguru import logger

try:
    from azure.identity import DefaultAzureCredential  # noqa: F401
    from azure.storage.blob import BlobServiceClient    # noqa: F401
except ImportError:
    pass

from agentcore.custom import Node
from agentcore.io import FileInput, HandleInput, MessageTextInput, Output
from agentcore.schema.data import Data
from agentcore.schema.message import Message


# ── Cached Azure credential ───────────────────────────────────────────────────
# Module-level cached Azure credential. We keep DefaultAzureCredential so
# the same code works on dev VMs (az login → AzureCliCredential) and in
# production AgentCore hosts (system-assigned Managed Identity).
#
# Caching matters: without it, every blob op constructs a fresh credential
# and re-runs the whole chain, which hammers IMDS (169.254.169.254) and
# causes intermittent "Failed to invoke the Azure CLI" errors when MI
# briefly fails and the chain falls through. One cached instance reuses
# its internal token cache across all calls.
#
# Token refresh resilience: every ~50-60 min the SDK refreshes the bearer
# token. If IMDS is briefly unreachable at that exact moment, the cached
# credential starts returning auth errors on every call. _with_az_retry()
# catches those, forces a fresh credential build, and retries the op once
# — recovering transparently from IMDS hiccups instead of cascading the
# failure up to the caller.

_AZ_CRED = None
_AZ_CRED_LOCK = threading.Lock()


def _get_az_credential(force_rebuild: bool = False):
    """Return the shared DefaultAzureCredential. Pass force_rebuild=True to
    discard the cached instance and create a fresh one — used by the retry
    wrapper after a transient IMDS / token-refresh failure."""
    global _AZ_CRED
    if force_rebuild:
        with _AZ_CRED_LOCK:
            old = _AZ_CRED
            _AZ_CRED = None
            if old is not None:
                try:
                    if hasattr(old, "close"):
                        old.close()
                except Exception:
                    pass
    if _AZ_CRED is not None:
        return _AZ_CRED
    with _AZ_CRED_LOCK:
        if _AZ_CRED is None:
            from azure.identity import DefaultAzureCredential
            # DefaultAzureCredential's built-in chain: WorkloadIdentity →
            # ManagedIdentity (IMDS) → SharedTokenCache → AzureCli →
            # AzurePowerShell → AzureDeveloperCli. Env + interactive
            # browser are excluded so a stray dev env var or pop-up
            # browser never wins over MI in production.
            _AZ_CRED = DefaultAzureCredential(
                exclude_environment_credential=True,
                exclude_interactive_browser_credential=True,
            )
    return _AZ_CRED


def _is_az_auth_error(exc: BaseException) -> bool:
    """True if exc looks like an Azure credential / token-acquisition failure
    (worth retrying with a fresh credential) vs a real blob-storage error
    (which a retry would not fix)."""
    try:
        from azure.core.exceptions import ClientAuthenticationError
        if isinstance(exc, ClientAuthenticationError):
            return True
    except Exception:
        pass
    msg = str(exc).lower()
    return any(s in msg for s in (
        "failed to invoke the azure cli",
        "managedidentitycredential",
        "defaultazurecredential failed",
        "no credential in this chain",
        "credentialunavailable",
        "azurecli",
    ))


def _with_az_retry(fn, *args, on_rebuild=None, max_attempts: int = 2, **kwargs):
    """Run fn(*args, **kwargs); on a transient Azure auth failure, force-rebuild
    the cached credential, invoke on_rebuild() (typically to invalidate any
    BlobServiceClient that was created with the now-stale credential), and
    retry once. Non-auth exceptions propagate immediately."""
    last_exc: BaseException | None = None
    for attempt in range(max_attempts):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            if not _is_az_auth_error(exc) or attempt == max_attempts - 1:
                raise
            last_exc = exc
            try:
                logger.warning(
                    f"Azure auth transient failure (attempt {attempt + 1}/{max_attempts}): "
                    f"{type(exc).__name__}: {exc} — rebuilding credential and retrying"
                )
            except Exception:
                pass
            _get_az_credential(force_rebuild=True)
            if on_rebuild is not None:
                try:
                    on_rebuild()
                except Exception:
                    pass
            time.sleep(min(2 ** attempt, 5))
    if last_exc is not None:
        raise last_exc


# ── Connector-catalogue lookup ────────────────────────────────────────────────

def _get_blob_config_by_name(connector_name: str) -> dict:
    name = (connector_name or "").strip()
    if not name:
        raise ValueError("blob_connector_name is empty.")
    try:
        import asyncio
        import concurrent.futures as _cf
        from sqlalchemy import select

        async def _fetch():
            from agentcore.services.deps import get_db_service
            from agentcore.services.database.models.connector_catalogue.model import ConnectorCatalogue
            db_service = get_db_service()
            async with db_service.with_session() as session:
                stmt = (
                    select(ConnectorCatalogue)
                    .where(ConnectorCatalogue.name == name)
                    .where(ConnectorCatalogue.provider == "azure_blob")
                )
                result = await session.execute(stmt)
                row = result.scalars().first()
                if row is None:
                    raise ValueError(f"No azure_blob connector named {name!r}.")
                cfg         = row.provider_config or {}
                account_url = (cfg.get("account_url") or row.host or "").strip()
                container   = (cfg.get("container_name") or row.database_name or "").strip()
                from urllib.parse import urlparse
                if not urlparse(account_url).netloc:
                    raise ValueError(f"Invalid Storage Account URL: {account_url!r}")
                if not container:
                    raise ValueError(f"No container_name in connector {name!r}.")
                return {"account_url": account_url, "container_name": container}

        try:
            asyncio.get_running_loop()
            with _cf.ThreadPoolExecutor() as pool:
                return pool.submit(asyncio.run, _fetch()).result(timeout=10)
        except RuntimeError:
            return asyncio.run(_fetch())
    except Exception as exc:
        raise RuntimeError(f"Blob connector lookup failed: {exc}") from exc


# ── Input bytes extraction ────────────────────────────────────────────────────

def _bytes_from_data(data: Optional[Data]) -> Optional[bytes]:
    """Pull raw bytes out of a wired Data payload. Supports the common
    shapes emitted by AgentCore file-loader / blob-reader nodes."""
    if data is None:
        return None
    payload = getattr(data, "data", None) or {}
    for key in ("bytes", "file_bytes", "content", "data"):
        val = payload.get(key)
        if isinstance(val, (bytes, bytearray)):
            return bytes(val)
        if isinstance(val, str) and key in ("content", "data"):
            return val.encode("utf-8")
    return None


# ── Main component ────────────────────────────────────────────────────────────

class AzureBlobFileOps(Node):
    display_name = "Azure Blob File Ops (Upload / Delete)"
    description = (
        "Upload a file to or delete a file from Azure Blob Storage at the "
        "given path. Resolves storage account + container via the named "
        "azure_blob connector from Settings → Connectors."
    )
    icon = "Database"
    name = "AzureBlobFileOps"

    inputs = [
        MessageTextInput(
            name="blob_connector_name",
            display_name="Azure Blob Connector Name",
            value="",
            info="Exact name of your Azure Blob connector in Settings → Connectors.",
        ),
        MessageTextInput(
            name="operation",
            display_name="Operation",
            value="upload",
            info='Either "upload" or "delete".',
        ),
        MessageTextInput(
            name="blob_path",
            display_name="Blob Path",
            value="",
            info=(
                "Full path within the container, e.g. "
                "procurement/2026-04/quote.pdf"
            ),
        ),
        FileInput(
            name="local_file",
            display_name="File to Upload",
            file_types=["*"],
            required=False,
            info=(
                "Upload your file directly on the node. AgentCore stores "
                "it in its file store (the 'Knowledge Base' picker — used "
                "purely as a staging area; no embeddings are created). "
                "The component reads the bytes from there and writes them "
                "to your Blob Path."
            ),
        ),
        HandleInput(
            name="file_data",
            display_name="File Data (optional, upload only)",
            input_types=["Data"],
            required=False,
            info=(
                "Alternative source: wire any node whose Data payload "
                "carries bytes. Accepted shapes: data['bytes'] / "
                "data['file_bytes'] = raw bytes (preferred); "
                "data['content'] / data['data'] = bytes or utf-8 text."
            ),
        ),
        MessageTextInput(
            name="file_bytes_path",
            display_name="File Bytes Path (optional, upload only)",
            value="",
            advanced=True,
            info=(
                "Absolute path to a file on the AgentCore host (only "
                "useful for self-hosted setups). Used only when Chat "
                "Message and File Data are both empty."
            ),
        ),
    ]

    outputs = [
        Output(display_name="Result", name="result", method="run_op", types=["Data"]),
        Output(
            display_name="Chat Response",
            name="chat_response",
            method="chat_response",
            types=["Message"],
            info="Wire into a Chat Output node to surface the outcome in the chat panel.",
        ),
    ]

    # ── Cached blob clients ────────────────────────────────────────────────

    def _blob_cfg(self) -> dict:
        if not hasattr(self, "_blob_config_cache"):
            self._blob_config_cache = _get_blob_config_by_name(self.blob_connector_name)
        return self._blob_config_cache

    def _container_client(self):
        """Return a cached ContainerClient — credential is initialised once per component instance."""
        if not hasattr(self, "_container_client_cache"):
            from azure.storage.blob import BlobServiceClient
            cfg        = self._blob_cfg()
            credential = _get_az_credential()
            self._container_client_cache = BlobServiceClient(
                account_url=cfg["account_url"],
                credential=credential,
            ).get_container_client(cfg["container_name"])
        return self._container_client_cache

    def _invalidate_container_client(self) -> None:
        """Drop the cached ContainerClient so the next call rebuilds it with
        a freshly-acquired credential. Called by _with_az_retry after a
        transient token-refresh failure — otherwise the cached client would
        keep using the stale credential it was constructed with."""
        cached = getattr(self, "_container_client_cache", None)
        if cached is None:
            return
        try:
            if hasattr(cached, "close"):
                cached.close()
        except Exception:
            pass
        try:
            del self._container_client_cache
        except Exception:
            pass

    def _safe_log(self, message: str) -> None:
        try:
            self.log(message)
        except Exception:
            logger.info(message)

    # ── Core ops ───────────────────────────────────────────────────────────

    def _upload_blob(self, raw: bytes, blob_path: str) -> None:
        def _do() -> None:
            self._container_client().get_blob_client(blob_path).upload_blob(
                raw, overwrite=True
            )

        _with_az_retry(_do, on_rebuild=self._invalidate_container_client)

    def _delete_blob(self, blob_path: str) -> bool:
        """Delete a single blob. Returns True if a blob existed, False if not."""
        from azure.core.exceptions import ResourceNotFoundError

        def _do() -> bool:
            try:
                self._container_client().get_blob_client(blob_path).delete_blob()
                return True
            except ResourceNotFoundError:
                return False

        return _with_az_retry(_do, on_rebuild=self._invalidate_container_client)

    def _resolve_upload_bytes(self) -> tuple[bytes, str]:
        """Return (bytes, source-label) for the upload payload.

        Resolution order:
          1. local_file       — file uploaded via the canvas FileInput (KB-staged)
          2. file_data        — wired Data from an upstream node
          3. file_bytes_path  — typed absolute path on the host (self-hosted)
        """
        local_path = (getattr(self, "local_file", "") or "").strip()
        if local_path:
            with open(local_path, "rb") as fh:
                return fh.read(), f"canvas upload ({local_path})"

        wired = getattr(self, "file_data", None)
        raw = _bytes_from_data(wired)
        if raw is not None:
            return raw, "wired File Data"

        path = (getattr(self, "file_bytes_path", "") or "").strip()
        if path:
            with open(path, "rb") as fh:
                return fh.read(), f"host path ({path})"

        raise ValueError(
            "Upload requires one of: File to Upload (canvas), File Data "
            "(wired), or File Bytes Path. None were provided."
        )

    # ── Output methods ─────────────────────────────────────────────────────

    def run_op(self) -> Data:
        op        = (getattr(self, "operation", "") or "").strip().lower()
        blob_path = (getattr(self, "blob_path", "") or "").strip()
        if not blob_path:
            raise ValueError("Blob Path is empty.")
        if op not in ("upload", "delete"):
            raise ValueError(f'Operation must be "upload" or "delete" (got {op!r}).')

        cfg = self._blob_cfg()
        if op == "upload":
            raw, source = self._resolve_upload_bytes()
            self._safe_log(
                f"Uploading {len(raw)} byte(s) from {source} to "
                f"{cfg['container_name']}/{blob_path}"
            )
            self._upload_blob(raw, blob_path)
            result = {
                "operation":      "upload",
                "container_name": cfg["container_name"],
                "blob_path":      blob_path,
                "bytes_written":  len(raw),
                "source":         source,
                "success":        True,
            }
            self._safe_log(f"Upload complete — {len(raw)} byte(s) → {blob_path}")
        else:
            self._safe_log(
                f"Deleting {cfg['container_name']}/{blob_path}"
            )
            existed = self._delete_blob(blob_path)
            result = {
                "operation":      "delete",
                "container_name": cfg["container_name"],
                "blob_path":      blob_path,
                "existed":        existed,
                "success":        True,
            }
            self._safe_log(
                f"Delete complete — {'removed' if existed else 'no-op (blob did not exist)'}: "
                f"{blob_path}"
            )

        self._last_result = result
        return Data(data=result)

    def chat_response(self) -> Message:
        """Chat-facing summary of the operation. Wire into a Chat Output node."""
        result = getattr(self, "_last_result", None)
        if result is None:
            self.run_op()
            result = getattr(self, "_last_result", None)
        if result is None:
            return Message(text="Azure Blob File Ops: no operation has been run yet.")

        container = result["container_name"]
        path      = result["blob_path"]
        if result["operation"] == "upload":
            text = (
                f"**Upload succeeded**\n"
                f"- Container: `{container}`\n"
                f"- Path: `{path}`\n"
                f"- Bytes written: {result['bytes_written']}"
            )
        else:
            outcome = "Blob deleted." if result["existed"] else "No matching blob found (no-op)."
            text = (
                f"**Delete succeeded**\n"
                f"- Container: `{container}`\n"
                f"- Path: `{path}`\n"
                f"- {outcome}"
            )
        return Message(text=text)
