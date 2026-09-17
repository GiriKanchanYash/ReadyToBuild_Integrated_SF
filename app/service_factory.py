"""
Selects the Snowflake or Fabric backend implementation for CTB/AI data, based
on a `data_source` query param / header sent by the frontend dropdown.

STRUCTURAL NOTE: `backend/fabric_app/*.py` was written with flat, bare
imports (`from config import Config`, `from db import run_df`, etc.) rather
than this project's usual package-relative style (`from app.db import ...`).
That means fabric_app's modules expect their OWN directory on `sys.path`,
not to be imported as `app.fabric_app.xxx`. Rather than rewriting every
import in every fabric_app file (risky to do blind, without a live
environment to test against), this factory adds `backend/fabric_app` to
`sys.path` once, then imports `service` and `copilot_service` as top-level
modules the same way fabric_app's own files already import each other.

If you later want fabric_app to be a "real" subpackage (cleaner long-term),
the fix is: change every `from config import X` / `from db import X` /
`import service` / `import ai_service` inside backend/fabric_app/*.py to
`from . import config as X` / relative imports, then this factory can do a
normal `from app.fabric_app import service as fabric_service` instead.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
from types import ModuleType

logger = logging.getLogger(__name__)

_FABRIC_APP_DIR = os.path.join(os.path.dirname(__file__), "..", "fabric_app")
_FABRIC_APP_DIR = os.path.abspath(_FABRIC_APP_DIR)

_lock = threading.Lock()
_fabric_service: ModuleType | None = None
_fabric_copilot_service: ModuleType | None = None
_fabric_transfer_service: ModuleType | None = None
_fabric_import_error: Exception | None = None


def _load_fabric_modules() -> None:
    global _fabric_service, _fabric_copilot_service, _fabric_transfer_service, _fabric_import_error
    if _fabric_service is not None or _fabric_import_error is not None:
        return
    with _lock:
        if _fabric_service is not None or _fabric_import_error is not None:
            return
        try:
            if _FABRIC_APP_DIR not in sys.path:
                sys.path.insert(0, _FABRIC_APP_DIR)
            import service as _svc                    # noqa: E402  (fabric_app/service.py)
            import copilot_service as _cop             # noqa: E402  (fabric_app/copilot_service.py)
            import transfer_service as _trf            # noqa: E402  (fabric_app/transfer_service.py)
            _fabric_service = _svc
            _fabric_copilot_service = _cop
            _fabric_transfer_service = _trf
        except Exception as exc:  # pragma: no cover - surfaced to the caller
            # Log the FULL traceback here (which module/line actually
            # failed) - the RuntimeError raised to callers below only
            # carries str(exc), which for import-time AttributeErrors
            # (e.g. dependency version mismatches like aiohttp missing an
            # attribute a newer azure-identity/openai expects) hides which
            # of service.py/copilot_service.py/transfer_service.py (or one
            # of their imports) actually triggered it. Check this log line
            # first when debugging "Fabric backend is not available".
            logger.exception("Failed to import fabric_app modules")
            _fabric_import_error = exc


def get_ctb_service(data_source: str | None):
    """Return the CTB data-service module for 'snowflake' (default) or 'fabric'."""
    if (data_source or "snowflake").lower() == "fabric":
        _load_fabric_modules()
        if _fabric_import_error is not None:
            raise RuntimeError(
                f"Fabric backend is not available: {_fabric_import_error}"
            )
        return _fabric_service

    from app.services import snowflake_service
    return snowflake_service


def get_copilot_service(data_source: str | None):
    """Return the AI/Copilot service module for 'snowflake' (default) or 'fabric'."""
    if (data_source or "snowflake").lower() == "fabric":
        _load_fabric_modules()
        if _fabric_import_error is not None:
            raise RuntimeError(
                f"Fabric backend is not available: {_fabric_import_error}"
            )
        return _fabric_copilot_service

    from app.services import ai_service
    return ai_service


def get_transfer_service(data_source: str | None):
    """Return the Cross-Site Transfer service module for 'snowflake' (default)
    or 'fabric'. Previously routers/transfer.py imported
    app.services.transfer_service directly, so switching the frontend's Data
    Source dropdown to 'fabric' had no effect on the Transfer page - it kept
    hitting Snowflake. This mirrors get_ctb_service/get_copilot_service so
    the Transfer page respects the selected data source too."""
    if (data_source or "snowflake").lower() == "fabric":
        _load_fabric_modules()
        if _fabric_import_error is not None:
            raise RuntimeError(
                f"Fabric backend is not available: {_fabric_import_error}"
            )
        return _fabric_transfer_service

    from app.services import transfer_service
    return transfer_service
