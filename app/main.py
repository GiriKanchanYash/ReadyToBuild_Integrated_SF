import math
import os
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, FileResponse
from fastapi.exceptions import HTTPException
from snowflake.connector.errors import ProgrammingError
from app.routers import ctb, ai, auth, transfer


def _sanitize_json_floats(obj):
    """Recursively replace NaN/Infinity floats with None.

    Starlette's default JSONResponse calls json.dumps(..., allow_nan=False)
    (correctly, per the JSON spec - browsers' JSON.parse can't read NaN/
    Infinity tokens either), so any leftover non-finite float anywhere in a
    response body turns into a hard 500 instead of a usable response. This
    is the safety net; the real fixes are the source-level ones in
    fabric_app/service.py and fabric_app/transfer_service.py, but this
    guarantees no endpoint (Snowflake OR Fabric, present or future) can
    crash this way again even if some other code path produces one.
    """
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _sanitize_json_floats(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_json_floats(v) for v in obj]
    return obj


class SafeJSONResponse(JSONResponse):
    """Drop-in JSONResponse that scrubs NaN/Infinity before encoding."""

    def render(self, content) -> bytes:
        return super().render(_sanitize_json_floats(content))


app = FastAPI(
    title="ReadyToBuild API",
    version="1.0.0",
    default_response_class=SafeJSONResponse,
)

_default_origins = "http://localhost:5173,http://localhost:3000"
_origins = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", _default_origins).split(",") if o.strip()]
_allow_azure = os.getenv("ALLOW_AZURE_ORIGINS", "true").lower() in {"1", "true", "yes"}

app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_origin_regex=r"https://.*\.azurewebsites\.net" if _allow_azure else None,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ctb.router)
app.include_router(ai.router)
app.include_router(auth.router)
app.include_router(transfer.router)


@app.exception_handler(ProgrammingError)
async def handle_snowflake_programming_error(_request, exc: ProgrammingError):
    msg = str(exc)
    if "resource monitor" in msg.lower() and "has exceeded its quota" in msg.lower():
        return JSONResponse(
            status_code=503,
            content={
                "detail": (
                    "Snowflake warehouse quota exceeded. "
                    "Ask your Snowflake admin to resume/increase resource monitor quota "
                    "or switch to an available warehouse."
                )
            },
        )
    return JSONResponse(status_code=500, content={"detail": msg})


@app.get("/health")
def health():
    return {"status": "ok"}


_static_dir = os.path.join(os.path.dirname(__file__), "..", "static")
if os.path.isdir(_static_dir):
    app.mount("/assets", StaticFiles(directory=os.path.join(_static_dir, "assets")), name="assets")


@app.exception_handler(404)
async def spa_fallback(request: Request, exc: HTTPException):
    path = request.url.path
    if path.startswith("/api") or path == "/health":
        return JSONResponse(status_code=404, content={"detail": "Not found"})
    if os.path.isdir(_static_dir):
        index = os.path.join(_static_dir, "index.html")
        if os.path.isfile(index):
            return FileResponse(
                index,
                headers={
                    "Cache-Control": "no-cache, no-store, must-revalidate",
                    "Pragma": "no-cache",
                },
            )
    return JSONResponse(status_code=404, content={"detail": "Not found"})
