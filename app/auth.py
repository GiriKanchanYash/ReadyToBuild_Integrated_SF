import base64
import hashlib
import hmac
import json
import os
import time
from typing import Literal

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

Role = Literal["admin", "planner", "sourcing", "exec"]

_SECRET = os.getenv("RTB_AUTH_SECRET", "rtb-dev-secret-change-me")
_bearer = HTTPBearer(auto_error=False)


class AuthUser(BaseModel):
    name: str
    role: Role


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def _sign(payload_b64: str) -> str:
    sig = hmac.new(_SECRET.encode("utf-8"), payload_b64.encode("utf-8"), hashlib.sha256).digest()
    return _b64url_encode(sig)


def create_access_token(name: str, role: Role, ttl_seconds: int = 60 * 60 * 8) -> str:
    payload = {
        "sub": name,
        "role": role,
        "iat": int(time.time()),
        "exp": int(time.time()) + ttl_seconds,
    }
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    return f"{payload_b64}.{_sign(payload_b64)}"


def parse_access_token(token: str) -> AuthUser:
    try:
        payload_b64, sig = token.split(".", 1)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token format") from e
    expected = _sign(payload_b64)
    if not hmac.compare_digest(sig, expected):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token signature")
    try:
        payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
        exp = int(payload.get("exp", 0))
        if exp <= int(time.time()):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")
        role = payload.get("role")
        name = payload.get("sub")
        if role not in {"admin", "planner", "sourcing", "exec"} or not name:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token claims")
        return AuthUser(name=str(name), role=role)  # type: ignore[arg-type]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload") from e


def get_current_user(credentials: HTTPAuthorizationCredentials | None = Depends(_bearer)) -> AuthUser:
    if not credentials:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    return parse_access_token(credentials.credentials)


def require_roles(*roles: Role):
    def checker(user: AuthUser = Depends(get_current_user)) -> AuthUser:
        if user.role not in roles:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permissions")
        return user

    return checker
