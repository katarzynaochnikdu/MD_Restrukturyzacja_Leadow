"""Narzędzie do odświeżania tokena dostępu Zoho CRM."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict


ACCOUNTS_TOKEN_URL = "https://accounts.zoho.eu/oauth/v2/token"


def refresh_access_token(client_id: str, client_secret: str, refresh_token: str) -> Dict[str, Any]:
    payload = urllib.parse.urlencode(
        {
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "refresh_token",
        }
    ).encode("utf-8")

    request = urllib.request.Request(
        ACCOUNTS_TOKEN_URL,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            data = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
        raise RuntimeError(f"Błąd HTTP {exc.code}: {body}") from exc

    payload_json = json.loads(data)
    if "access_token" not in payload_json:
        raise RuntimeError("Brak 'access_token' w odpowiedzi: " + data)
    return payload_json
