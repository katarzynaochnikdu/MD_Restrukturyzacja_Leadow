"""
Pomocnicze funkcje do pobierania danych o użytkownikach Zoho CRM.
"""

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

from refresh_zoho_access_token import refresh_access_token

ZOHO_CRM_API_BASE_URL = "https://www.zohoapis.eu/crm/v8"
REQUEST_SLEEP_S = 0.2
MAX_RETRY_TIMEOUT = 2

USER_TOKEN: Dict[str, Any] = {"token": None, "refreshed_at": 0.0}


def get_access_token() -> str:
    client_id = os.environ.get("ZOHO_MEDIDESK_CLIENT_ID", "WPISZ_CLIENT_ID")
    client_secret = os.environ.get("ZOHO_MEDIDESK_CLIENT_SECRET", "WPISZ_CLIENT_SECRET")
    refresh_token = os.environ.get("ZOHO_MEDIDESK_REFRESH_TOKEN", "WPISZ_REFRESH_TOKEN")

    if any(val.startswith("WPISZ_") for val in (client_id, client_secret, refresh_token)):
        raise RuntimeError("Skonfiguruj zmienne środowiskowe Zoho OAuth.")

    token_info = refresh_access_token(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
    )
    USER_TOKEN["token"] = token_info["access_token"]
    USER_TOKEN["refreshed_at"] = time.time()
    return token_info["access_token"]


def _execute_request(access_token: str, url: str) -> Optional[Dict[str, Any]]:
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    request = urllib.request.Request(url, headers=headers, method="GET")
    time.sleep(REQUEST_SLEEP_S)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = response.read().decode("utf-8")
            return json.loads(payload)
    except urllib.error.HTTPError as exc:
        logging.error(f"HTTPError {exc.code}: {exc.reason}")
        if exc.code in (401, 403):
            return None
        return None
    except Exception as exc:
        logging.error(f"Błąd żądania użytkowników: {exc}")
        return None


def get_active_users(access_token: str) -> List[Dict[str, Any]]:
    """
    Pobiera aktywnych użytkowników Zoho (ActiveUsers) w obecnym orgu.
    """
    users: List[Dict[str, Any]] = []
    page = 1
    while True:
        params = urllib.parse.urlencode({"type": "ActiveUsers", "page": page})
        url = f"{ZOHO_CRM_API_BASE_URL}/users?{params}"
        payload = _execute_request(access_token, url)
        if not payload:
            break
        data = payload.get("users") or []
        users.extend(data)
        info = payload.get("info", {})
        if not info.get("more_records"):
            break
        page += 1
    return users
