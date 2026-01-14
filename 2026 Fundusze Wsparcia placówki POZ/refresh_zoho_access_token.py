import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict


ACCOUNTS_TOKEN_URL: str = "https://accounts.zoho.eu/oauth/v2/token"


def refresh_access_token(
    client_id: str, client_secret: str, refresh_token: str
) -> Dict[str, Any]:
    """
    Generuje nowy token dostępowy na podstawie refresh tokena.

    Args:
        client_id: Identyfikator klienta.
        client_secret: Sekret klienta.
        refresh_token: Refresh token.

    Returns:
        Słownik z odpowiedzią z API (w tym nowy access token).

    Raises:
        RuntimeError: W przypadku błędu HTTP, błędu połączenia lub nieprawidłowej odpowiedzi.
    """
    data = {
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
    }
    encoded = urllib.parse.urlencode(data).encode("utf-8")

    request = urllib.request.Request(
        ACCOUNTS_TOKEN_URL,
        data=encoded,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload_text = response.read().decode("utf-8")
    except urllib.error.HTTPError as http_err:
        detail = (
            http_err.read().decode("utf-8", errors="replace")
            if hasattr(http_err, "read")
            else ""
        )
        raise RuntimeError(f"Błąd HTTP {http_err.code}: {detail}") from http_err
    except urllib.error.URLError as url_err:
        raise RuntimeError(f"Błąd połączenia: {url_err}") from url_err

    try:
        payload: Dict[str, Any] = json.loads(payload_text)
    except json.JSONDecodeError as json_err:
        raise RuntimeError(
            "Niepoprawna odpowiedź z serwera (brak JSON)"
        ) from json_err

    if "access_token" not in payload:
        raise RuntimeError("Brak 'access_token' w odpowiedzi: " + payload_text)

    return payload


