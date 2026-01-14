import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Optional, Tuple


REGION_TO_ACCOUNTS: Dict[str, str] = {
    "com": "https://accounts.zoho.com",
    "eu": "https://accounts.zoho.eu",
    "in": "https://accounts.zoho.in",
    "jp": "https://accounts.zoho.jp",
    "au": "https://accounts.zoho.com.au",
    "ca": "https://accounts.zohocloud.ca",
    "sa": "https://accounts.zoho.com.br",
}


def _accounts_base(region: str) -> str:
    base = REGION_TO_ACCOUNTS.get(region.lower())
    if not base:
        raise ValueError(
            "Nieznany region. Użyj jednego z: " + ", ".join(sorted(REGION_TO_ACCOUNTS))
        )
    return base


def build_auth_url(
    *,
    client_id: str,
    redirect_uri: str,
    scopes: str,
    region: str = "eu",
    access_type: str = "offline",
    prompt: str = "consent",
    state: Optional[str] = None,
) -> str:
    base = _accounts_base(region)
    query = {
        "response_type": "code",
        "client_id": client_id,
        "scope": scopes,
        "redirect_uri": redirect_uri,
        "access_type": access_type,
        "prompt": prompt,
    }
    if state:
        query["state"] = state
    return f"{base}/oauth/v2/auth?{urllib.parse.urlencode(query)}"


def exchange_code_for_tokens(
    *,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    code: str,
    region: str = "eu",
) -> Dict[str, Any]:
    base = _accounts_base(region)
    url = f"{base}/oauth/v2/token"
    data = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "code": code,
    }
    encoded = urllib.parse.urlencode(data).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=encoded,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload_text = response.read().decode("utf-8")
    except urllib.error.HTTPError as http_err:
        detail = http_err.read().decode("utf-8", errors="replace") if hasattr(http_err, "read") else ""
        raise RuntimeError(f"Błąd HTTP {http_err.code}: {detail}") from http_err
    except urllib.error.URLError as url_err:
        raise RuntimeError(f"Błąd połączenia: {url_err}") from url_err

    try:
        payload: Dict[str, Any] = json.loads(payload_text)
    except json.JSONDecodeError as json_err:
        raise RuntimeError("Niepoprawna odpowiedź z serwera (brak JSON)") from json_err

    return payload


class _CodeCatcher(BaseHTTPRequestHandler):
    server_version = "ZohoCodeCatcher/1.0"
    code_value: Optional[str] = None
    error_value: Optional[str] = None

    def do_GET(self) -> None:  # noqa: N802 - nazwa wymagana przez BaseHTTPRequestHandler
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        if "code" in params:
            _CodeCatcher.code_value = params["code"][0]
            content = b"Autoryzacja zako\xc5\x84czona. Mo\xc5\xbcesz zamkn\xc4\x85\xc4\x87 to okno."
            self._write_response(200, content)
        elif "error" in params:
            _CodeCatcher.error_value = params["error"][0]
            content = b"Wyst\xc4\x85pi\xc5\x82 b\xc5\x82\xc4\x85d autoryzacji."
            self._write_response(400, content)
        else:
            self._write_response(404, b"Not Found")

    def log_message(self, fmt: str, *args: Any) -> None:  # wyciszenie logów
        return

    def _write_response(self, status: int, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _maybe_listen_for_code(redirect_uri: str, timeout_seconds: int = 180) -> Tuple[Optional[str], Optional[str]]:
    parsed = urllib.parse.urlparse(redirect_uri)
    if parsed.scheme != "http" or parsed.hostname not in {"localhost", "127.0.0.1"}:
        return None, None

    host = parsed.hostname or "localhost"
    port = parsed.port or 80
    server = HTTPServer((host, port), _CodeCatcher)

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    start = time.time()
    try:
        while time.time() - start < timeout_seconds:
            if _CodeCatcher.code_value or _CodeCatcher.error_value:
                break
            time.sleep(0.1)
    finally:
        server.shutdown()
        server.server_close()

    return _CodeCatcher.code_value, _CodeCatcher.error_value


def get_initial_refresh_token(
    *,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    scopes: str,
    region: str = "eu",
    open_browser: bool = True,
) -> str:
    auth_url = build_auth_url(
        client_id=client_id,
        redirect_uri=redirect_uri,
        scopes=scopes,
        region=region,
    )

    if open_browser:
        try:
            webbrowser.open(auth_url)
        except Exception:
            pass

    code, error = _maybe_listen_for_code(redirect_uri)
    if not code and not error:
        # Brak lokalnego listenera – użytkownik musi wkleić code ręcznie.
        raise RuntimeError(
            "Otwórz URL w przeglądarce, zaloguj się i podaj code: " + auth_url
        )
    if error:
        raise RuntimeError(f"Błąd autoryzacji: {error}")

    payload = exchange_code_for_tokens(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        code=code or "",
        region=region,
    )

    refresh_token = payload.get("refresh_token")
    if not refresh_token:
        raise RuntimeError(
            "Brak 'refresh_token' w odpowiedzi. Upewnij się, że użyłeś access_type=offline i prompt=consent."
        )
    return refresh_token
