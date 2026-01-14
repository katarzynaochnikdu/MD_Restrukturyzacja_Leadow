"""
Wspólny klient do komunikacji z Zoho CRM API v8.
Wyodrębnia obsługę tokena, retry, pagination — używany przez wszystkie fetchery.
"""

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Dict, Iterator, List, Optional

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

from refresh_zoho_access_token import refresh_access_token

# --- Konfiguracja API Zoho ---
ZOHO_CRM_API_BASE_URL = "https://www.zohoapis.eu/crm/v8"
REQUEST_SLEEP_S = 0.2
MAX_RETRY_TIMEOUT = 3
RECORDS_PER_PAGE = 200
MAX_FIELDS_PER_REQUEST = 45

# Stan globalny tokena
_TOKEN_STATE: Dict[str, Any] = {"token": None, "refreshed_at": 0.0}

logger = logging.getLogger(__name__)


def get_access_token(force_refresh: bool = False) -> str:
    """
    Pobiera token dostępowy (odświeża jeśli potrzeba lub force_refresh=True).
    """
    client_id = os.environ.get("ZOHO_MEDIDESK_CLIENT_ID", "")
    client_secret = os.environ.get("ZOHO_MEDIDESK_CLIENT_SECRET", "")
    refresh_token_val = os.environ.get("ZOHO_MEDIDESK_REFRESH_TOKEN", "")

    if not all([client_id, client_secret, refresh_token_val]):
        raise RuntimeError(
            "Brak konfiguracji OAuth Zoho. Ustaw zmienne środowiskowe:\n"
            "ZOHO_MEDIDESK_CLIENT_ID, ZOHO_MEDIDESK_CLIENT_SECRET, ZOHO_MEDIDESK_REFRESH_TOKEN"
        )

    last_refresh = float(_TOKEN_STATE.get("refreshed_at") or 0.0)
    # Odświeżaj nie częściej niż co 15 sekund, chyba że force
    if not force_refresh and _TOKEN_STATE.get("token") and (time.time() - last_refresh < 15.0):
        return _TOKEN_STATE["token"]

    token_info = refresh_access_token(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token_val,
    )
    _TOKEN_STATE["token"] = token_info["access_token"]
    _TOKEN_STATE["refreshed_at"] = time.time()
    logger.debug("Token odświeżony")
    return token_info["access_token"]


def _do_request(url: str, access_token: str) -> Optional[Dict[str, Any]]:
    """Wykonuje pojedyncze żądanie GET do API."""
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    request = urllib.request.Request(url, headers=headers, method="GET")
    time.sleep(REQUEST_SLEEP_S)
    logger.debug(f"GET {url}")
    with urllib.request.urlopen(request, timeout=60) as response:
        data = response.read().decode("utf-8")
        if not data:
            return None
        return json.loads(data)


def execute_api_request(url: str, access_token: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Wykonuje żądanie API z obsługą retry i automatycznego odświeżania tokena.
    """
    token = access_token or _TOKEN_STATE.get("token") or get_access_token()

    for attempt in range(MAX_RETRY_TIMEOUT + 1):
        try:
            return _do_request(url, token)
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                pass
            logger.warning(f"HTTPError {e.code} for {url}: {body[:500]}")

            if e.code in (401, 403):
                # Token wygasł — odśwież i spróbuj ponownie
                token = get_access_token(force_refresh=True)
                continue
            elif e.code == 429:
                # Rate limit — czekaj i ponów
                wait_time = 2 ** attempt
                logger.warning(f"Rate limit, czekam {wait_time}s...")
                time.sleep(wait_time)
                continue
            else:
                return None
        except Exception as e:
            err_text = str(e)
            if "timed out" in err_text or "10060" in err_text:
                wait_time = 2 + attempt * 3
                logger.warning(f"Timeout, czekam {wait_time}s... (próba {attempt + 1})")
                time.sleep(wait_time)
                continue
            logger.error(f"Błąd API ({url}): {e}")
            return None
    return None


def chunk_fields(all_fields: List[str], chunk_size: int = MAX_FIELDS_PER_REQUEST) -> List[List[str]]:
    """
    Dzieli listę pól na mniejsze paczki (zawsze z 'id').
    """
    unique_fields = []
    seen = set()
    for field in all_fields:
        if field not in seen:
            seen.add(field)
            unique_fields.append(field)

    chunks = []
    for i in range(0, len(unique_fields), chunk_size):
        chunk = unique_fields[i:i + chunk_size]
        if "id" not in chunk:
            chunk = ["id"] + chunk[:chunk_size - 1]
        chunks.append(chunk)
    return chunks


def fetch_all_records(
    module: str,
    fields: Optional[List[str]] = None,
    criteria: Optional[str] = None,
    access_token: Optional[str] = None,
    show_progress: bool = True,
) -> List[Dict[str, Any]]:
    """
    Pobiera WSZYSTKIE rekordy z modułu z paginacją.

    Args:
        module: Nazwa modułu API (np. "Marketing_Leads", "Leads", "Deals")
        fields: Lista pól do pobrania (None = wszystkie domyślne)
        criteria: Opcjonalne kryteria COQL do filtrowania
        access_token: Token (opcjonalnie, domyślnie z get_access_token)
        show_progress: Czy pokazywać progress bar

    Returns:
        Lista rekordów (dict).
    """
    token = access_token or get_access_token()
    all_records: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    page = 1

    # Progress bar (unknown total)
    pbar = None
    if show_progress and HAS_TQDM:
        pbar = tqdm(desc=f"  {module}", unit=" rec", leave=False)

    while True:
        params: Dict[str, Any] = {"per_page": RECORDS_PER_PAGE}
        if fields:
            params["fields"] = ",".join(fields)
        if page_token:
            params["page_token"] = page_token
        else:
            params["page"] = page

        url = f"{ZOHO_CRM_API_BASE_URL}/{module}?{urllib.parse.urlencode(params)}"
        payload = execute_api_request(url, token) or {}
        data = payload.get("data") or []
        all_records.extend(data)

        if pbar is not None:
            pbar.update(len(data))

        info = payload.get("info") or {}
        if not info.get("more_records"):
            break

        page_token = info.get("next_page_token") or info.get("page_token")
        if not page_token:
            logger.warning(f"Brak page_token mimo more_records=True dla {module}. Przerywam.")
            break
        page += 1

    if pbar is not None:
        pbar.close()

    logger.info(f"Pobrano {len(all_records)} rekordów z {module}")
    return all_records


def fetch_records_chunked_fields(
    module: str,
    all_fields: List[str],
    access_token: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Pobiera rekordy z modułu, dzieląc pola na paczki i łącząc wyniki po ID.
    Używane gdy moduł ma >50 pól.

    Args:
        module: Nazwa modułu API
        all_fields: Pełna lista pól do pobrania
        access_token: Token (opcjonalnie)

    Returns:
        Lista rekordów z połączonymi polami.
    """
    token = access_token or get_access_token()
    field_chunks = chunk_fields(all_fields)
    logger.info(f"Pobieram {module} w {len(field_chunks)} paczkach pól")

    combined: Dict[str, Dict[str, Any]] = {}

    for idx, chunk in enumerate(field_chunks, 1):
        logger.info(f"Paczka {idx}/{len(field_chunks)}: {len(chunk)} pól")
        records = fetch_all_records(module, fields=chunk, access_token=token)
        for record in records:
            record_id = record.get("id")
            if not record_id:
                continue
            combined.setdefault(record_id, {}).update(record)

    result = list(combined.values())
    logger.info(f"Połączono {len(result)} unikalnych rekordów z {module}")
    return result


def fetch_related_list(
    module: str,
    record_id: str,
    related_list_api_name: str,
    fields: Optional[List[str]] = None,
    access_token: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Pobiera related list dla konkretnego rekordu.

    Args:
        module: Moduł nadrzędny (np. "Leads")
        record_id: ID rekordu nadrzędnego
        related_list_api_name: Nazwa API related list (np. "Events", "Events_History")
        fields: Opcjonalne pola do pobrania
        access_token: Token (opcjonalnie)

    Returns:
        Lista powiązanych rekordów.
    """
    token = access_token or get_access_token()
    all_related: List[Dict[str, Any]] = []
    page = 1

    while True:
        params: Dict[str, Any] = {"per_page": RECORDS_PER_PAGE, "page": page}
        if fields:
            params["fields"] = ",".join(fields)

        url = f"{ZOHO_CRM_API_BASE_URL}/{module}/{record_id}/{related_list_api_name}?{urllib.parse.urlencode(params)}"
        payload = execute_api_request(url, token) or {}
        data = payload.get("data") or []
        all_related.extend(data)

        info = payload.get("info") or {}
        if not info.get("more_records"):
            break
        page += 1

    return all_related


def flatten_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Spłaszcza zagnieżdżone struktury w rekordzie (lookup fields, listy).
    """
    flattened = {}
    for key, value in record.items():
        if value is None:
            flattened[key] = ""
        elif isinstance(value, dict):
            if "id" in value and "name" in value:
                flattened[f"{key}_id"] = value["id"]
                flattened[f"{key}_name"] = value["name"]
                flattened[key] = value["name"]
            elif "name" in value:
                flattened[key] = value["name"]
            elif "id" in value:
                flattened[key] = value["id"]
            else:
                flattened[key] = json.dumps(value, ensure_ascii=False)
        elif isinstance(value, list):
            str_items = []
            for item in value:
                if isinstance(item, dict):
                    str_items.append(item.get("name") or item.get("id") or json.dumps(item, ensure_ascii=False))
                else:
                    str_items.append(str(item))
            flattened[key] = "; ".join(str_items)
        else:
            flattened[key] = value
    return flattened
