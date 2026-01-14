"""
Skrypt do wyszukiwania WSZYSTKICH firm (nie tylko POZ) które:
1. Status_klienta != "jest"
2. Miały kiedykolwiek spotkanie (Events powiązane z Lead lub Deal)
3. Nie mają otwartego Deala (lub Deal jest na wykluczonych etapach)
4. Wyniki podzielone według Adres_w_rekordzie z ID placówek i województwami
5. Dodatkowa kolumna: Czy Specjalizacja zawiera POZ (True/False)
"""

import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import argparse
import pickle
import math
import glob
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import pandas as pd
except ImportError:
    print("Błąd: Wymagana biblioteka pandas nie jest zainstalowana.", file=sys.stderr)
    print("Proszę ją zainstalować komendą: pip install pandas", file=sys.stderr)
    sys.exit(1)

try:
    import openpyxl
except ImportError:
    print("Błąd: Wymagana biblioteka openpyxl nie jest zainstalowana.", file=sys.stderr)
    print("Proszę ją zainstalować komendą: pip install openpyxl", file=sys.stderr)
    sys.exit(1)

try:
    from tqdm import tqdm
except ImportError:
    print("Błąd: Wymagana biblioteka tqdm nie jest zainstalowana.", file=sys.stderr)
    print("Proszę ją zainstalować komendą: pip install tqdm", file=sys.stderr)
    sys.exit(1)

from refresh_zoho_access_token import refresh_access_token

# --- Konfiguracja API Zoho ---
ZOHO_CRM_API_BASE_URL = "https://www.zohoapis.eu/crm/v3"
ZOHO_CRM_COQL_URL = f"{ZOHO_CRM_API_BASE_URL}/coql"
REQUEST_SLEEP_S = 0.2
MAX_RETRY_TIMEOUT = 2
COQL_IN_MAX_VALUES = 50

# Aktualny token
ACCESS_TOKEN_HOLDER: Dict[str, Any] = {"token": None, "refreshed_at": 0.0}

# --- Definicje statusów wykluczonych Dealów ---
WYKLUCZONE_DEAL_ETAPY = [
    "Spotkania",
    "Oferta (Value Proposition)",
    "Negocjace (Negotiation/Review)",
    "Oferta zaakceptowana (Akceptacja oferty)",
    "Umowa w podpisie (Umowa)",
    "Closed Won",
    "Wdrożenie",
    "Trial",
    "Wdrożeni Klienci",
]

# --- Konfiguracja logowania ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("search_all_companies.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)


def get_access_token() -> str:
    """Pobiera token dostępowy z env lub refresh token."""
    client_id = os.environ.get("ZOHO_MEDIDESK_CLIENT_ID", "WPISZ_CLIENT_ID")
    client_secret = os.environ.get("ZOHO_MEDIDESK_CLIENT_SECRET", "WPISZ_CLIENT_SECRET")
    refresh_token = os.environ.get("ZOHO_MEDIDESK_REFRESH_TOKEN", "WPISZ_REFRESH_TOKEN")

    if any(val.startswith("WPISZ_") for val in [client_id, client_secret, refresh_token]):
        raise RuntimeError(
            "Błąd: Dane (client_id, client_secret, refresh_token) nie zostały skonfigurowane.\n"
            "Ustaw zmienne środowiskowe (np. ZOHO_MEDIDESK_CLIENT_ID) lub wpisz je bezpośrednio w pliku."
        )

    token_info = refresh_access_token(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
    )
    ACCESS_TOKEN_HOLDER["token"] = token_info["access_token"]
    ACCESS_TOKEN_HOLDER["refreshed_at"] = time.time()
    return token_info["access_token"]


def _maybe_refresh_token_cooldown() -> Optional[str]:
    """Odświeża token tylko jeśli minęło >= 15s od ostatniego odświeżenia."""
    try:
        last = float(ACCESS_TOKEN_HOLDER.get("refreshed_at") or 0.0)
        if time.time() - last < 15.0:
            return ACCESS_TOKEN_HOLDER.get("token")
        new_tok = get_access_token()
        return new_tok
    except Exception as e:
        logging.error(f"Cooldown refresh failed: {e}")
        return None


def execute_api_request(access_token: str, url: str) -> Optional[Dict[str, Any]]:
    """Wykonuje żądanie API z automatycznym odświeżaniem tokena."""
    def _do_request(tok: str) -> Optional[Dict[str, Any]]:
        headers = {"Authorization": f"Zoho-oauthtoken {tok}"}
        request = urllib.request.Request(url, headers=headers, method="GET")
        time.sleep(REQUEST_SLEEP_S)
        logging.debug(f"HTTP GET: {url}")
        with urllib.request.urlopen(request, timeout=30) as response:
            data = response.read().decode("utf-8")
            if not data:
                return None
            return json.loads(data)

    try:
        tok0 = ACCESS_TOKEN_HOLDER.get("token") or access_token
        return _do_request(tok0)
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        logging.error(f"HTTPError GET {e.code} for {url}: {body}")
        if e.code in (401, 403):
            try:
                new_tok = _maybe_refresh_token_cooldown() or access_token
                return _do_request(new_tok)
            except Exception as e2:
                logging.error(f"Ponowienie po odświeżeniu tokena nie powiodło się: {e2}")
                return None
        return None
    except Exception as e:
        err_text = str(e)
        if "timed out" in err_text or "10060" in err_text:
            for i in range(MAX_RETRY_TIMEOUT):
                time.sleep(2 + i * 3)
                try:
                    tok = ACCESS_TOKEN_HOLDER.get("token") or access_token
                    return _do_request(tok)
                except Exception:
                    continue
        logging.error(f"Błąd podczas wykonywania zapytania do API ({url}): {e}")
        return None


def fetch_all_accounts_list_records(access_token: str) -> List[Dict[str, Any]]:
    """
    Pobiera WSZYSTKIE Accounts przez /Accounts z page_token (to endpoint, który w praktyce zwraca page_token).
    Dzięki temu nie mamy limitu 10k COQL ani limitu 2000 stron po "page".
    """
    all_acc: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    page = 1

    fields = "id,Account_Name,Specjalizacja,Status_klienta,Adres_w_rekordzie,Billing_State,Shipping_State,Billing_City,Shipping_City,Firma_NIP"

    old_level = logging.getLogger().level
    logging.getLogger().setLevel(logging.WARNING)
    pbar = tqdm(total=None, desc="Pobieranie Accounts (list/page_token)", unit=" rek", file=sys.stderr, leave=True, dynamic_ncols=True)

    while True:
        params: Dict[str, Any] = {"fields": fields, "per_page": 200}
        if page_token:
            params["page_token"] = page_token
        else:
            params["page"] = page

        url = f"{ZOHO_CRM_API_BASE_URL}/Accounts?{urllib.parse.urlencode(params)}"
        payload = execute_api_request(access_token, url) or {}
        data = payload.get("data", []) or []
        all_acc.extend(data)
        pbar.update(len(data))

        info = payload.get("info", {}) or {}
        if not info.get("more_records"):
            break

        # Zoho zwraca token pod kluczem next_page_token / previous_page_token
        page_token = info.get("next_page_token") or info.get("page_token")
        if not page_token:
            pbar.write("Brak page_token mimo more_records=True. Przerywam (grozi limitem 2000).")
            break

        page += 1

    pbar.close()
    logging.getLogger().setLevel(old_level)
    return all_acc


def merge_xlsx_files(input_files: List[str], output_file: str) -> None:
    """
    Scala wiele plików XLSX (o tych samych kolumnach) w jeden.
    Robi deduplikację po (NIP, ID_placówki, Wykluczony, Powod_wykluczenia).
    """
    if not input_files:
        raise ValueError("Brak plików do scalenia.")
    frames: List[pd.DataFrame] = []
    for fp in sorted(input_files):
        try:
            frames.append(pd.read_excel(fp, dtype=str))
        except Exception:
            # jeśli plik jest chwilowo zablokowany lub uszkodzony, pomiń
            continue
    if not frames:
        raise ValueError("Nie udało się wczytać żadnego pliku XLSX do scalenia.")
    df = pd.concat(frames, ignore_index=True)
    for col in ("Wykluczony", "Powod_wykluczenia", "NIP", "ID_placówki"):
        if col not in df.columns:
            df[col] = ""
    df = df.drop_duplicates(subset=["NIP", "ID_placówki", "Wykluczony", "Powod_wykluczenia"], keep="last")
    df.to_excel(output_file, index=False, engine="openpyxl")


def execute_coql_query(access_token: str, query: str) -> Optional[List[Dict[str, Any]]]:
    """Wykonuje zapytanie COQL."""
    def _do_post(tok: str) -> Optional[List[Dict[str, Any]]]:
        payload = json.dumps({"select_query": query}).encode("utf-8")
        headers = {
            "Authorization": f"Zoho-oauthtoken {tok}",
            "Content-Type": "application/json",
        }
        req = urllib.request.Request(ZOHO_CRM_COQL_URL, data=payload, headers=headers, method="POST")
        time.sleep(REQUEST_SLEEP_S)
        logging.debug(f"COQL: {query}")
        with urllib.request.urlopen(req, timeout=30) as resp:
            text = resp.read().decode("utf-8")
            if not text:
                return []
            return json.loads(text).get("data", [])

    try:
        tok0 = ACCESS_TOKEN_HOLDER.get("token") or access_token
        return _do_post(tok0)
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        logging.error(f"HTTPError COQL {e.code}: {body} | {query}")
        if e.code in (401, 403):
            try:
                new_tok = _maybe_refresh_token_cooldown() or access_token
                return _do_post(new_tok)
            except Exception as e2:
                logging.error(f"Ponowienie COQL po odświeżeniu tokena nie powiodło się: {e2}")
                return None
        return None
    except Exception as e:
        err_text = str(e)
        if "timed out" in err_text or "10060" in err_text:
            for i in range(MAX_RETRY_TIMEOUT):
                time.sleep(2 + i * 3)
                try:
                    tok = ACCESS_TOKEN_HOLDER.get("token") or access_token
                    return _do_post(tok)
                except Exception:
                    continue
        logging.error(f"Błąd COQL: {e} | {query}")
        return None


def _parse_zoho_dt(val: Any) -> Optional[datetime]:
    """
    Próbuje sparsować datę/czas z Zoho (np. '2026-01-12T15:15:00+01:00' albo '...Z').
    Zwraca datetime (aware jeśli offset był w wejściu) lub None.
    """
    if not val:
        return None
    s = str(val).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _format_dt_for_excel(val: Any) -> str:
    """Format: 'YYYY-MM-DD HH:MM:SS' jeśli da się sparsować; wpp zwraca oryginał jako string."""
    dt = _parse_zoho_dt(val)
    if dt is None:
        return str(val) if val is not None else ""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def search_module_by_relation(
    access_token: str,
    module: str,
    field_api: str,
    related_id: str,
    fields: List[str],
) -> List[Dict[str, Any]]:
    """Wyszukuje rekordy modułu powiązane przez field_api z related_id."""
    records: List[Dict[str, Any]] = []
    page = 1
    while True:
        criteria = f"({field_api}:equals:{related_id})"
        params = {
            "criteria": criteria,
            "fields": ",".join(fields),
            "per_page": 200,
            "page": page,
        }
        url = f"{ZOHO_CRM_API_BASE_URL}/{module}/search?{urllib.parse.urlencode(params)}"
        payload = execute_api_request(access_token, url)
        if not payload:
            break
        data = payload.get("data", [])
        records.extend(data)
        info = payload.get("info", {})
        if not info or not info.get("more_records"):
            break
        page += 1
    return records


def _chunks(lst: List[str], n: int) -> List[List[str]]:
    if n <= 0:
        return [lst]
    return [lst[i : i + n] for i in range(0, len(lst), n)]


def _normalize_nip(val: Any) -> str:
    """
    Normalizacja NIP dla deduplikacji "rodzin" (różne formaty, myślniki/spacje).
    Jeśli da się wyciągnąć 10 cyfr -> zwraca te 10 cyfr, wpp zwraca przycięty string wejściowy.
    """
    if val is None:
        return ""
    s = str(val).strip()
    if not s:
        return ""
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) == 10:
        return digits
    return s


def _normalize_nip_10(val: Any) -> str:
    """Zwraca tylko poprawny NIP (10 cyfr) albo pusty string."""
    digits = "".join(ch for ch in str(val or "") if str(val or "") and ch.isdigit())
    return digits if len(digits) == 10 else ""


def get_accounts_by_nip(access_token: str, nip: str) -> List[Dict[str, Any]]:
    """Zwraca wszystkie Accounts z danym NIP."""
    if not nip or not nip.strip():
        return []

    nip_raw = str(nip).strip()
    nip_norm = _normalize_nip(nip_raw)
    candidates = [nip_raw]
    if nip_norm and nip_norm != nip_raw:
        candidates.append(nip_norm)

    by_id: Dict[str, Dict[str, Any]] = {}
    for n in candidates:
        query = (
            "select id, Account_Name, Firma_NIP, Status_klienta, Adres_w_rekordzie, Billing_State, Shipping_State, "
            f"Specjalizacja, Billing_City, Shipping_City from Accounts where Firma_NIP = '{n}'"
        )
        rows = execute_coql_query(access_token, query) or []
        for r in rows:
            rid = str(r.get("id") or "")
            if rid:
                by_id[rid] = r
    return list(by_id.values())


def get_leads_for_accounts(access_token: str, account_ids: List[str]) -> List[Dict[str, Any]]:
    """Zwraca wszystkie Leads powiązane z Accounts."""
    if not account_ids:
        return []
    all_rows: List[Dict[str, Any]] = []
    for batch in _chunks(account_ids, COQL_IN_MAX_VALUES):
        ids_str = ",".join(batch)
        query = f"select id, Lead_Status, Firma_w_bazie, Modified_Time from Leads where Firma_w_bazie.id in ({ids_str})"
        rows = execute_coql_query(access_token, query) or []
        all_rows.extend(rows)
    return all_rows


def get_deals_for_accounts(access_token: str, account_ids: List[str]) -> List[Dict[str, Any]]:
    """Zwraca wszystkie Deals powiązane z Accounts."""
    if not account_ids:
        return []
    all_rows: List[Dict[str, Any]] = []
    for batch in _chunks(account_ids, COQL_IN_MAX_VALUES):
        ids_str = ",".join(batch)
        query = f"select id, Stage, Account_Name, Modified_Time from Deals where Account_Name.id in ({ids_str})"
        rows = execute_coql_query(access_token, query) or []
        all_rows.extend(rows)
    return all_rows


def get_events_for_leads(access_token: str, lead_ids: List[str]) -> List[Dict[str, Any]]:
    """Zwraca Events powiązane z Leads (Who_Id)."""
    events: List[Dict[str, Any]] = []
    for lid in lead_ids:
        events.extend(
            search_module_by_relation(
                # W praktyce spotkania "na Leadzie" są zwykle podpięte przez What_Id = Lead,
                # a Who_Id bywa puste lub wskazuje inną encję. To powodowało 0 wyników dla Leadów.
                access_token, "Events", "What_Id", lid, ["id", "Event_Title", "Start_DateTime", "What_Id", "Who_Id"]
            )
        )
        # Fallback przez relacje
        page = 1
        while True:
            url = f"{ZOHO_CRM_API_BASE_URL}/Leads/{lid}/Events?fields=id,Event_Title,Start_DateTime,What_Id,Who_Id&per_page=200&page={page}"
            payload = execute_api_request(access_token, url)
            if not payload:
                break
            data = payload.get("data", [])
            events.extend(data)
            info = payload.get("info", {})
            if not info or not info.get("more_records"):
                break
            page += 1
    return events


def get_events_for_deals(access_token: str, deal_ids: List[str]) -> List[Dict[str, Any]]:
    """Zwraca Events powiązane z Deals (What_Id)."""
    events: List[Dict[str, Any]] = []
    for did in deal_ids:
        events.extend(
            search_module_by_relation(
                access_token, "Events", "What_Id", did, ["id", "Event_Title", "Start_DateTime", "What_Id", "Who_Id"]
            )
        )
        # Fallback przez relacje
        page = 1
        while True:
            url = f"{ZOHO_CRM_API_BASE_URL}/Deals/{did}/Events?fields=id,Event_Title,Start_DateTime,What_Id,Who_Id&per_page=200&page={page}"
            payload = execute_api_request(access_token, url)
            if not payload:
                break
            data = payload.get("data", [])
            events.extend(data)
            info = payload.get("info", {})
            if not info or not info.get("more_records"):
                break
            page += 1
    return events


def has_excluded_deal_stage(deals: List[Dict[str, Any]]) -> bool:
    """
    Sprawdza czy któreś Deals są na wykluczonym etapie.
    Zwraca True jeśli: któryś Deal jest na wykluczonym etapie.
    Zwraca False jeśli: brak Dealów LUB wszystkie Deals są na nie-wykluczonych etapach.
    """
    if not deals:
        # Brak Dealów = nie ma wykluczonego etapu (spełnia warunek)
        return False
    for deal in deals:
        stage = deal.get("Stage")
        if isinstance(stage, dict):
            stage = stage.get("name", "")
        if stage in WYKLUCZONE_DEAL_ETAPY:
            return True
    return False


def main() -> None:
    """Główna funkcja skryptu."""
    logging.info("Rozpoczynam wyszukiwanie WSZYSTKICH firm z prezentacją produktu (nie tylko POZ)...")

    parser = argparse.ArgumentParser(description="Wyszukiwanie firm z prezentacją (Events) + filtrowanie dealami.")
    parser.add_argument(
        "--max-nips",
        type=int,
        default=50,
        help="Ile NIP-ów przetworzyć (domyślnie 50). Ustaw 0, żeby przetwarzać wszystkie.",
    )
    # Stronnicowanie po NIP-ach (żeby przerabiać całość partiami i zapisywać osobne pliki)
    parser.add_argument("--nip-page-size", type=int, default=0, help="Rozmiar strony NIP-ów (0 = wszystkie).")
    parser.add_argument("--nip-page", type=int, default=1, help="Numer strony (1-based) dla NIP-ów.")
    # Cache Accounts, żeby nie pobierać 26k rekordów przy każdej stronie NIP-ów
    parser.add_argument(
        "--accounts-cache",
        type=str,
        default="accounts_cache.pkl",
        help="Ścieżka do cache (pickle) z pobranymi Accounts. Przyspiesza stronnicowanie po NIP-ach.",
    )
    parser.add_argument(
        "--refresh-accounts",
        action="store_true",
        help="Wymuś ponowne pobranie Accounts i nadpisz cache.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="wyniki_all_companies_with_presentation",
        help="Folder bazowy na wyniki (strony + scalony plik).",
    )
    parser.add_argument(
        "--merge-now",
        action="store_true",
        help="Scal wszystkie pliki stron z folderu wyników do jednego XLSX na końcu tego uruchomienia.",
    )
    args = parser.parse_args()
    max_nips = int(args.max_nips or 0)
    nip_page_size = int(args.nip_page_size or 0)
    nip_page = max(1, int(args.nip_page or 1))
    
    access_token = get_access_token()
    
    # 1. Pobierz wszystkie Accounts (bez filtra) stronicując przez page_token,
    # a filtrowanie (Status_klienta, NIP itp.) wykonujemy lokalnie.
    cache_path = str(args.accounts_cache or "").strip()
    all_accounts: List[Dict[str, Any]] = []
    if cache_path and (not args.refresh_accounts) and os.path.exists(cache_path):
        try:
            with open(cache_path, "rb") as f:
                all_accounts = pickle.load(f) or []
            logging.info(f"Wczytano Accounts z cache: {cache_path} ({len(all_accounts)} rekordów)")
        except Exception as e:
            logging.warning(f"Nie udało się wczytać cache ({cache_path}): {e}. Pobieram z API...")
            all_accounts = []

    if not all_accounts:
        logging.info("Pobieranie Accounts (wszystkie, list + page_token)...")
        all_accounts = fetch_all_accounts_list_records(access_token)
        logging.info(f"Pobrano {len(all_accounts)} Accounts (wszystkie)")
        if cache_path:
            try:
                with open(cache_path, "wb") as f:
                    pickle.dump(all_accounts, f, protocol=pickle.HIGHEST_PROTOCOL)
                logging.info(f"Zapisano cache Accounts: {cache_path}")
            except Exception as e:
                logging.warning(f"Nie udało się zapisać cache Accounts ({cache_path}): {e}")
    
    # 2. Grupuj Accounts po NIP (tylko poprawne NIP-y = 10 cyfr; brak NIP pomijamy)
    accounts_by_nip: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for acc in all_accounts:
        nip_key = _normalize_nip_10(acc.get("Firma_NIP"))
        if nip_key:
            accounts_by_nip[nip_key].append(acc)
    
    logging.info(f"Znaleziono {len(accounts_by_nip)} unikalnych NIP-ów")
    
    # 3. Dla każdego NIP sprawdź Events i Deals
    results: List[Dict[str, Any]] = []
    included_rows = 0
    processed_nips: Set[str] = set()
    
    # Wyłącz logi podczas przetwarzania NIPów
    logging.getLogger().setLevel(logging.WARNING)
    
    nip_keys_all = sorted(accounts_by_nip.keys())
    total_unique_nips = len(nip_keys_all)

    nip_keys = nip_keys_all
    if max_nips > 0:
        nip_keys = nip_keys[:max_nips]
    if nip_page_size > 0:
        total_pages = int(math.ceil(len(nip_keys) / float(nip_page_size))) if nip_keys else 0
        logging.info(f"Stronnicowanie NIP: page_size={nip_page_size}, page={nip_page}, pages_total={total_pages}, nips_total={len(nip_keys)} (z {total_unique_nips} unikalnych NIP)")
        start = (nip_page - 1) * nip_page_size
        end = start + nip_page_size
        nip_keys = nip_keys[start:end]
    else:
        logging.info(f"Przetwarzam NIP-y: {len(nip_keys)} (z {total_unique_nips} unikalnych NIP)")

    total_nips = len(nip_keys)
    with tqdm(total=total_nips, desc="Przetwarzanie NIPow", unit=" NIP", file=sys.stderr, leave=True) as pbar_nips:
        for nip in nip_keys:
            accounts = accounts_by_nip.get(nip, [])
            if nip in processed_nips:
                pbar_nips.update(1)
                continue
            processed_nips.add(nip)
            
            def _czy_poz_from_acc(acc: Dict[str, Any]) -> bool:
                spec = acc.get("Specjalizacja")
                if isinstance(spec, list):
                    return any("POZ" in str(item) for item in spec)
                if isinstance(spec, str):
                    return "POZ" in spec
                return "POZ" in str(spec) if spec else False

            def _status_for_acc(acc_id: str, leads_by_account: Dict[str, List[Dict[str, Any]]], deals_by_account: Dict[str, List[Dict[str, Any]]]) -> Tuple[str, str]:
                status_lead_deal = ""
                typ_statusu = "brak"
                acc_deals = deals_by_account.get(acc_id, [])
                acc_leads = leads_by_account.get(acc_id, [])
                if acc_deals:
                    latest_deal = max(acc_deals, key=lambda d: d.get("Modified_Time", ""))
                    stage = latest_deal.get("Stage")
                    if isinstance(stage, dict):
                        status_lead_deal = stage.get("name", "")
                    else:
                        status_lead_deal = str(stage) if stage else ""
                    typ_statusu = "Deal"
                elif acc_leads:
                    latest_lead = max(acc_leads, key=lambda l: l.get("Modified_Time", ""))
                    status = latest_lead.get("Lead_Status")
                    if isinstance(status, dict):
                        status_lead_deal = status.get("name", "")
                    else:
                        status_lead_deal = str(status) if status else ""
                    typ_statusu = "Lead"
                return status_lead_deal, typ_statusu

            def _append_rows(
                acc_list: List[Dict[str, Any]],
                wykluczony: bool,
                powod: str,
                leads_by_account: Optional[Dict[str, List[Dict[str, Any]]]] = None,
                deals_by_account: Optional[Dict[str, List[Dict[str, Any]]]] = None,
                events_count_val: Any = "",
                last_event_val: str = "",
            ) -> None:
                nonlocal included_rows
                leads_by_account = leads_by_account or {}
                deals_by_account = deals_by_account or {}
                for acc in acc_list:
                    acc_id = str(acc.get("id", ""))
                    if not acc_id:
                        continue
                    wojewodztwo = acc.get("Billing_State") or acc.get("Shipping_State") or ""
                    if isinstance(wojewodztwo, dict):
                        wojewodztwo = wojewodztwo.get("name", "")
                    wojewodztwo = str(wojewodztwo or "").strip().lower()
                    miasto = acc.get("Billing_City") or acc.get("Shipping_City") or ""
                    if isinstance(miasto, dict):
                        miasto = miasto.get("name", "")
                    adres_w_rekordzie = acc.get("Adres_w_rekordzie") or ""
                    if isinstance(adres_w_rekordzie, dict):
                        adres_w_rekordzie = adres_w_rekordzie.get("name", "")

                    status_lead_deal, typ_statusu = _status_for_acc(acc_id, leads_by_account, deals_by_account)
                    results.append(
                        {
                            "Wykluczony": bool(wykluczony),
                            "Powod_wykluczenia": str(powod or ""),
                            "ID_placówki": acc_id,
                            "Nazwa_firmy": acc.get("Account_Name", ""),
                            "NIP": nip,
                            "Województwo": wojewodztwo,
                            "Miasto": miasto,
                            "Adres_w_rekordzie": adres_w_rekordzie,
                            "Czy_POZ_w_Specjalizacji": bool(_czy_poz_from_acc(acc)),
                            "Specjalizacja": "; ".join(acc.get("Specjalizacja")) if isinstance(acc.get("Specjalizacja"), list) else (acc.get("Specjalizacja") or ""),
                            "Status_klienta": acc.get("Status_klienta", ""),
                            "Status_Lead_Deal": status_lead_deal,
                            "Typ_statusu": typ_statusu,
                            "Events_Count": events_count_val,
                            "Last_Event": last_event_val,
                        }
                    )
                    if not wykluczony:
                        included_rows += 1

            all_nip_accounts = accounts
            if not all_nip_accounts:
                pbar_nips.update(1)
                continue
            
            # Jeśli KTÓRYKOLWIEK rekord w rodzinie NIP jest klientem, to cały NIP odpada
            if any(acc.get("Status_klienta") == "jest" for acc in all_nip_accounts):
                _append_rows(all_nip_accounts, True, "KLIENT_W_RODZINIE_NIP")
                pbar_nips.update(1)
                continue
            
            # Filtruj tylko te z Status_klienta != "jest" (czyli placówki, które realnie chcemy wypisać)
            nip_accounts_filtered = [
                acc for acc in all_nip_accounts
                if acc.get("Status_klienta") != "jest"
            ]
            
            if not nip_accounts_filtered:
                pbar_nips.update(1)
                continue
            
            account_ids = [str(acc.get("id", "")) for acc in all_nip_accounts if acc.get("id")]
            if not account_ids:
                pbar_nips.update(1)
                continue
            
            # Pobierz Leads i Deals dla wszystkich Accounts z tym NIP
            leads = get_leads_for_accounts(access_token, account_ids)
            deals = get_deals_for_accounts(access_token, account_ids)

            # Mapy do ustalania statusu per placówka (również w wierszach wykluczonych)
            leads_by_account: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
            deals_by_account: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
            for lead in leads:
                firm = lead.get("Firma_w_bazie")
                if isinstance(firm, dict):
                    acc_id = str(firm.get("id", ""))
                    if acc_id:
                        leads_by_account[acc_id].append(lead)
            for deal in deals:
                firm = deal.get("Account_Name")
                if isinstance(firm, dict):
                    acc_id = str(firm.get("id", ""))
                    if acc_id:
                        deals_by_account[acc_id].append(deal)
            
            # Sprawdź czy są Events powiązane z Leads lub Deals
            lead_ids = [str(lead.get("id", "")) for lead in leads if lead.get("id")]
            deal_ids = [str(deal.get("id", "")) for deal in deals if deal.get("id")]
            
            events = []
            if lead_ids:
                events.extend(get_events_for_leads(access_token, lead_ids))
            if deal_ids:
                events.extend(get_events_for_deals(access_token, deal_ids))
            events_count = len(events)
            last_event = ""
            try:
                latest_dt: Optional[datetime] = None
                latest_raw: Any = None
                for ev in events:
                    raw = ev.get("Start_DateTime") or ev.get("Call_Start_Time") or ev.get("Due_Date")
                    dt = _parse_zoho_dt(raw)
                    if dt is None:
                        continue
                    if latest_dt is None or dt > latest_dt:
                        latest_dt = dt
                        latest_raw = raw
                if latest_dt is not None:
                    last_event = _format_dt_for_excel(latest_raw)
            except Exception:
                last_event = ""
            
            if not events:
                # Brak Events - wyklucz
                _append_rows(nip_accounts_filtered, True, "BRAK_SPOTKAN", leads_by_account, deals_by_account, 0, "")
                pbar_nips.update(1)
                continue
            
            # Sprawdź czy nie ma wykluczonego Deal Stage
            if has_excluded_deal_stage(deals):
                # Ma wykluczony Deal Stage - wyklucz
                _append_rows(nip_accounts_filtered, True, "AKTYWNY_DEAL_W_RODZINIE_NIP", leads_by_account, deals_by_account, events_count, last_event)
                pbar_nips.update(1)
                continue
            
            # Spełnia wszystkie kryteria
            _append_rows(nip_accounts_filtered, False, "", leads_by_account, deals_by_account, events_count, last_event)
            
            pbar_nips.update(1)
    
    # Przywróć logi
    logging.getLogger().setLevel(logging.INFO)
    
    logging.info(f"Zapisano {len(results)} wierszy, z czego {included_rows} spełnia kryteria (Wykluczony=False)")
    
    # 4. Zapisz wyniki do Excel (XLSX)
    if not results:
        logging.warning("Brak wyników do zapisania")
        return

    # Folder wyników: dla paginacji stabilny per page_size, żeby strony wpadały w jedno miejsce
    base_out_dir = str(args.output_dir or "").strip() or "wyniki_all_companies_with_presentation"
    if nip_page_size > 0:
        run_dir = os.path.join(base_out_dir, f"pageSize{nip_page_size}")
    else:
        run_dir = os.path.join(base_out_dir, datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
    os.makedirs(run_dir, exist_ok=True)

    suffix = ""
    if nip_page_size > 0:
        suffix = f"_nipPage{nip_page}_size{nip_page_size}"
    output_file = os.path.join(run_dir, f"all_companies_with_presentation{suffix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
    
    # Utwórz DataFrame z wynikami
    df = pd.DataFrame(results)
    
    # Upewnij się że kolumny są w odpowiedniej kolejności
    columns_order = [
        "Wykluczony",
        "Powod_wykluczenia",
        "ID_placówki",
        "Nazwa_firmy",
        "NIP",
        "Województwo",
        "Miasto",
        "Adres_w_rekordzie",
        "Czy_POZ_w_Specjalizacji",
        "Specjalizacja",
        "Status_klienta",
        "Status_Lead_Deal",
        "Typ_statusu",
        "Events_Count",
        "Last_Event",
    ]
    df = df.reindex(columns=columns_order)
    
    # Zapisz do Excel
    df.to_excel(output_file, index=False, engine="openpyxl")
    
    logging.info(f"Wyniki zapisane do pliku: {output_file}")

    # Jeśli to ostatnia strona albo merge-now: scal wszystko do jednego pliku
    try:
        do_merge = bool(args.merge_now)
        if nip_page_size > 0 and "total_pages" in locals() and total_pages and nip_page >= total_pages:
            do_merge = True
        if do_merge and nip_page_size > 0:
            pattern = os.path.join(run_dir, f"all_companies_with_presentation_nipPage*_size{nip_page_size}_*.xlsx")
            files = glob.glob(pattern)
            merged_path = os.path.join(run_dir, f"all_companies_with_presentation_merged_size{nip_page_size}.xlsx")
            merge_xlsx_files(files, merged_path)
            logging.info(f"Scalono {len(files)} plików do: {merged_path}")
    except Exception as e:
        logging.warning(f"Scalanie plików nie powiodło się: {e}")
    
    # 5. Podsumowanie według Adres_w_rekordzie
    by_adres = defaultdict(int)
    for result in results:
        adres = result.get("Adres_w_rekordzie", "brak")
        by_adres[adres] += 1
    
    logging.info("Podsumowanie według Adres_w_rekordzie:")
    for adres, count in sorted(by_adres.items()):
        logging.info(f"  {adres}: {count}")
    
    # 6. Podsumowanie według Specjalizacji
    by_spec = defaultdict(int)
    for result in results:
        spec = result.get("Specjalizacja", "brak")
        by_spec[spec] += 1
    
    logging.info("Podsumowanie według Specjalizacji:")
    for spec, count in sorted(by_spec.items(), key=lambda x: -x[1])[:20]:  # Top 20
        logging.info(f"  {spec}: {count}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Przerwano przez użytkownika")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Błąd: {e}", exc_info=True)
        sys.exit(1)
