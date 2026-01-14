"""
Skrypt do wyszukiwania firm POZ które:
Skrypt do wyszukiwania firm (bez ograniczania do POZ) które:
1. Status_klienta != "jest"
2. Miały kiedykolwiek spotkanie (Events powiązane z Lead lub Deal)
3. Nie mają otwartego Deala (lub Deal jest na wykluczonych etapach)
4. Wyniki podzielone według Adres_w_rekordzie z ID placówek i województwami
5. Dodatkowa kolumna: Czy Specjalizacja zawiera POZ (True/False)
2. Status_klienta != "jest"
3. Miały kiedykolwiek spotkanie (Events powiązane z Lead lub Deal)
4. Nie mają otwartego Deala (lub Deal jest na wykluczonych etapach)
5. Wyniki podzielone według Adres_w_rekordzie z ID placówek i województwami
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
    from tqdm.auto import tqdm
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
        logging.FileHandler("search_poz_companies.log", encoding="utf-8"),
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
    # python datetime.fromisoformat nie akceptuje 'Z' -> zamień na +00:00
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
            f"Specjalizacja from Accounts where Firma_NIP = '{n}'"
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
    logging.info("Rozpoczynam wyszukiwanie firm z prezentacją produktu (bez ograniczania do POZ)...")

    parser = argparse.ArgumentParser(description="Wyszukiwanie firm z prezentacją (Events) + filtrowanie dealami.")
    parser.add_argument(
        "--max-nips",
        type=int,
        default=50,
        help="Ile NIP-ów przetworzyć (domyślnie 50). Ustaw 0, żeby przetwarzać wszystkie.",
    )
    args = parser.parse_args()
    max_nips = int(args.max_nips or 0)

    access_token = get_access_token()
    
    # 1. Wyszukaj Accounts z Status_klienta != "jest" (bez ograniczania Specjalizacji)
    # Próba 1: COQL (może być ograniczone do 10k rekordów)
    # Próba 2: search API z criteria not_equal + page_token
    logging.info("Wyszukiwanie Accounts z filtrem (Status_klienta != 'jest')...")

    def fetch_accounts_coql_filtered() -> List[Dict[str, Any]]:
        all_acc: List[Dict[str, Any]] = []
        limit_local = 200  # max dla COQL
        offset_local = 0
        # Wyłącz logi INFO podczas pobierania, żeby nie zakłócać paska
        old_level = logging.getLogger().level
        logging.getLogger().setLevel(logging.WARNING)
        
        pbar = tqdm(total=None, desc="Pobieranie Accounts", unit=" rek", file=sys.stderr, leave=True, dynamic_ncols=True)
        while True:
            query = (
                "select id, Account_Name, Specjalizacja, Status_klienta, Adres_w_rekordzie, "
                f"Billing_State, Shipping_State, Billing_City, Shipping_City, Firma_NIP from Accounts "
                f"where Status_klienta != 'jest' "
                f"limit {limit_local} offset {offset_local}"
            )
            rows = execute_coql_query(access_token, query) or []
            if not rows:
                break
            all_acc.extend(rows)
            pbar.update(len(rows))
            if len(rows) < limit_local:
                break
            offset_local += limit_local
            if offset_local >= 10000:
                pbar.write("COQL limit 10k osiagnięty")
                break
        pbar.close()
        # Przywróć logi
        logging.getLogger().setLevel(old_level)
        return all_acc

    def fetch_accounts_search_filtered() -> List[Dict[str, Any]]:
        all_acc: List[Dict[str, Any]] = []
        page_token: Optional[str] = None
        page = 1
        # Wyłącz logi INFO podczas pobierania
        old_level = logging.getLogger().level
        logging.getLogger().setLevel(logging.WARNING)
        
        pbar = tqdm(total=None, desc="Search API Accounts", unit=" rek", file=sys.stderr, leave=True, dynamic_ncols=True)
        while True:
            params = {
                "criteria": "(Status_klienta:not_equal:jest)",
                "fields": "id,Account_Name,Specjalizacja,Status_klienta,Adres_w_rekordzie,Billing_State,Shipping_State,Billing_City,Shipping_City,Firma_NIP",
                "per_page": 200,
            }
            if page_token:
                params["page_token"] = page_token
            else:
                params["page"] = page

            url = f"{ZOHO_CRM_API_BASE_URL}/Accounts/search?{urllib.parse.urlencode(params)}"
            payload = execute_api_request(access_token, url)
            if not payload:
                break
            data = payload.get("data", [])
            all_acc.extend(data)
            pbar.update(len(data))
            info = payload.get("info", {})
            if not info or not info.get("more_records"):
                break
            page_token = info.get("page_token")
            if not page_token:
                page += 1
                if page > 20:
                    pbar.write("Brak page_token, przerywam po 20 stronach")
                    break
        pbar.close()
        # Przywróć logi
        logging.getLogger().setLevel(old_level)
        return all_acc

    all_accounts: List[Dict[str, Any]] = []
    try:
        all_accounts = fetch_accounts_coql_filtered()
    except Exception as e:
        logging.error(f"Błąd COQL filtrowanego pobierania: {e}")
        all_accounts = []

    if not all_accounts:
        logging.info("Fallback na search API (COQL zwrócił 0 rekordów)")
        all_accounts = fetch_accounts_search_filtered()
    elif len(all_accounts) >= 10000:
        logging.warning("COQL zwrócił 10k rekordów (limit). Kontynuuję na tych 10k bez fallbacku na Search API.")

    logging.info(f"Pobrano {len(all_accounts)} Accounts")
    
    # Filtruj po Status_klienta != "jest"
    filtered_accounts = []
    for acc in all_accounts:
        status = acc.get("Status_klienta")
        if status == "jest":
            continue

        filtered_accounts.append(acc)
    
    logging.info(f"Po filtracji (Status_klienta != 'jest'): {len(filtered_accounts)} Accounts")
    
    # 2. Grupuj Accounts po NIP
    accounts_by_nip: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for acc in filtered_accounts:
        nip_key = _normalize_nip(acc.get("Firma_NIP"))
        if nip_key:
            accounts_by_nip[nip_key].append(acc)
    
    logging.info(f"Znaleziono {len(accounts_by_nip)} unikalnych NIP-ów")
    
    # 3. Dla każdego NIP sprawdź Events i Deals
    results: List[Dict[str, Any]] = []
    included_rows = 0
    processed_nips: Set[str] = set()
    
    # Wyłącz logi INFO podczas przetwarzania NIPów
    logging.getLogger().setLevel(logging.WARNING)
    
    total_nips = len(accounts_by_nip) if max_nips <= 0 else min(len(accounts_by_nip), max_nips)
    with tqdm(total=total_nips, desc="Przetwarzanie NIPow", unit=" NIP", file=sys.stderr, leave=True) as pbar_nips:
        for nip, accounts in accounts_by_nip.items():
            if max_nips > 0 and len(processed_nips) >= max_nips:
                break
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

            # Pobierz wszystkie Accounts z tym NIP (użyj "surowej" wartości z rekordów, żeby złapać formaty typu 123-...)
            nip_probe = accounts[0].get("Firma_NIP") if accounts else nip
            all_nip_accounts = get_accounts_by_nip(access_token, nip_probe)
            if not all_nip_accounts:
                pbar_nips.update(1)
                continue

            # Jeśli KTÓRYKOLWIEK rekord w rodzinie NIP jest klientem, to cały NIP odpada
            if any(acc.get("Status_klienta") == "jest" for acc in all_nip_accounts):
                _append_rows(all_nip_accounts, True, "KLIENT_W_RODZINIE_NIP")
                pbar_nips.update(1)
                continue
            
            # Filtruj tylko te z POZ i Status_klienta != "jest"
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
    
    output_file = f"companies_with_presentation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
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
    
    # 5. Podsumowanie według Adres_w_rekordzie
    by_adres = defaultdict(int)
    for result in results:
        adres = result.get("Adres_w_rekordzie", "brak")
        by_adres[adres] += 1
    
    logging.info("Podsumowanie według Adres_w_rekordzie:")
    for adres, count in sorted(by_adres.items()):
        logging.info(f"  {adres}: {count}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Przerwano przez użytkownika")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Błąd: {e}", exc_info=True)
        sys.exit(1)
