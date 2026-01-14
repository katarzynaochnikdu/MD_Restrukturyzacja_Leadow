import argparse
import json
import logging
import os
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

try:
    import pandas as pd
except ImportError:
    print("Błąd: Biblioteka 'pandas' nie jest zainstalowana.", file=sys.stderr)
    sys.exit(1)

from refresh_zoho_access_token import refresh_access_token

ZOHO_CRM_API_BASE_URL = "https://www.zohoapis.eu/crm/v3"
ZOHO_CRM_COQL_URL = f"{ZOHO_CRM_API_BASE_URL}/coql"
REQUEST_SLEEP_S = 0.2  # delikatny break między wywołaniami API

# --- Definicje statusów ---

AKTYWNE_LEAD_STATUSY = [
    "Aktywny zimny", "Dzwonienie", "Nurturing",
    "Umówione spotkanie", "Zakwalifikowane do sales"
]

AKTYWNE_DEAL_ETAPY = [
    "Qualification", "Spotkania", "Oferta", "Negocjacje",
    "Oferta zaakceptowana", "Umowa w podpisie"
]

WYGRANE_DEAL_ETAPY = [
    "Closed Won", "Wdrożenie", "Trial", "Wdrożeni Klienci"
]

# --- Funkcje pomocnicze ---

def setup_logging(run_dir: str) -> None:
    log_file = os.path.join(run_dir, "log.txt")
    root = logging.getLogger()
    if root.handlers:
        for h in list(root.handlers):
            root.removeHandler(h)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

def fmt_date_pl(value: Optional[str]) -> str:
    """Formatuje datę/czas z ISO/Zoho na DD-MM-RRRR. Gdy brak lub nie parsuje się – zwraca pusty ciąg."""
    if not value:
        return ""
    s = value.strip()
    try:
        # ISO z "Z" na końcu
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        # Próbujemy ISO z offsetem
        dt = datetime.fromisoformat(s)
        return dt.strftime("%d-%m-%Y")
    except Exception:
        pass
    # Drugie podejście: znane patterny
    for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(value[:19], pattern)
            return dt.strftime("%d-%m-%Y")
        except Exception:
            continue
    # Ostatnia deska ratunku: YYYY-MM-DD -> DD-MM-YYYY
    if len(value) >= 10 and value[4] == '-' and value[7] == '-':
        y, m, d = value[:10].split('-')
        return f"{d}-{m}-{y}"
    return ""

def get_access_token() -> str:
    logging.info("Pobieranie tokena dostępowego Zoho...")
    client_id = os.environ.get("ZOHO_MEDIDESK_CLIENT_ID")
    client_secret = os.environ.get("ZOHO_MEDIDESK_CLIENT_SECRET")
    refresh_token = os.environ.get("ZOHO_MEDIDESK_REFRESH_TOKEN")

    if not all([client_id, client_secret, refresh_token]):
        logging.error("Błąd: Zmienne środowiskowe nie są w pełni skonfigurowane.")
        sys.exit(1)

    try:
        token_info = refresh_access_token(client_id, client_secret, refresh_token)
        logging.info("Pomyślnie pobrano token.")
        return token_info["access_token"]
    except RuntimeError as e:
        logging.error(f"Wystąpił błąd podczas pobierania tokena: {e}")
        sys.exit(1)

def execute_api_request(access_token: str, url: str) -> Optional[Dict[str, Any]]:
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    request = urllib.request.Request(url, headers=headers, method="GET")

    try:
        time.sleep(REQUEST_SLEEP_S)
        logging.info(f"HTTP GET request: {url}")
        with urllib.request.urlopen(request, timeout=30) as response:
            data = response.read().decode("utf-8")
            if not data:
                logging.info("HTTP GET response: <empty>")
                return None
            logging.info(f"HTTP GET response JSON: {data}")
            return json.loads(data)
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = "<brak treści odpowiedzi>"
        logging.error(f"Błąd HTTP podczas zapytania do API ({url}): {e} | body: {body}")
        return None
    except Exception as e:
        logging.error(f"Błąd podczas wykonywania zapytania do API ({url}): {e}")
        return None

def execute_coql_query(access_token: str, query: str) -> Optional[List[Dict[str, Any]]]:
    data = {"select_query": query}
    encoded_data = json.dumps(data).encode("utf-8")
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/json",
    }
    request = urllib.request.Request(
        ZOHO_CRM_COQL_URL, data=encoded_data, headers=headers, method="POST"
    )

    try:
        logging.info(f"COQL zapytanie: {query}")
        logging.info(f"COQL request JSON: {json.dumps(data, ensure_ascii=False)}")
        time.sleep(REQUEST_SLEEP_S)
        with urllib.request.urlopen(request, timeout=30) as response:
            payload_text = response.read().decode("utf-8")
            if not payload_text:
                logging.info("COQL odpowiedź: pusty payload")
                return []
            logging.info(f"COQL response JSON: {payload_text}")
            payload = json.loads(payload_text)
            rows = payload.get("data", [])
            logging.info(f"COQL OK: {len(rows)} rekordów")
            return rows
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = "<brak treści odpowiedzi>"
        logging.error(f"Błąd COQL HTTP  ({e}) | body: {body} | query: {query}")
        return None
    except Exception as e:
        logging.error(f"Błąd podczas wykonywania zapytania COQL: {e} | query: {query}")
        return None

def coql_escape(value: str) -> str:
    """Ucieczka znaków w literale COQL (pojedyncze cudzysłowy/backslash)."""
    if value is None:
        return ""
    # najpierw backslash, potem apostrof
    return str(value).replace("\\", "\\\\").replace("'", "\\'")

def coql_in_list(values: List[str]) -> str:
    escaped = [f"'{coql_escape(v)}'" for v in values if v]
    return ", ".join(escaped) if escaped else "''"

def coql_in_id_list(ids: List[str]) -> str:
    only_ids = [str(v) for v in ids if v]
    return ", ".join(only_ids) if only_ids else "0"

def _chunk_list(items: List[str], size: int = 50) -> List[List[str]]:
    return [items[i:i+size] for i in range(0, len(items), size)]

# --- Główna logika pobierania danych ---

def get_account_details_by_nip(access_token: str, nip: str) -> Optional[Dict[str, Any]]:
    logging.info(f"Wyszukiwanie konta dla NIP: {nip}...")
    # Pobieramy wszystkie konta z tym NIP, aby wybrać korzeń (bez rodzica) jeśli istnieje
    query = (
        "select id, Status_klienta, Parent_Account, Firma_NIP, Nazwa_zwyczajowa, Account_Name from Accounts "
        f"where Firma_NIP = '{nip}'"
    )
    results = execute_coql_query(access_token, query) or []
    if not results:
        logging.warning("Nie znaleziono konta o podanym numerze NIP.")
        return None
    # Preferuj rekord bez rodzica
    def has_parent(rec: Dict[str, Any]) -> bool:
        p = rec.get("Parent_Account")
        if isinstance(p, dict):
            return bool(p.get("id"))
        if isinstance(p, str):
            return bool(p)
        return False
    root_candidates = [r for r in results if not has_parent(r)]
    chosen = root_candidates[0] if root_candidates else results[0]
    logging.info(f"Znaleziono konto startowe. ID: {chosen['id']} (spośród {len(results)} z tym NIP)")
    return chosen


def find_related_account_ids(
    access_token: str,
    main_account: Dict[str, Any],
) -> List[str]:
    """Zwraca listę ID wszystkich powiązanych placówek dla całego drzewa:
    - wyznacza korzeń (konto bez rodzica) podążając po Parent_Account,
    - zbiera rekurencyjnie wszystkie potomne (dowolna głębokość),
    - dodaje także konta o tym samym NIP co konto główne (jeśli dostępny).
    """
    def get_parent_id(acc_id: str) -> Optional[str]:
        rows = execute_coql_query(access_token, f"select Parent_Account from Accounts where id = {acc_id}") or []
        if not rows:
            return None
        parent_field = rows[0].get("Parent_Account")
        if isinstance(parent_field, dict) and parent_field.get("id"):
            return str(parent_field.get("id"))
        if isinstance(parent_field, str) and parent_field:
            return parent_field
        return None

    main_id: str = str(main_account.get("id"))
    if not main_id:
        return []

    # znajdź korzeń (bez rodzica)
    root_id = main_id
    while True:
        pid = get_parent_id(root_id)
        if not pid:
            break
        root_id = pid

    # BFS po całym drzewie potomków od korzenia
    seen: set = set()
    result: List[str] = []
    queue: List[str] = [root_id]
    while queue:
        current = queue.pop(0)
        if current in seen:
            continue
        seen.add(current)
        result.append(current)
        children = execute_coql_query(access_token, f"select id from Accounts where Parent_Account.id = {current}") or []
        for ch in children:
            cid = str(ch.get("id")) if ch.get("id") else None
            if cid and cid not in seen:
                queue.append(cid)

    # Konta z tym samym NIP co konto główne
    nip_val = main_account.get("Firma_NIP")
    if nip_val:
        same_nip = execute_coql_query(access_token, f"select id from Accounts where Firma_NIP = '{nip_val}'") or []
        for acc in same_nip:
            rid = str(acc.get("id")) if acc.get("id") else None
            if rid and rid not in seen:
                result.append(rid)
                seen.add(rid)

    logging.info(f"Powiązane konta (drzewo od korzenia {root_id}): {len(result)} -> {', '.join(result)}")
    return result


def get_account_name(access_token: str, account_id: str) -> str:
    rec = execute_coql_query(
        access_token,
        f"select Account_Name from Accounts where id = {account_id}",
    ) or []
    if rec and isinstance(rec[0], dict):
        return str(rec[0].get("Account_Name") or "")
    return ""

def get_display_name(access_token: str, main_account: Dict[str, Any]) -> str:
    name_custom = str(main_account.get("Nazwa_zwyczajowa") or "").strip()
    if name_custom:
        return name_custom
    parent_field = main_account.get("Parent_Account")
    parent_id: Optional[str] = None
    if isinstance(parent_field, dict):
        parent_id = parent_field.get("id")
    elif isinstance(parent_field, str) and parent_field:
        parent_id = parent_field
    if parent_id:
        return get_account_name(access_token, parent_id) or ""
    # fallback: własna nazwa konta
    acc_id = str(main_account.get("id") or "")
    return get_account_name(access_token, acc_id) or ""


def get_account_status(access_token: str, account_id: str) -> str:
    rec = execute_coql_query(
        access_token,
        f"select Status_klienta from Accounts where id = {account_id}",
    ) or []
    if rec and isinstance(rec[0], dict):
        return str(rec[0].get("Status_klienta") or "Brak danych")
    return "Brak danych"

def get_related_records(access_token: str, module: str, record_id: str, fields: List[str]) -> List[Dict[str, Any]]:
    fields_str = ",".join(fields)
    url = f"{ZOHO_CRM_API_BASE_URL}/Accounts/{record_id}/{module}?fields={fields_str}&per_page=200"
    response = execute_api_request(access_token, url)
    return response.get("data", []) if response else []

# ---- Nowe: wyszukiwanie po Who_Id / What_Id (search) z paginacją ----

def search_module_by_relation(
    access_token: str,
    module: str,
    field_api: str,
    related_id: str,
    fields: List[str],
) -> List[Dict[str, Any]]:
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
    logging.info(f"RELATION SEARCH: module={module} field={field_api} id={related_id} -> znaleziono {len(records)} rekordów")
    return records

# ---- Aktywności na podstawie powiązań (Account, Contacts, Deals) ----

def get_all_activities(access_token: str, account_ids: List[str], contact_ids: List[str], lead_ids: List[str], deal_ids: List[str], other_refs: Optional[List[Tuple[str, str]]] = None) -> List[Dict[str, Any]]:
    all_activities: List[Dict[str, Any]] = []
    other_refs = other_refs or []

    # Z kont (What_Id = każdy z account_ids)
    calls_from_account: List[Dict[str, Any]] = []
    events_from_account: List[Dict[str, Any]] = []
    tasks_from_account: List[Dict[str, Any]] = []
    for acc_id in account_ids:
        calls_from_account += search_module_by_relation(
            access_token, "Calls", "What_Id", acc_id, ["Subject", "Call_Start_Time", "Description", "Who_Id", "What_Id"]
        )
        events_from_account += search_module_by_relation(
            access_token, "Events", "What_Id", acc_id, ["Event_Title", "Start_DateTime", "Description", "Who_Id", "What_Id"]
        )
        tasks_from_account += search_module_by_relation(
            access_token, "Tasks", "What_Id", acc_id, ["Subject", "Due_Date", "Description", "Who_Id", "What_Id"]
        )

    # Z kontaktów i leadów (Who_Id = Contact/Lead)
    calls_from_contacts: List[Dict[str, Any]] = []
    events_from_contacts: List[Dict[str, Any]] = []
    tasks_from_contacts: List[Dict[str, Any]] = []
    for cid in contact_ids:
        calls_from_contacts += search_module_by_relation(
            access_token, "Calls", "Who_Id", cid, ["Subject", "Call_Start_Time", "Description", "Who_Id", "What_Id"]
        )
        events_from_contacts += search_module_by_relation(
            access_token, "Events", "Who_Id", cid, ["Event_Title", "Start_DateTime", "Description", "Who_Id", "What_Id"]
        )
        tasks_from_contacts += search_module_by_relation(
            access_token, "Tasks", "Who_Id", cid, ["Subject", "Due_Date", "Description", "Who_Id", "What_Id"]
        )

    # Dodatkowo: bezpośrednio z Leadów – tylko Who_Id (search) oraz fallback: relacje /Leads/{id}/Calls|Events|Tasks
    calls_from_leads: List[Dict[str, Any]] = []
    events_from_leads: List[Dict[str, Any]] = []
    tasks_from_leads: List[Dict[str, Any]] = []
    for lid in lead_ids:
        # próba przez search (Who_Id)
        calls_from_leads += search_module_by_relation(
            access_token, "Calls", "Who_Id", lid, ["Subject", "Call_Start_Time", "Description", "Who_Id", "What_Id"]
        )
        events_from_leads += search_module_by_relation(
            access_token, "Events", "Who_Id", lid, ["Event_Title", "Start_DateTime", "Description", "Who_Id", "What_Id"]
        )
        tasks_from_leads += search_module_by_relation(
            access_token, "Tasks", "Who_Id", lid, ["Subject", "Due_Date", "Description", "Who_Id", "What_Id"]
        )
        # Who_Id jest poprawnym powiązaniem dla Lead; What_Id dla Lead pomijamy
        # fallback: relacje dla Lead
        for rel_mod, date_field in [("Calls", "Call_Start_Time"), ("Events", "Start_DateTime"), ("Tasks", "Due_Date")]:
            rel_fields = (["Event_Title", date_field, "Description", "Who_Id", "What_Id"] if rel_mod == "Events" else ["Subject", date_field, "Description", "Who_Id", "What_Id"])
            fields_str = ",".join(rel_fields)
            page = 1
            while True:
                rel_url = f"{ZOHO_CRM_API_BASE_URL}/Leads/{lid}/{rel_mod}?fields={fields_str}&per_page=200&page={page}"
                payload = execute_api_request(access_token, rel_url)
                if not payload:
                    break
                data = payload.get("data", [])
                if rel_mod == "Calls":
                    calls_from_leads += data
                elif rel_mod == "Events":
                    events_from_leads += data
                else:
                    tasks_from_leads += data
                info = payload.get("info", {})
                if not info or not info.get("more_records"):
                    break
                page += 1

    # Z deali (What_Id = Deal)
    calls_from_deals: List[Dict[str, Any]] = []
    events_from_deals: List[Dict[str, Any]] = []
    tasks_from_deals: List[Dict[str, Any]] = []
    for did in deal_ids:
        calls_from_deals += search_module_by_relation(
            access_token, "Calls", "What_Id", did, ["Subject", "Call_Start_Time", "Description", "Who_Id", "What_Id"]
        )
        events_from_deals += search_module_by_relation(
            access_token, "Events", "What_Id", did, ["Event_Title", "Start_DateTime", "Description", "Who_Id", "What_Id"]
        )
        tasks_from_deals += search_module_by_relation(
            access_token, "Tasks", "What_Id", did, ["Subject", "Due_Date", "Description", "Who_Id", "What_Id"]
        )

    # Fallback bezpośrednio z Deals/{id}/Calls|Events|Tasks (z paginacją)
    for did in deal_ids:
        for rel_mod, date_field in [("Calls", "Call_Start_Time"), ("Events", "Start_DateTime"), ("Tasks", "Due_Date")]:
            rel_fields = (["Event_Title", date_field, "Description", "Who_Id", "What_Id"] if rel_mod == "Events" else ["Subject", date_field, "Description", "Who_Id", "What_Id"])
            fields_str = ",".join(rel_fields)
            page = 1
            while True:
                rel_url = f"{ZOHO_CRM_API_BASE_URL}/Deals/{did}/{rel_mod}?fields={fields_str}&per_page=200&page={page}"
                payload = execute_api_request(access_token, rel_url)
                if not payload:
                    break
                data = payload.get("data", [])
                if rel_mod == "Calls":
                    calls_from_deals += data
                elif rel_mod == "Events":
                    events_from_deals += data
                else:
                    tasks_from_deals += data
                info = payload.get("info", {})
                if not info or not info.get("more_records"):
                    break
                page += 1

    # Z modułów custom (What_Id = inne rekordy powiązane) + fallback po relacjach modułów custom
    calls_from_others: List[Dict[str, Any]] = []
    events_from_others: List[Dict[str, Any]] = []
    tasks_from_others: List[Dict[str, Any]] = []
    for mod_api, oid in other_refs:
        calls_from_others += search_module_by_relation(
            access_token, "Calls", "What_Id", oid, ["Subject", "Call_Start_Time", "Description", "Who_Id", "What_Id"]
        )
        events_from_others += search_module_by_relation(
            access_token, "Events", "What_Id", oid, ["Event_Title", "Start_DateTime", "Description", "Who_Id", "What_Id"]
        )
        tasks_from_others += search_module_by_relation(
            access_token, "Tasks", "What_Id", oid, ["Subject", "Due_Date", "Description", "Who_Id", "What_Id"]
        )

    # Fallback: /{CustomModule}/{id}/Calls|Events|Tasks jeśli dostępne
    for mod_api, oid in other_refs:
        for rel_mod, date_field in [("Calls", "Call_Start_Time"), ("Events", "Start_DateTime"), ("Tasks", "Due_Date")]:
            rel_fields = (["Event_Title", date_field, "Description", "Who_Id", "What_Id"] if rel_mod == "Events" else ["Subject", date_field, "Description", "Who_Id", "What_Id"])
            fields_str = ",".join(rel_fields)
            page = 1
            while True:
                rel_url = f"{ZOHO_CRM_API_BASE_URL}/{mod_api}/{oid}/{rel_mod}?fields={fields_str}&per_page=200&page={page}"
                payload = execute_api_request(access_token, rel_url)
                if not payload:
                    break
                data = payload.get("data", [])
                if rel_mod == "Calls":
                    calls_from_others += data
                elif rel_mod == "Events":
                    events_from_others += data
                else:
                    tasks_from_others += data
                info = payload.get("info", {})
                if not info or not info.get("more_records"):
                    break
                page += 1

    def normalize(module: str, rec: Dict[str, Any]) -> Dict[str, Any]:
        if module == "Calls":
            when = rec.get("Call_Start_Time")
        elif module == "Events":
            when = rec.get("Start_DateTime")
        else:  # Tasks
            when = rec.get("Due_Date")
        related_name = None
        related_symbol = "🧩"
        who = rec.get("Who_Id")
        what = rec.get("What_Id")
        # Pola Who_Id/What_Id mogą być obiektami lub stringami zależnie od API
        if isinstance(who, dict):
            related_name = who.get("name")
            mod = (who.get("module") or "").lower()
            if mod == "contacts":
                related_symbol = "👤"
            elif mod == "leads":
                related_symbol = "🧲"
        if not related_name and isinstance(what, dict):
            related_name = what.get("name")
            mod = (what.get("module") or "").lower()
            if mod == "accounts":
                related_symbol = "🏢"
            elif mod == "deals":
                related_symbol = "🤝"
        # Tytuł: dla Events używamy Event_Title, dla pozostałych Subject
        subj_val = rec.get("Event_Title") if module == "Events" else rec.get("Subject", "")
        # Opis: obetnij do 180 znaków, jeśli >300
        desc_val = rec.get("Description", "")
        if isinstance(desc_val, str) and len(desc_val) > 300:
            desc_val = desc_val[:180]
        return {
            "type": module[:-1] if module.endswith("s") else module,
            "time": when or "",
            "related_to": related_name or "",
            "related_symbol": related_symbol,
            "Subject": subj_val or "",
            "Description": desc_val or "",
        }

    for r in calls_from_account + calls_from_contacts + calls_from_leads + calls_from_deals + calls_from_others:
        all_activities.append(normalize("Calls", r))
    for r in events_from_account + events_from_contacts + events_from_leads + events_from_deals + events_from_others:
        all_activities.append(normalize("Events", r))
    for r in tasks_from_account + tasks_from_contacts + tasks_from_leads + tasks_from_deals + tasks_from_others:
        all_activities.append(normalize("Tasks", r))

    # Sort chronologiczny malejąco
    all_activities.sort(key=lambda x: x.get("time", ""), reverse=True)
    logging.info(
        f"AKTYWNOŚCI zebrane: acc={len(calls_from_account)+len(events_from_account)+len(tasks_from_account)}, "
        f"contacts={len(calls_from_contacts)+len(events_from_contacts)+len(tasks_from_contacts)}, "
        f"leads={len(calls_from_leads)+len(events_from_leads)+len(tasks_from_leads)}, "
        f"deals={len(calls_from_deals)+len(events_from_deals)+len(tasks_from_deals)}, "
        f"others={len(calls_from_others)+len(events_from_others)+len(tasks_from_others)}, "
        f"total={len(all_activities)}"
    )
    return all_activities

# ---- Formatowanie końcowe ----

def format_final_output(details: Dict[str, Any]) -> str:
    output: List[str] = []
    # Nagłówek: ikona kontraktu + status klienta
    kontrakt = "📄"
    disp = details.get('display_name', '')
    if disp:
        output.append(f"Nazwa: {disp}")
    output.append(f"{kontrakt} {details.get('status_klienta', 'Brak')}")
    output.append("-" * 20)
    # Nagłówek – przegląd ikonami
    contacts_cnt = int(details.get('contacts_count', 0) or 0)
    accounts_cnt = int(details.get('related_accounts_count', 0) or 0)
    leads_cnt = int(details.get('leads_count', 0) or 0)
    deals_cnt = int(details.get('deals_count', 0) or 0)
    # Usuwamy linię 'PRZEGLĄD: …' – zamiast tego pokazujemy tylko trzy wiersze poniżej

    # Stare sekcje można pominąć w trybie kompaktowym – ale zostawiamy je niżej, tu prezent kompaktowy

    # Kompakt: ilości i statusy z datami
    last = details.get('last_dates', {}) or {}
    counts: Dict[str, int] = details.get('status_counts', {}) or {}
    # linia 1 już była: status klienta
    # linia 2: kontakty i firmy powiązane
    line2 = []
    if contacts_cnt:
        line2.append(f"👥 {contacts_cnt}")
    if accounts_cnt:
        line2.append(f"🏢 {accounts_cnt}")
    if line2:
        output.append(" ".join(line2))
    # linia 3: leady – [❌ N data | ⚡ M]
    if leads_cnt:
        line = f"🧲 {leads_cnt}"
        bracket_parts: List[str] = []
        if counts.get('leads_disqualified'):
            dt = last.get('lead_disqualified', '')
            bracket_parts.append(f"❌ {counts['leads_disqualified']}{(' ' + dt) if dt else ''}")
        if counts.get('leads_active'):
            bracket_parts.append(f"⚡ {counts['leads_active']}")
        if bracket_parts:
            line += " [" + " | ".join(bracket_parts) + "]"
        output.append(line)
    # linia 4: deale – [❌ N data | ⚡ M | 🏆 K data]
    if deals_cnt:
        line = f"🎯 {deals_cnt}"
        bracket_parts: List[str] = []
        if counts.get('deals_lost'):
            dt = last.get('deal_lost', '')
            bracket_parts.append(f"❌ {counts['deals_lost']}{(' ' + dt) if dt else ''}")
        if counts.get('deals_active'):
            bracket_parts.append(f"⚡ {counts['deals_active']}")
        if counts.get('deals_won'):
            dt = last.get('deal_won', '')
            bracket_parts.append(f"🏆 {counts['deals_won']}{(' ' + dt) if dt else ''}")
        if bracket_parts:
            line += " [" + " | ".join(bracket_parts) + "]"
        output.append(line)
    output.append("-" * 20)

    # Sekcje LEADY/DEALE (statusy) usuwamy – informacja jest w przeglądzie

    # MODUŁY DODATKOWE – tylko niezerowe, w jednej linii
    custom_counts: Dict[str, int] = details.get('custom_counts', {}) or {}
    if custom_counts:
        nonzero = [f"{k.split('.')[0]}:{v}" for k, v in custom_counts.items() if v]
        if nonzero:
            output.append("MODUŁY CUSTOM: " + " | ".join(nonzero))
            output.append("-" * 20)

    all_activities = details.get('all_activities', [])

    # Sekcję „Ostatnia interakcja” i skrót usuwamy – przechodzimy do pełnej historii

    # Sekcję skróconą usuwamy

    if all_activities:
        if details.get('full_activities'):
            # Pełna lista, ale format jak w kompaktowym: data | ikona(typ) -> temat (bez opisu)
            for activity in all_activities:
                a_type = activity.get('type', 'N/A')
                type_icon = '🧩' if a_type == 'Task' else ('📞' if a_type == 'Call' else ('📅' if a_type == 'Event' else '🧩'))
                date_pl = fmt_date_pl(activity.get('time',''))
                subject = activity.get('Subject', 'Brak tematu')
                output.append(f"  - {date_pl} | {type_icon} -> {subject}")
        else:
            seen = set()
            task_count = 0
            call_count = 0
            event_count = 0
            max_tasks = 1
            max_calls = 5
            max_events = 4
            for activity in all_activities:
                key = (activity.get('type'), fmt_date_pl(activity.get('time','')), (activity.get('Subject') or '').strip())
                if key in seen:
                    continue
                # Limity per typ
                a_type = activity.get('type')
                if a_type == 'Task':
                    if task_count >= max_tasks:
                        continue
                    task_count += 1
                elif a_type == 'Call':
                    if call_count >= max_calls:
                        continue
                    call_count += 1
                elif a_type == 'Event':
                    if event_count >= max_events:
                        continue
                    event_count += 1
                seen.add(key)
                # Ikona wg typu aktywności
                type_icon = '🧩' if a_type == 'Task' else ('📞' if a_type == 'Call' else ('📅' if a_type == 'Event' else '🧩'))
                date_pl = fmt_date_pl(activity.get('time',''))
                subject = activity.get('Subject', 'Brak tematu')
                # W kompaktowym nie pokazujemy opisu, zwłaszcza "Opis: None"
                output.append(f"  - {date_pl} | {type_icon} -> {subject}")
    else:
        output.append("Brak zarejestrowanych aktywności.")

    return "\n".join(output)

# ---- main ----

def main():
    # Przygotowanie katalogu wyników
    base_output_dir = "wyniki_szczegóły_rekordu_firma"
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_dir = os.path.join(base_output_dir, timestamp)
    os.makedirs(run_dir, exist_ok=True)

    setup_logging(run_dir)

    parser = argparse.ArgumentParser(description="Pobiera szczegółowe dane o koncie z Zoho CRM na podstawie NIP.")
    parser.add_argument("nip", help="Numer NIP do wyszukania.")
    args = parser.parse_args()

    nip_to_check = args.nip

    try:
        access_token = get_access_token()

        logging.info("Czekam 2s na aktywację tokena...")
        time.sleep(2)

        account_info = get_account_details_by_nip(access_token, nip_to_check)

        if account_info:
            account_id = account_info.get("id")

            # 1. Wyznaczamy grupę powiązanych kont
            related_account_ids = find_related_account_ids(access_token, account_info)

            # 2. Pobieranie danych bazowych ze wszystkich kont z grupy
            contacts: List[Dict[str, Any]] = []
            deals: List[Dict[str, Any]] = []
            for acc_id in related_account_ids:
                contacts += get_related_records(access_token, "Contacts", acc_id, ["id"])
                deals += get_related_records(access_token, "Deals", acc_id, ["id", "Stage", "Modified_Time"])

            # Leads dla wielu kont – najpierw spróbuj po lookup ID, potem fallback po nazwie
            account_names = [get_account_name(access_token, aid) for aid in related_account_ids]
            names_in = coql_in_list(account_names)
            ids_in = coql_in_id_list(related_account_ids)
            leads = execute_coql_query(
                access_token,
                f"select id, Lead_Status, Modified_Time from Leads where Firma_w_bazie.id in ({ids_in})",
            )
            if leads is None:
                leads = execute_coql_query(
                    access_token,
                    f"select id, Lead_Status, Modified_Time from Leads where Firma_w_bazie in ({names_in})",
                )
            leads = leads or []
            lead_ids = [str(l.get("id")) for l in leads if l.get("id")]

            # 2.1 Dodatkowe moduły (zliczenia i ID dopasowań po nazwie firmy)
            custom_counts: Dict[str, int] = {}
            custom_ids_all: List[str] = []
            custom_refs_all: List[Tuple[str, str]] = []  # (module_api, id)
            def count_records_id_only(module: str, field: str) -> int:
                total = 0
                for batch in _chunk_list(related_account_ids, 50):
                    ids_in_batch = ", ".join(batch)
                    data = execute_coql_query(
                        access_token,
                        f"select id from {module} where {field}.id in ({ids_in_batch})",
                    ) or []
                    total += len(data)
                return total
            def fetch_ids_id_only(module: str, field: str) -> List[str]:
                out: List[str] = []
                seen: set = set()
                for batch in _chunk_list(related_account_ids, 50):
                    ids_in_batch = ", ".join(batch)
                    data = execute_coql_query(
                        access_token,
                        f"select id from {module} where {field}.id in ({ids_in_batch})",
                    ) or []
                    for r in data:
                        if r.get("id"):
                            rid = str(r.get("id"))
                            if rid not in seen:
                                seen.add(rid)
                                out.append(rid)
                return out
            custom_counts["Klienci.Firma_ASU"] = count_records_id_only("Klienci", "Firma_ASU")
            custom_counts["Klienci.Firma_Platnik"] = count_records_id_only("Klienci", "Firma_Platnik")
            custom_counts["USER_Historia.Firma"] = count_records_id_only("USER_Historia", "Firma")
            custom_counts["Marketing_Leads.Firma_w_bazie"] = count_records_id_only("Marketing_Leads", "Firma_w_bazie")
            custom_counts["Edu_Leads.Firma_w_bazie"] = count_records_id_only("Edu_Leads", "Firma_w_bazie")
            custom_counts["Ankiety_Spotkan.Firma"] = count_records_id_only("Ankiety_Spotkan", "Firma")

            # Zbieramy ID rekordów z tych modułów, aby sprawdzić na nich aktywności
            for mod, field in [
                ("Klienci", "Firma_ASU"),
                ("Klienci", "Firma_Platnik"),
                ("USER_Historia", "Firma"),
                ("Marketing_Leads", "Firma_w_bazie"),
                ("Edu_Leads", "Firma_w_bazie"),
                ("Ankiety_Spotkan", "Firma"),
            ]:
                ids = fetch_ids_id_only(mod, field)
                custom_ids_all += ids
                for oid in ids:
                    custom_refs_all.append((mod, oid))

            # Dodatkowo USER_Historia po gościach (kontakty) – GOSC.id wśród kontaktów
            contact_ids = [c['id'] for c in contacts]
            if contact_ids:
                extra_user_historia: List[str] = []
                for batch in _chunk_list(contact_ids, 50):
                    ids_in_batch = ", ".join(batch)
                    rows = execute_coql_query(access_token, f"select id from USER_Historia where GOSC.id in ({ids_in_batch})") or []
                    for r in rows:
                        if r.get("id"):
                            extra_user_historia.append(str(r.get("id")))
                custom_counts["USER_Historia.GOSC"] = custom_counts.get("USER_Historia.GOSC", 0) + len(extra_user_historia)
                for oid in extra_user_historia:
                    custom_refs_all.append(("USER_Historia", oid))

            deal_ids = [d['id'] for d in deals]

            # 3. Aktywności na podstawie pełnych powiązań (wszystkie konta z grupy)
            # Do Who_Id dodajemy zarówno kontakty jak i leady
            # Pomijamy aktywności dla USER_Historia (nie mają aktywności)
            filtered_refs = [(m, oid) for (m, oid) in custom_refs_all if m != "USER_Historia"]
            all_activities = get_all_activities(access_token, related_account_ids, contact_ids, lead_ids, deal_ids, filtered_refs)

            # 4. Przetwarzanie danych
            lead_statuses = [l.get("Lead_Status", "") for l in leads]
            deal_stages = [d.get("Stage", "") for d in deals]

            # Daty ostatnich statusów (przybliżenie: Modified_Time rekordu na danym statusie)
            def latest_date(items: List[Dict[str, Any]], pred) -> str:
                dates: List[str] = []
                for it in items:
                    if pred(it):
                        mt = it.get("Modified_Time") or it.get("Modified_Time".lower())
                        if mt:
                            dates.append(fmt_date_pl(mt))
                return max(dates) if dates else ""
            # Do deals dołóż Modified_Time wcześniej w pobieraniu powiązanych
            # Leads – dołóż Modified_Time w zapytaniu COQL poniżej (już niżej zaktualizujemy select)

            aktywny_lead_exists = any(
                l.get("Lead_Status") in AKTYWNE_LEAD_STATUSY
                for l in leads
            )
            aktywny_deal_exists = any(
                d.get("Stage") in AKTYWNE_DEAL_ETAPY
                for d in deals
            )

            is_customer_based_on_deals = any(
                d.get("Stage") in WYGRANE_DEAL_ETAPY for d in deals
            )
            original_status = account_info.get("Status_klienta", "Brak danych")
            final_status_klienta = "jest" if is_customer_based_on_deals else original_status

            # 5. Agregacja
            final_details = {
                "status_klienta": final_status_klienta,
                "contacts_count": len(contacts),
                "related_accounts_count": len(related_account_ids),
                "leads_count": len(leads),
                "lead_statuses": lead_statuses,
                "aktywny_lead_exists": aktywny_lead_exists,
                "deals_count": len(deals),
                "deal_stages": deal_stages,
                "aktywny_deal_exists": aktywny_deal_exists,
                "all_activities": all_activities,
                "custom_counts": custom_counts,
                "status_counts": {
                    "leads_active": sum(1 for l in leads if l.get("Lead_Status") in AKTYWNE_LEAD_STATUSY),
                    "leads_disqualified": sum(1 for l in leads if l.get("Lead_Status") == "Zdyskwalifikowany"),
                    "deals_active": sum(1 for d in deals if d.get("Stage") in AKTYWNE_DEAL_ETAPY),
                    "deals_won": sum(1 for d in deals if d.get("Stage") in WYGRANE_DEAL_ETAPY),
                    "deals_lost": sum(1 for d in deals if d.get("Stage") == "Closed Lost"),
                },
                "last_dates": {
                    "lead_disqualified": latest_date(leads, lambda l: l.get("Lead_Status") == "Zdyskwalifikowany"),
                    "deal_lost": latest_date(deals, lambda d: d.get("Stage") == "Closed Lost"),
                    "deal_won": latest_date(deals, lambda d: d.get("Stage") in WYGRANE_DEAL_ETAPY),
                },
            }

            # 5. Formatowanie i output (wynik agregowany dla całej grupy)
            # Wyznaczamy nazwę do nagłówka
            display_name = get_display_name(access_token, account_info)
            final_details["display_name"] = display_name
            final_details["nip"] = nip_to_check

            formatted_output = format_final_output(final_details)
            print("\n--- ZESTAWIENIE DANYCH O KLIENCIE ---\n")
            print(formatted_output)
            print("\n-------------------------------------\n")

            # Zapis do pliku wynikowego w katalogu uruchomienia
            result_path = os.path.join(run_dir, "wynik.txt")
            with open(result_path, "w", encoding="utf-8-sig") as f:
                f.write(formatted_output)
            logging.info(f"Zapisano wynik do pliku: {result_path}")

            # Bufor na raport łączony (agregat + sekcje per-placówka)
            combined_parts: List[str] = []
            combined_parts.append("### RAPORT ZBIORCZY – AGREGAT GRUPY\n")
            combined_parts.append(formatted_output)
            combined_parts.append("\n\n### RAPORTY PER-PLACÓWKA\n")

            # 6. Analiza per-konto dla wszystkich kont w grupie i zapis do osobnych plików
            for acc_id in related_account_ids:
                try:
                    acc_name = get_account_name(access_token, acc_id) or acc_id
                    acc_status_orig = get_account_status(access_token, acc_id)

                    # Dane bazowe tylko dla tego konta
                    acc_contacts = get_related_records(access_token, "Contacts", acc_id, ["id"])
                    acc_deals = get_related_records(access_token, "Deals", acc_id, ["id", "Stage", "Modified_Time"])
                    # Leads per-konto – najpierw lookup po ID, potem fallback po nazwie
                    acc_leads = execute_coql_query(
                        access_token,
                        f"select id, Lead_Status, Modified_Time from Leads where Firma_w_bazie.id = {acc_id}",
                    )
                    if acc_leads is None and acc_name:
                        acc_leads = execute_coql_query(
                            access_token,
                            f"select id, Lead_Status, Modified_Time from Leads where Firma_w_bazie = '{coql_escape(acc_name)}'",
                        )
                    acc_leads = acc_leads or []
                    acc_lead_ids = [str(l.get("id")) for l in acc_leads if l.get("id")]

                    # per-konto: pobierz ID rekordów z modułów custom powiązanych nazwą tej placówki
                    acc_other_ids: List[str] = []
                    acc_other_refs: List[Tuple[str, str]] = []
                    # najpierw po ID, potem po nazwie
                    for module, field in [
                        ("Klienci", "Firma_ASU"),
                        ("Klienci", "Firma_Platnik"),
                        ("USER_Historia", "Firma"),
                        ("Marketing_Leads", "Firma_w_bazie"),
                        ("Edu_Leads", "Firma_w_bazie"),
                        ("Ankiety_Spotkan", "Firma"),
                    ]:
                        rows = execute_coql_query(access_token, f"select id from {module} where {field}.id = {acc_id}")
                        for r in rows or []:
                            if r.get("id"):
                                oid = str(r.get("id"))
                                acc_other_ids.append(oid)
                                acc_other_refs.append((module, oid))

                    acc_contact_ids: List[str] = [c.get('id') for c in acc_contacts if c.get('id')]
                    acc_deal_ids: List[str] = [d.get('id') for d in acc_deals if d.get('id')]

                    acc_acts = get_all_activities(access_token, [acc_id], acc_contact_ids, acc_lead_ids, acc_deal_ids, acc_other_refs)

                    acc_lead_statuses = [l.get("Lead_Status", "") for l in acc_leads]
                    acc_deal_stages = [d.get("Stage", "") for d in acc_deals]

                    # Daty ostatnich statusów (przybliżenie: Modified_Time rekordu na danym statusie)
                    def latest_date(items: List[Dict[str, Any]], pred) -> str:
                        dates: List[str] = []
                        for it in items:
                            if pred(it):
                                mt = it.get("Modified_Time") or it.get("Modified_Time".lower())
                                if mt:
                                    dates.append(fmt_date_pl(mt))
                        return max(dates) if dates else ""
                    # Do deals dołóż Modified_Time wcześniej w pobieraniu powiązanych
                    # Leads – dołóż Modified_Time w zapytaniu COQL poniżej (już niżej zaktualizujemy select)

                    acc_active_lead = any(
                        l.get("Lead_Status") in AKTYWNE_LEAD_STATUSY
                        for l in acc_leads
                    )
                    acc_active_deal = any(d.get("Stage") in AKTYWNE_DEAL_ETAPY for d in acc_deals)
                    acc_customer_by_deals = any(d.get("Stage") in WYGRANE_DEAL_ETAPY for d in acc_deals)
                    acc_final_status = "jest" if acc_customer_by_deals else acc_status_orig

                    acc_details = {
                        "status_klienta": acc_final_status,
                        "contacts_count": len(acc_contacts),
                        "related_accounts_count": 1,
                        "leads_count": len(acc_leads),
                        "lead_statuses": acc_lead_statuses,
                        "aktywny_lead_exists": acc_active_lead,
                        "deals_count": len(acc_deals),
                        "deal_stages": acc_deal_stages,
                        "aktywny_deal_exists": acc_active_deal,
                        "all_activities": acc_acts,
                        "full_activities": True,
                        # per-konto: proste zliczenie po nazwie tylko dla tego konta
                        "custom_counts": {
                            "Klienci.Firma_ASU": len(execute_coql_query(access_token, f"select id from Klienci where Firma_ASU.id = {acc_id}") or []),
                            "Klienci.Firma_Platnik": len(execute_coql_query(access_token, f"select id from Klienci where Firma_Platnik.id = {acc_id}") or []),
                            "USER_Historia.Firma": len(execute_coql_query(access_token, f"select id from USER_Historia where Firma.id = {acc_id}") or []),
                            "Marketing_Leads.Firma_w_bazie": len(execute_coql_query(access_token, f"select id from Marketing_Leads where Firma_w_bazie.id = {acc_id}") or []),
                            "Edu_Leads.Firma_w_bazie": len(execute_coql_query(access_token, f"select id from Edu_Leads where Firma_w_bazie.id = {acc_id}") or []),
                            "Ankiety_Spotkan.Firma": len(execute_coql_query(access_token, f"select id from Ankiety_Spotkan where Firma.id = {acc_id}") or []),
                        },
                        "status_counts": {
                            "leads_active": sum(1 for l in acc_leads if l.get("Lead_Status") in AKTYWNE_LEAD_STATUSY),
                            "leads_disqualified": sum(1 for l in acc_leads if l.get("Lead_Status") == "Zdyskwalifikowany"),
                            "deals_active": sum(1 for d in acc_deals if d.get("Stage") in AKTYWNE_DEAL_ETAPY),
                            "deals_won": sum(1 for d in acc_deals if d.get("Stage") in WYGRANE_DEAL_ETAPY),
                            "deals_lost": sum(1 for d in acc_deals if d.get("Stage") == "Closed Lost"),
                        },
                        "last_dates": {
                            "lead_disqualified": latest_date(acc_leads, lambda l: l.get("Lead_Status") == "Zdyskwalifikowany"),
                            "deal_lost": latest_date(acc_deals, lambda d: d.get("Stage") == "Closed Lost"),
                            "deal_won": latest_date(acc_deals, lambda d: d.get("Stage") in WYGRANE_DEAL_ETAPY),
                        },
                    }

                    acc_details["display_name"] = acc_name
                    acc_text = f"Placówka: {acc_name} (ID: {acc_id})\n" + format_final_output(acc_details)
                    safe_name = "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in str(acc_name) or str(acc_id))
                    per_file = os.path.join(run_dir, f"wynik_{safe_name}.txt")
                    with open(per_file, "w", encoding="utf-8-sig") as f:
                        f.write(acc_text)
                    logging.info(f"Zapisano wynik dla konta {acc_id} do pliku: {per_file}")

                    # Dodanie do raportu zbiorczego
                    combined_parts.append(f"\n--- {acc_name} (ID: {acc_id}) ---\n")
                    combined_parts.append(acc_text)
                except Exception as sub_e:
                    logging.error(f"Błąd podczas analizy konta {acc_id}: {sub_e}")

            # Zapis raportu zbiorczego do jednego pliku
            combined_path = os.path.join(run_dir, "raport_spojny.txt")
            with open(combined_path, "w", encoding="utf-8-sig") as f:
                f.write("\n".join(combined_parts))
            logging.info(f"Zapisano spójny raport: {combined_path}")

        else:
            print(f"\nNie udało się znaleźć informacji dla NIP: {nip_to_check}")

    except Exception as e:
        logging.error(f"Wystąpił nieoczekiwany błąd: {e}", exc_info=True)


if __name__ == "__main__":
    main()

