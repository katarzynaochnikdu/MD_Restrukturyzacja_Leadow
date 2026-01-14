import argparse
import csv
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Set

# Zależności opcjonalne (wyłącznie do wczytania CSV w trybie awaryjnym) – podstawą jest csv.DictReader
try:
    import pandas as pd
except Exception:
    pd = None  # nie wymagamy bezwzględnie
try:
    from tqdm import tqdm
except Exception:
    tqdm = None  # pasek postępu opcjonalny

# Wykorzystujemy istniejące mechanizmy dostępu do Zoho z pliku check_nip_in_zoho.py
from check_nip_in_zoho import (
    get_access_token,
    execute_coql_query,
    ACCESS_TOKEN_HOLDER,
    ZOHO_CRM_API_BASE_URL,
)

# Nie modyfikujemy istniejących plików – konfiguracja logowania lokalnie
def setup_logging_for_run(log_dir: str) -> None:
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "log.txt")
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
    handlers = [logging.FileHandler(log_file, encoding="utf-8")]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] - %(message)s",
        handlers=handlers,
    )


def _post_with_retry(access_token: str, url: str, body: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    data_bytes = json.dumps(body or {}).encode("utf-8")

    def _do_post(tok: str) -> Optional[Dict[str, Any]]:
        req = urllib.request.Request(
            url,
            data=data_bytes,
            headers={
                "Authorization": f"Zoho-oauthtoken {tok}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            text = resp.read().decode("utf-8")
            if not text:
                return {}
            return json.loads(text)

    try:
        tok0 = ACCESS_TOKEN_HOLDER.get("token") or access_token
        return _do_post(tok0)
    except urllib.error.HTTPError as e:
        body_text = ""
        try:
            body_text = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        logging.error(f"HTTPError POST {e.code} for {url}: {body_text}")
        if e.code in (401, 403):
            # Spróbuj pobrać świeży token i ponów raz
            try:
                fresh = get_access_token()
                ACCESS_TOKEN_HOLDER["token"] = fresh
                return _do_post(fresh)
            except Exception as e2:
                logging.error(f"Ponowienie po odświeżeniu tokena nie powiodło się: {e2}")
                return None
        return None
    except Exception as e:
        logging.error(f"Błąd POST {url}: {e}")
        return None


def _get_with_retry(access_token: str, url: str) -> Optional[Dict[str, Any]]:
    def _do_get(tok: str) -> Optional[Dict[str, Any]]:
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Zoho-oauthtoken {tok}",
            },
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            text = resp.read().decode("utf-8")
            if not text:
                return {}
            return json.loads(text)

    try:
        tok0 = ACCESS_TOKEN_HOLDER.get("token") or access_token
        return _do_get(tok0)
    except urllib.error.HTTPError as e:
        body_text = ""
        try:
            body_text = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        logging.error(f"HTTPError GET {e.code} for {url}: {body_text}")
        if e.code in (401, 403):
            try:
                fresh = get_access_token()
                ACCESS_TOKEN_HOLDER["token"] = fresh
                return _do_get(fresh)
            except Exception as e2:
                logging.error(f"Ponowienie GET po odświeżeniu tokena nie powiodło się: {e2}")
                return None
        return None
    except Exception as e:
        logging.error(f"Błąd GET {url}: {e}")
        return None


def write_status_line(message: str) -> None:
    try:
        if tqdm is not None:
            tqdm.write(message, file=sys.stderr)
        else:
            sys.stderr.write(message + "\n")
            sys.stderr.flush()
    except Exception:
        pass


def get_parent_account_id(access_token: str, account_id: str) -> Optional[str]:
    rows = execute_coql_query(access_token, f"select Parent_Account from Accounts where id = {account_id}") or []
    if not rows:
        return None
    parent = rows[0].get("Parent_Account")
    if isinstance(parent, dict) and parent.get("id"):
        return str(parent["id"])  # typ lookup
    if isinstance(parent, str) and parent:
        return parent
    return None


def get_children_account_ids(access_token: str, parent_id: str) -> List[str]:
    # Uwaga: COQL bez paginacji zwykle zwraca do 200 rekordów – przy dużych drzewach można rozważyć OFFSET
    rows = execute_coql_query(access_token, f"select id from Accounts where Parent_Account.id = {parent_id}") or []
    return [str(r.get("id")) for r in rows if r.get("id")]


def build_hierarchy_ids(access_token: str, any_account_id: str) -> List[str]:
    # 1) Idź do korzenia (rekord bez rodzica)
    root_id = any_account_id
    safety = 0
    while safety < 50:
        safety += 1
        pid = get_parent_account_id(access_token, root_id)
        if not pid:
            break
        root_id = pid

    # 2) BFS po wszystkich potomkach
    queue: List[str] = [root_id]
    seen: Set[str] = set()
    result: List[str] = []
    while queue:
        current = queue.pop(0)
        if current in seen:
            continue
        seen.add(current)
        result.append(current)
        try:
            children = get_children_account_ids(access_token, current)
        except Exception as e:
            logging.error(f"Błąd pobierania dzieci dla {current}: {e}")
            children = []
        for cid in children:
            if cid not in seen:
                queue.append(cid)
        # Delikatna pauza, aby nie przekroczyć limitów
        time.sleep(0.1)
    return result


def get_record_tags(access_token: str, account_id: str) -> List[str]:
    # Pobieramy pole Tag przez COQL (stabilniejsze niż relacja /tags)
    rows = execute_coql_query(
        access_token,
        f"select Tag from Accounts where id = {account_id}",
    ) or []
    if not rows:
        return []
    try:
        tag_field = rows[0].get("Tag")
        if not tag_field:
            return []
        # Pole Tag jest listą obiektów {name: "..."}
        out: List[str] = []
        for t in tag_field:
            if isinstance(t, dict) and t.get("name"):
                out.append(str(t.get("name")))
            elif isinstance(t, str):
                out.append(t)
        return out
    except Exception:
        return []


def get_accounts_nips(access_token: str, ids: List[str]) -> Dict[str, str]:
    """Zwraca mapę {id -> Firma_NIP} dla podanych ID kont (batch COQL)."""
    result: Dict[str, str] = {}
    if not ids:
        return result
    # bezpieczne porcje (<=50 ID)
    def chunks(items: List[str], size: int) -> List[List[str]]:
        return [items[i : i + size] for i in range(0, len(items), size)]
    for batch in chunks(ids, 50):
        ids_in = ", ".join(batch)
        try:
            rows = execute_coql_query(access_token, f"select id, Firma_NIP from Accounts where id in ({ids_in})") or []
            for r in rows:
                rid = str(r.get("id") or "")
                nip = str(r.get("Firma_NIP") or "")
                if rid:
                    result[rid] = nip
        except Exception:
            continue
        time.sleep(0.05)
    return result


def add_tag_to_accounts(access_token: str, account_ids: List[str], tag_name: str) -> Tuple[List[str], List[Tuple[str, str]]]:
    """
    Zwraca (lista_sukcesów, lista_błędów[(id, komunikat)]).
    Wysyła wsadowo, po 100 ID.
    """
    successes: List[str] = []
    failures: List[Tuple[str, str]] = []
    if not account_ids:
        return successes, failures

    # Chunkuj po 100 ID (bezpieczny rozmiar zapytania)
    def chunks(items: List[str], size: int) -> List[List[str]]:
        return [items[i : i + size] for i in range(0, len(items), size)]

    for batch in chunks(account_ids, 100):
        params = {
            "ids": ",".join(batch),
        }
        url = f"{ZOHO_CRM_API_BASE_URL}/Accounts/actions/add_tags?{urllib.parse.urlencode(params)}"
        logging.info(f"Dodawanie tagu do {len(batch)} rekordów Accounts")
        # W API wymagane jest body z kluczem 'tags'
        body = {"tags": [{"name": tag_name}]}
        payload = _post_with_retry(access_token, url, body=body)
        if not payload:
            for rid in batch:
                failures.append((rid, "Brak odpowiedzi API"))
            continue
        # Odpowiedź sukcesów/błędów – standard: data i możliwe błędy per rekord
        try:
            details = payload.get("data", [])
            if isinstance(details, list) and details:
                for item in details:
                    rid = str(item.get("details", {}).get("id") or "")
                    status = (item.get("status") or "").upper()
                    code = str(item.get("code") or "")
                    if status == "SUCCESS" and rid:
                        successes.append(rid)
                    else:
                        failures.append((rid or "N/A", code or "Błąd dodawania tagu"))
            else:
                # Jeśli brak sekcji data – załóżmy sukces dla batcha (API bywa lakoniczne)
                successes.extend(batch)
        except Exception as e:
            for rid in batch:
                failures.append((rid, f"Błąd parsowania odpowiedzi: {e}"))
        time.sleep(0.2)
    return successes, failures


def _normalize_header(name: str) -> str:
    try:
        return "".join(ch for ch in str(name).strip().lower() if ch.isalnum())
    except Exception:
        return ""


def _choose_id_column(df) -> Optional[str]:
    # Priorytet: kolumny zawierające frazę "Identyfikator rekordu" lub "ID rekordu"
    # (case-insensitive, ignorujemy spacje/znaki specjalne)
    normalized_to_original: Dict[str, str] = { _normalize_header(c): c for c in df.columns }
    targets_in_order = [
        "identyfikatorrekordu",
        "idrekordu",
    ]
    for target in targets_in_order:
        for norm, orig in normalized_to_original.items():
            if target in norm:
                return orig
    # Drugi priorytet: skrócone formy (zachowane dla kompatybilności)
    fallback_exact = {
        "identyfikator",
        "identyfiaktor",  # literówka wspierana
        "id",
    }
    for norm, orig in normalized_to_original.items():
        if norm in fallback_exact:
            logging.warning(
                f"Nie znaleziono kolumny z frazą 'Identyfikator rekordu' / 'ID rekordu'. Użyto kolumny pomocniczej: {orig}"
            )
            return orig
    # Fallback: pierwsza kolumna
    logging.warning(
        "Nie znaleziono dopasowania kolumny po nazwie. Używam pierwszej kolumny z pliku."
    )
    try:
        return df.columns[0]
    except Exception:
        return None


def read_ids_from_any(path: str) -> List[str]:
    # Preferuj pandas jeśli dostępny (obsługa CSV i Excel)
    ids: List[str] = []
    ext = os.path.splitext(path)[1].lower()
    if pd is not None:
        try:
            if ext in (".xlsx", ".xls"):
                df = pd.read_excel(path, dtype=str)
            else:
                try:
                    df = pd.read_csv(path, dtype=str, sep=";")
                except Exception:
                    df = pd.read_csv(path, dtype=str)
            if df is None or df.empty:
                return []
            col = _choose_id_column(df)
            if not col:
                return []
            logging.info(f"Wybrano kolumnę z ID: {col}")
            for v in df[col].tolist():
                if v is None or (isinstance(v, float) and str(v) == "nan"):
                    continue
                s = str(v).strip()
                if s:
                    ids.append(s)
            return ids
        except Exception:
            pass
    # Fallback bez pandas – tylko CSV
    if ext in (".xlsx", ".xls"):
        raise RuntimeError("Do odczytu plików Excel wymagane jest pandas (i openpyxl). Zainstaluj: pip install pandas openpyxl")
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except Exception:
            dialect = csv.excel
        reader = csv.reader(f, dialect)
        rows = list(reader)
        if not rows:
            return []
        # Spróbuj wyłuskać nagłówek i wybrać kolumnę
        header = rows[0]
        if header and all(isinstance(c, str) for c in header):
            col_idx = 0
            best_idx = None
            # Najpierw frazy priorytetowe
            for i, col_name in enumerate(header):
                nm = _normalize_header(col_name)
                if ("identyfikatorrekordu" in nm) or ("idrekordu" in nm):
                    best_idx = i
                    break
            # Następnie krótkie formy
            if best_idx is None:
                for i, col_name in enumerate(header):
                    if _normalize_header(col_name) in {"identyfikator", "identyfiaktor", "id"}:
                        best_idx = i
                        break
            if best_idx is None:
                best_idx = 0
                logging.warning("Fallback: użyto pierwszej kolumny (brak rozpoznanej nazwy kolumny z ID)")
            else:
                try:
                    logging.info(f"Wybrano kolumnę z ID: {header[best_idx]}")
                except Exception:
                    pass
            for row in rows[1:]:
                if not row or best_idx >= len(row):
                    continue
                s = str(row[best_idx]).strip()
                if s:
                    ids.append(s)
        else:
            # Brak sensownych nagłówków – bierz pierwszą kolumnę
            for row in rows:
                if not row:
                    continue
                s = str(row[0]).strip()
                if s:
                    ids.append(s)
    return ids


def prompt_for_tag_with_confirmation(initial: Optional[str] = None) -> str:
    """Pobiera od użytkownika nazwę tagu i prosi o potwierdzenie.
    Pętla trwa, dopóki użytkownik nie potwierdzi wyboru.
    """
    suggested = (initial or "").strip() or "202509 MIGTEL znane"
    while True:
        try:
            entered = input(f"Podaj nazwę tagu do ustawienia [{suggested}]: ").strip()
        except Exception:
            entered = ""
        tag = entered or suggested
        if not tag:
            try:
                sys.stderr.write("Tag nie może być pusty.\n")
                sys.stderr.flush()
            except Exception:
                pass
            continue
        try:
            print(f"Wybrany tag: '{tag}'")
            confirm = input("Potwierdź tag (T/tak/y/yes aby kontynuować, inny klawisz aby zmienić): ").strip().lower()
        except Exception:
            confirm = ""
        if confirm in ("", "t", "tak", "y", "yes", "1"):
            return tag
        # jeśli brak potwierdzenia – powtórz z ostatnią propozycją
        suggested = tag

def main() -> None:
    parser = argparse.ArgumentParser(description="Tagowanie hierarchii kont (Accounts) na podstawie listy ID z CSV/XLSX.")
    parser.add_argument("input_file", nargs="?", help="Ścieżka do pliku CSV lub Excel z listą ID (kolumna 'Identyfiaktor' lub 'ID').")
    parser.add_argument("--tag", default="202509 MIGTEL znane", help="Nazwa tagu do ustawienia.")
    parser.add_argument("--outdir", default="tagowanie", help="Katalog wyjściowy na logi i raport.")
    args = parser.parse_args()

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_dir = os.path.join(args.outdir, timestamp)
    setup_logging_for_run(run_dir)
    try:
        # Zapowiedź miejsca zapisu wyników (para plików dla uruchomienia)
        expected_report = os.path.join(run_dir, f"raport_tagowania_{timestamp}.csv")
        expected_report_xlsx = os.path.join(run_dir, f"raport_tagowania_{timestamp}.xlsx")
        expected_log = os.path.join(run_dir, "log.txt")
        write_status_line(
            f"Wyniki tego uruchomienia: {run_dir} | pliki: raport -> {os.path.basename(expected_report)}, raport_xlsx -> {os.path.basename(expected_report_xlsx)}, log -> {os.path.basename(expected_log)}"
        )
    except Exception:
        pass

    # 1) Najpierw wybór i potwierdzenie tagu
    tag_name = prompt_for_tag_with_confirmation(getattr(args, "tag", None))
    try:
        write_status_line(f"Wybrany tag do ustawienia: {tag_name}")
    except Exception:
        pass
    logging.info(f"Tag do dodania: {tag_name}")

    # 2) Po potwierdzeniu tagu – prosimy o ścieżkę do pliku wejściowego
    input_path = args.input_file
    if not input_path:
        raw_path = input("Przeciągnij plik CSV/XLSX do okna i naciśnij Enter: ")
        clean_path = raw_path.strip()
        if clean_path.startswith("& "):
            clean_path = clean_path[2:].strip()
        input_path = clean_path.strip("'\"")

    if not os.path.exists(input_path):
        print(f"Błąd: Plik '{input_path}' nie istnieje.", file=sys.stderr)
        sys.exit(1)

    logging.info(f"Start tagowania kont z pliku: {input_path}")
    logging.info(f"Tag do dodania: {tag_name}")
    try:
        access_token = get_access_token()
        ACCESS_TOKEN_HOLDER["token"] = access_token
        time.sleep(1.5)
    except Exception as e:
        logging.error(f"Błąd uzyskiwania tokena: {e}")
        sys.exit(1)

    try:
        seed_ids = read_ids_from_any(input_path)
        seed_ids = [s for s in map(lambda x: str(x).strip(), seed_ids) if s]
        if not seed_ids:
            logging.warning("Plik nie zawiera ID w kolumnie 'Identyfiaktor' / 'ID' ani w pierwszej kolumnie.")
            return
        logging.info(f"Wczytano {len(seed_ids)} ID startowych.")
    except Exception as e:
        logging.error(f"Błąd odczytu CSV: {e}")
        sys.exit(1)

    # Zbuduj pełną listę unikalnych ID do otagowania w oparciu o hierarchie
    all_to_tag: Set[str] = set()
    per_seed_hierarchy_count: List[Tuple[str, int]] = []
    seed_to_hierarchy: Dict[str, List[str]] = {}
    if tqdm is not None:
        with tqdm(total=len(seed_ids), desc="Budowa hierarchii", unit="ID") as pbar:
            for idx, seed in enumerate(seed_ids, start=1):
                try:
                    hierarchy = build_hierarchy_ids(access_token, seed)
                    seed_to_hierarchy[seed] = hierarchy
                    for rid in hierarchy:
                        all_to_tag.add(rid)
                    per_seed_hierarchy_count.append((seed, len(hierarchy)))
                    write_status_line(f"[OK] {seed} -> hierarchia {len(hierarchy)} rekordów")
                except Exception as e:
                    write_status_line(f"[UWAGA] {seed} -> błąd budowy hierarchii: {e}")
                pbar.update(1)
                time.sleep(0.1)
    else:
        for idx, seed in enumerate(seed_ids, start=1):
            try:
                hierarchy = build_hierarchy_ids(access_token, seed)
                seed_to_hierarchy[seed] = hierarchy
                for rid in hierarchy:
                    all_to_tag.add(rid)
                per_seed_hierarchy_count.append((seed, len(hierarchy)))
                write_status_line(f"[OK] {seed} -> hierarchia {len(hierarchy)} rekordów")
            except Exception as e:
                write_status_line(f"[UWAGA] {seed} -> błąd budowy hierarchii: {e}")
            time.sleep(0.1)

    ids_list = sorted(all_to_tag)
    logging.info(f"Łącznie do otagowania: {len(ids_list)} rekordów Accounts")

    # Pobierz istniejące tagi (analityka)
    id_to_tags: Dict[str, List[str]] = {}
    if tqdm is not None:
        with tqdm(total=len(ids_list), desc="Pobieranie tagów", unit="rekord") as pbar:
            for rid in ids_list:
                try:
                    id_to_tags[rid] = get_record_tags(access_token, rid)
                except Exception:
                    id_to_tags[rid] = []
                pbar.update(1)
                time.sleep(0.05)
    else:
        for rid in ids_list:
            try:
                id_to_tags[rid] = get_record_tags(access_token, rid)
            except Exception:
                id_to_tags[rid] = []
            time.sleep(0.05)

    # Pobierz NIP-y dla wszystkich rekordów (do raportu)
    id_to_nip: Dict[str, str] = {}
    try:
        id_to_nip = get_accounts_nips(access_token, ids_list)
    except Exception:
        id_to_nip = {rid: "" for rid in ids_list}

    # Tagowanie – wsadowo, z podsumowaniem
    successes, failures = add_tag_to_accounts(access_token, ids_list, tag_name)
    success_set = set(successes)
    failure_map: Dict[str, str] = {rid: err for rid, err in failures}

    # Druk linii per rekord (wynik | ID | istniejące tagi)
    if tqdm is not None:
        with tqdm(total=len(ids_list), desc="Status tagowania", unit="rekord") as pbar:
            for rid in ids_list:
                prev_tags = id_to_tags.get(rid, [])
                had_before = tag_name in prev_tags
                if rid in success_set:
                    result = ("JUŻ BYŁ" if had_before else "DODANO")
                    write_status_line(f"[OK] {result} | {rid} | {', '.join(prev_tags) if prev_tags else 'brak tagów'}")
                else:
                    err = failure_map.get(rid, "BŁĄD")
                    write_status_line(f"[BŁĄD] {rid} | {err} | {', '.join(prev_tags) if prev_tags else 'brak tagów'}")
                pbar.update(1)
                time.sleep(0.02)
    else:
        for rid in ids_list:
            prev_tags = id_to_tags.get(rid, [])
            had_before = tag_name in prev_tags
            if rid in success_set:
                result = ("JUŻ BYŁ" if had_before else "DODANO")
                write_status_line(f"[OK] {result} | {rid} | {', '.join(prev_tags) if prev_tags else 'brak tagów'}")
            else:
                err = failure_map.get(rid, "BŁĄD")
                write_status_line(f"[BŁĄD] {rid} | {err} | {', '.join(prev_tags) if prev_tags else 'brak tagów'}")
            time.sleep(0.02)

    # Analityka per grupa (seed)
    per_seed_stats: List[Tuple[str, int, int, int, int]] = []  # seed, hier_size, already_had, newly_tagged, total_after
    for seed, hierarchy in seed_to_hierarchy.items():
        already_had = sum(1 for rid in hierarchy if tag_name in (id_to_tags.get(rid, [])))
        newly_tagged = sum(1 for rid in hierarchy if (rid in success_set) and (tag_name not in (id_to_tags.get(rid, []))))
        total_after = already_had + newly_tagged
        per_seed_stats.append((seed, len(hierarchy), already_had, newly_tagged, total_after))
        write_status_line(f"[SUMA] {seed} | razem: {len(hierarchy)} | już miało: {already_had} | dodano: {newly_tagged} | łącznie ma po: {total_after}")

    # Raport końcowy CSV + XLSX
    report_csv = os.path.join(run_dir, f"raport_tagowania_{timestamp}.csv")
    with open(report_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["Wejściowe ID (seed)", "Wielkość hierarchii", "Już miało tag", "Dodano nowy tag", "Łącznie ma po"])
        for seed, sz, had, added, total_after in per_seed_stats:
            writer.writerow([seed, sz, had, added, total_after])
        writer.writerow([])
        writer.writerow(["Wejściowe ID (seed)", "Firma_NIP", "ID rekordu", "Miał tag przed", "Działanie", "Ma tag po", "Status", "Tagi (przed)", "Tagi (po)", "Tag docelowy"])
        for seed, hierarchy in seed_to_hierarchy.items():
            for rid in hierarchy:
                prev_tags = id_to_tags.get(rid, [])
                had_before = tag_name in prev_tags
                if rid in success_set:
                    status_txt = ("SUCCESS (już był)" if had_before else "SUCCESS (dodano)")
                    action = ("JUŻ POSIADAŁ" if had_before else "DODANO TAG")
                else:
                    status_txt = f"ERROR: {failure_map.get(rid, 'BŁĄD')}"
                    action = "BŁĄD"
                has_after = had_before or (rid in success_set)
                # Tagi po – szacowane: jeśli dodano i nie było, dołóż tag docelowy do listy
                if has_after and (tag_name not in prev_tags):
                    tags_after_list = prev_tags + [tag_name]
                else:
                    tags_after_list = prev_tags
                writer.writerow([
                    seed,
                    id_to_nip.get(rid, ""),
                    rid,
                    ("TAK" if had_before else "NIE"),
                    action,
                    ("TAK" if has_after else "NIE"),
                    status_txt,
                    ", ".join(prev_tags),
                    ", ".join(sorted(set(tags_after_list))),
                    tag_name,
                ])

    # XLSX (jeśli dostępne pandas + openpyxl)
    report_xlsx = os.path.join(run_dir, f"raport_tagowania_{timestamp}.xlsx")
    try:
        if pd is not None:
            # Przygotuj DF dla dwóch arkuszy
            df_summary = pd.DataFrame(per_seed_stats, columns=[
                "Wejściowe ID (seed)", "Wielkość hierarchii", "Już miało tag", "Dodano nowy tag", "Łącznie ma po"
            ])
            detailed_rows = []
            for seed, hierarchy in seed_to_hierarchy.items():
                for rid in hierarchy:
                    prev_tags = id_to_tags.get(rid, [])
                    had_before = tag_name in prev_tags
                    if rid in success_set:
                        status_txt = ("SUCCESS (już był)" if had_before else "SUCCESS (dodano)")
                        action = ("JUŻ POSIADAŁ" if had_before else "DODANO TAG")
                    else:
                        status_txt = f"ERROR: {failure_map.get(rid, 'BŁĄD')}"
                        action = "BŁĄD"
                    has_after = had_before or (rid in success_set)
                    if has_after and (tag_name not in prev_tags):
                        tags_after_list = prev_tags + [tag_name]
                    else:
                        tags_after_list = prev_tags
                    detailed_rows.append([
                        seed,
                        id_to_nip.get(rid, ""),
                        rid,
                        ("TAK" if had_before else "NIE"),
                        action,
                        ("TAK" if has_after else "NIE"),
                        status_txt,
                        ", ".join(prev_tags),
                        ", ".join(sorted(set(tags_after_list))),
                        tag_name,
                    ])
            df_details = pd.DataFrame(detailed_rows, columns=[
                "Wejściowe ID (seed)", "Firma_NIP", "ID rekordu", "Miał tag przed", "Działanie", "Ma tag po", "Status", "Tagi (przed)", "Tagi (po)", "Tag docelowy"
            ])
            with pd.ExcelWriter(report_xlsx, engine="openpyxl") as writer:
                df_summary.to_excel(writer, index=False, sheet_name="Podsumowanie")
                df_details.to_excel(writer, index=False, sheet_name="Szczegóły")
            logging.info(f"Zapisano raport XLSX: {report_xlsx}")
        else:
            logging.warning("Pominięto zapis XLSX (brak pandas). Zainstaluj: pip install pandas openpyxl")
    except Exception as e:
        logging.warning(f"Nie udało się zapisać XLSX: {e}")

    logging.info(f"Zakończono. Sukcesy: {len(successes)}, błędy: {len(failures)}")
    logging.info(f"Raport CSV: {report_csv}")
    try:
        if os.path.exists(report_xlsx):
            logging.info(f"Raport XLSX: {report_xlsx}")
    except Exception:
        pass
    try:
        write_status_line(
            f"Zapisano pliki w: {run_dir} | raport CSV: {os.path.basename(report_csv)} | raport XLSX: {os.path.basename(report_xlsx)} | log: log.txt"
        )
    except Exception:
        pass


if __name__ == "__main__":
    main()


