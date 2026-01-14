"""Flask GUI Server dla interaktywnego scalania firm."""

import json
import logging
import sys
import webbrowser
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from flask import Flask, render_template, request, jsonify, session
from modules.token_manager import TokenManager
from modules.zoho_api_client import ZohoAPIClient
from modules.account_scorer import AccountScorer
from modules.account_merger import AccountMerger
from modules.data_sanitizer import DataSanitizer
from modules.phone_formatter import PhoneFormatter


app = Flask(__name__)
app.secret_key = "zoho_cleanup_secret_key_change_in_production"

# Konfiguracja loggera
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Globalne dane sesji
merge_data: Dict[str, Any] = {
    "duplicates": [],
    "all_accounts": [],
    "current_index": 0,
    "results": [],
    "api_client": None,
    "merger": None,
    "scorer": None,
    "dry_run": True,
    "run_dir": None
}


@app.route("/")
def index():
    """Strona główna - interfejs scalania."""
    return render_template("merge_interface.html")


@app.route("/api/get_logs")
def get_logs():
    """Zwraca ścieżkę do aktualnych logów."""
    try:
        run_dir = merge_data.get("run_dir")
        if run_dir:
            log_file = run_dir / "cleanup.log"
            if log_file.exists():
                return jsonify({
                    "success": True,
                    "log_path": str(log_file.absolute()),
                    "run_dir": str(run_dir.absolute())
                })
        
        return jsonify({
            "success": False,
            "message": "Logi nie są jeszcze dostępne"
        })
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500


@app.route("/api/get_progress")
def get_progress():
    """Zwraca postęp pobierania (dla live updates podczas init)."""
    try:
        return jsonify({
            "success": True,
            "fetched_accounts": len(merge_data.get("all_accounts", [])),
            "found_duplicates": len(merge_data.get("duplicates", [])),
            "status": "fetching"
        })
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500


@app.route("/api/init", methods=["POST"])
def initialize():
    """Inicjalizacja - pobieranie firm i identyfikacja duplikatów."""
    try:
        data = request.json
        limit = data.get("limit", 100)
        dry_run = data.get("dry_run", True)
        
        logger.info("Inicjalizacja GUI - limit: %d, dry_run: %s", limit, dry_run)
        
        # Utworzenie folderu roboczego
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = Path(f"run_{timestamp}_gui")
        run_dir.mkdir(exist_ok=True)
        
        merge_data["run_dir"] = run_dir
        merge_data["dry_run"] = dry_run
        
        # Inicjalizuj API
        token_manager = TokenManager()
        access_token = token_manager.get_access_token()
        api_client = ZohoAPIClient(access_token)
        
        scorer = AccountScorer(api_client)
        merger = AccountMerger(api_client, scorer)
        
        merge_data["api_client"] = api_client
        merge_data["scorer"] = scorer
        merge_data["merger"] = merger
        
        # KROK 1: Pobierz WSZYSTKIE potrzebne firmy NARAZ (smart fetch)
        logger.info("KROK 1/4: Pobieranie firm...")
        merge_data["all_accounts"] = []  # Reset dla progress tracking
        
        if limit > 0 and limit <= 1000:
            # Smart fetch - pobiera dopóki nie znajdzie X par duplikatów
            accounts = _fetch_until_duplicates_smart_with_progress(api_client, merger, limit)
        else:
            # Pełna baza lub duży limit
            accounts = _fetch_all_with_progress(api_client, limit)
        
        logger.info("✓ Pobrano %d firm", len(accounts))
        merge_data["all_accounts"] = accounts
        
        # KROK 2: Backup (bezpieczeństwo)
        logger.info("KROK 2/4: Tworzenie backupu...")
        backup_file = run_dir / "backup_accounts.json"
        with open(backup_file, "w", encoding="utf-8") as f:
            json.dump(accounts, f, ensure_ascii=False, indent=2)
        logger.info("✓ Backup zapisany: %s", backup_file)
        
        # KROK 3: Znajdź WSZYSTKIE duplikaty NARAZ
        logger.info("KROK 3/4: Szukanie WSZYSTKICH duplikatów...")
        duplicates_groups = merger.find_duplicates(accounts)
        logger.info("✓ Znaleziono %d grup duplikatów", len(duplicates_groups))
        
        # KROK 4: Oblicz scoring dla WSZYSTKICH firm w duplikatach NARAZ
        logger.info("KROK 4/4: Obliczanie scoringu...")
        
        # Zbierz wszystkie firmy w duplikatach
        all_dup_accounts = []
        for group in duplicates_groups.values():
            all_dup_accounts.extend(group)
        
        # Deduplikacja (ta sama firma może być w wielu grupach)
        unique_accounts = {acc["id"]: acc for acc in all_dup_accounts}.values()
        logger.info("Scoring dla %d unikalnych firm...", len(unique_accounts))
        
        # Oblicz scoring dla wszystkich
        for i, account in enumerate(unique_accounts, 1):
            if "_score" not in account:
                try:
                    if i % 5 == 0:  # Log co 5 firm
                        logger.info("  Scoring %d/%d...", i, len(unique_accounts))
                    score_result = scorer.calculate_full_score(account["id"])
                    account["_score"] = score_result
                except Exception as exc:
                    logger.error("Błąd scoringu dla %s: %s", account["id"], exc)
                    account["_score"] = {"total_score": 0}
        
        logger.info("✓ Scoring ukończony")
        
        # KROK 5: Przygotuj WSZYSTKIE pary do GUI (lista gotowa!)
        duplicates_list = []
        for key, group in duplicates_groups.items():
            
            # Sortuj po scoringu
            group_sorted = sorted(group, key=lambda x: x.get("_score", {}).get("total_score", 0), reverse=True)
            
            master = group_sorted[0]
            slaves = group_sorted[1:]
            
            # Sprawdź czy to "prosty" przypadek (wszystkie slave < 5 scoring)
            all_slaves_low_score = all(s.get("_score", {}).get("total_score", 999) < 5 for s in slaves)
            
            for slave in slaves:
                duplicates_list.append({
                    "key": key,
                    "master": master,
                    "slave": slave,
                    "auto_merge_safe": all_slaves_low_score
                })
        
        merge_data["duplicates"] = duplicates_list
        merge_data["current_index"] = 0
        merge_data["results"] = []
        
        # Statystyki
        simple_count = sum(1 for d in duplicates_list if d["auto_merge_safe"])
        
        logger.info("=" * 80)
        logger.info("INICJALIZACJA ZAKOŃCZONA!")
        logger.info("=" * 80)
        logger.info("Pobrano firm: %d", len(accounts))
        logger.info("Znaleziono par duplikatów: %d", len(duplicates_list))
        logger.info("  - Proste (score < 5): %d", simple_count)
        logger.info("  - Złożone: %d", len(duplicates_list) - simple_count)
        logger.info("=" * 80)
        logger.info("WSZYSTKIE PARY SĄ JUŻ GOTOWE - GUI może iterować!")
        logger.info("=" * 80)
        
        return jsonify({
            "success": True,
            "total_pairs": len(duplicates_list),
            "simple_count": simple_count,
            "complex_count": len(duplicates_list) - simple_count,
            "message": f"Znaleziono {len(duplicates_list)} par do scalenia"
        })
    
    except Exception as exc:
        logger.error("Błąd inicjalizacji: %s", exc, exc_info=True)
        return jsonify({"success": False, "error": str(exc)}), 500


def _fetch_until_duplicates_smart_with_progress(api_client, merger, target_pairs: int) -> List[Dict]:
    """Pobiera firmy dopóki nie znajdzie wystarczająco duplikatów (z progress tracking)."""
    all_accounts = []
    page_num = 1
    page_token = None
    per_page = 200
    
    fields = [
        "id", "Account_Name", "Adres_w_rekordzie", "Nazwa_zwyczajowa", "Nazwa_handlowa_szyld",
        "Firma_NIP", "Firma_REGON", "Firma_KRS", "Status_REGON", "Domena_z_www", "Website",
        "Billing_Street", "Billing_Code", "Billing_City", "Billing_State",
        "Shipping_Street", "Shipping_Code", "Shipping_City", "Shipping_State",
        "Mobile_phone_1", "Phone", "Firma_EMAIL1",
        "Parent_Account", "Tag"
    ]
    fields_param = ",".join(fields)
    
    while True:
        if page_token:
            url = f"{api_client.BASE_URL}/Accounts?fields={fields_param}&per_page={per_page}&page_token={page_token}"
        else:
            url = f"{api_client.BASE_URL}/Accounts?fields={fields_param}&per_page={per_page}&page={page_num}"
        
        raw = api_client._get(url)
        records = raw.get("data", [])
        
        if not records:
            break
        
        all_accounts.extend(records)
        merge_data["all_accounts"] = all_accounts  # Update dla progress endpoint
        
        # Sprawdź duplikaty
        duplicates = merger.find_duplicates(all_accounts)
        pairs_count = sum(len(group) - 1 for group in duplicates.values())
        
        logger.info("Pobrano %d firm, znaleziono %d par duplikatów", len(all_accounts), pairs_count)
        
        if pairs_count >= target_pairs:
            break
        
        info = raw.get("info", {})
        if not info.get("more_records", False):
            break
        
        page_token = info.get("next_page_token")
        if not page_token:
            break
        
        page_num += 1
    
    return all_accounts


def _fetch_all_with_progress(api_client, limit: int) -> List[Dict]:
    """Pobiera wszystkie firmy z progress tracking."""
    all_accounts = api_client.get_all_accounts()
    merge_data["all_accounts"] = all_accounts  # Update dla progress
    
    if limit > 0:
        all_accounts = all_accounts[:limit]
    
    return all_accounts


@app.route("/api/get_merge_fields", methods=["POST"])
def get_merge_fields():
    """Zwraca pola do wyświetlenia dla bieżącej pary."""
    try:
        data = request.json
        master = data.get("master")
        slave = data.get("slave")
        
        # Lista kluczowych pól do pokazania
        important_fields = [
            ("Account_Name", "Nazwa firmy"),
            ("Nazwa_zwyczajowa", "Nazwa zwyczajowa"),
            ("Nazwa_handlowa_szyld", "Nazwa handlowa"),
            ("Firma_NIP", "NIP"),
            ("Website", "Strona www"),
            ("Phone", "Telefon"),
            ("Firma_EMAIL1", "Email"),
            ("Billing_Street", "Ulica (siedziba)"),
            ("Billing_City", "Miasto (siedziba)"),
        ]
        
        fields_data = []
        for field_name, field_label in important_fields:
            master_value = master.get(field_name, "")
            slave_value = slave.get(field_name, "")
            
            # Określ domyślny wybór
            if master_value and not slave_value:
                default_source = "master"
            elif slave_value and not master_value:
                default_source = "slave"
            elif master_value and slave_value and master_value != slave_value:
                default_source = "master"  # Domyślnie master wygrywa
            else:
                default_source = "master"
            
            fields_data.append({
                "name": field_name,
                "label": field_label,
                "master_value": master_value or "",
                "slave_value": slave_value or "",
                "default_source": default_source,
                "has_conflict": bool(master_value and slave_value and master_value != slave_value)
            })
        
        return jsonify({"success": True, "fields": fields_data})
    
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500


@app.route("/api/get_current_pair")
def get_current_pair():
    """Zwraca bieżącą parę do scalenia."""
    try:
        idx = merge_data["current_index"]
        duplicates = merge_data["duplicates"]
        
        logger.info("get_current_pair: idx=%d, total=%d", idx, len(duplicates))
        
        if not duplicates:
            return jsonify({"finished": True, "message": "Brak duplikatów"})
        
        if idx >= len(duplicates):
            return jsonify({"finished": True, "message": "Wszystkie pary przetworzone"})
        
        current = duplicates[idx]
        
        # Serializuj dane (usuń _score jeśli za duże)
        master_clean = {k: v for k, v in current["master"].items() if not k.startswith("_")}
        slave_clean = {k: v for k, v in current["slave"].items() if not k.startswith("_")}
        
        # Dodaj scoring jako osobne pole
        master_clean["_score"] = current["master"].get("_score", {})
        slave_clean["_score"] = current["slave"].get("_score", {})
        
        return jsonify({
            "finished": False,
            "index": idx + 1,
            "total": len(duplicates),
            "key": current["key"],
            "master": master_clean,
            "slave": slave_clean,
            "auto_merge_safe": current.get("auto_merge_safe", False)
        })
    
    except Exception as exc:
        logger.error("Błąd w get_current_pair: %s", exc, exc_info=True)
        return jsonify({"success": False, "error": str(exc)}), 500


@app.route("/api/merge_pair", methods=["POST"])
def merge_pair():
    """Scala parę firm z edytowanymi polami."""
    try:
        data = request.json
        pair_index = data.get("pair_index")
        edited_fields = data.get("edited_fields", {})
        
        if pair_index >= len(merge_data["duplicates"]):
            return jsonify({"success": False, "error": "Nieprawidłowy indeks"}), 400
        
        pair = merge_data["duplicates"][pair_index]
        master = pair["master"]
        slave = pair["slave"]
        master_id = master["id"]
        slave_id = slave["id"]
        
        logger.info("Scalanie: Master=%s, Slave=%s", master_id, slave_id)
        
        # 1. Zastosuj edycje pól (jeśli są)
        if edited_fields and not merge_data["dry_run"]:
            # Aktualizuj master z wybranymi polami
            update_data = {}
            for field_name, field_info in edited_fields.items():
                source = field_info.get("source")  # "master", "slave", lub "custom"
                custom_value = field_info.get("custom_value")
                
                if source == "slave":
                    update_data[field_name] = slave.get(field_name)
                elif source == "custom" and custom_value is not None:
                    update_data[field_name] = custom_value
            
            if update_data:
                logger.info("Aktualizacja pól master przed scaleniem: %s", list(update_data.keys()))
                merge_data["api_client"].update_account(master_id, update_data)
        
        # 2. Standardowe scalanie
        merger = merge_data["merger"]
        result = merger.merge_accounts(master_id, slave_id, merge_data["dry_run"])
        
        # Zapisz wynik
        merge_data["results"].append({
            "master_id": master_id,
            "master_name": master.get("Account_Name"),
            "slave_id": slave_id,
            "slave_name": slave.get("Account_Name"),
            "result": result,
            "edited_fields": list(edited_fields.keys()) if edited_fields else []
        })
        
        # NIE zwiększamy current_index tutaj - frontend to zrobi!
        
        return jsonify({
            "success": True,
            "message": "Scalono pomyślnie",
            "result": result,
            "next_index": pair_index + 1  # Podpowiedź dla frontend
        })
    
    except Exception as exc:
        logger.error("Błąd scalania: %s", exc, exc_info=True)
        return jsonify({"success": False, "error": str(exc)}), 500


@app.route("/api/skip_pair", methods=["POST"])
def skip_pair():
    """Pomija bieżącą parę."""
    data = request.json or {}
    new_index = data.get("new_index")
    
    if new_index is not None:
        merge_data["current_index"] = new_index
        logger.info("Skip: ustawiono index na %d", new_index)
    else:
        merge_data["current_index"] += 1
        logger.info("Skip: zwiększono index do %d", merge_data["current_index"])
    
    return jsonify({"success": True, "current_index": merge_data["current_index"]})


@app.route("/api/approve_all_simple", methods=["POST"])
def approve_all_simple():
    """Zatwierdza wszystkie proste scalenia (slave score < 5)."""
    try:
        count = 0
        for dup in merge_data["duplicates"]:
            if dup["auto_merge_safe"]:
                # Auto-scalanie prostych przypadków
                # TODO: Implementacja
                count += 1
        
        return jsonify({"success": True, "merged": count})
    
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500


def open_browser():
    """Otwiera przeglądarkę po 1 sekundzie (daje czas na start serwera)."""
    import time
    time.sleep(1.5)
    webbrowser.open("http://localhost:5000")


def start_gui(config_path: str = "Referencyjne/config.json"):
    """Uruchamia GUI server."""
    print("\n")
    print("=" * 80)
    print("🌐 ZOHO DATA CLEANUP - WEB GUI")
    print("=" * 80)
    print("")
    print("Interfejs graficzny zostanie otwarty w przeglądarce...")
    print("")
    print("URL: http://localhost:5000")
    print("")
    print("Aby zatrzymać serwer: Ctrl+C")
    print("=" * 80)
    print("")
    
    # Otwórz przeglądarkę w osobnym wątku
    threading.Thread(target=open_browser, daemon=True).start()
    
    # Uruchom serwer
    app.run(debug=False, port=5000, use_reloader=False)


if __name__ == "__main__":
    start_gui()

