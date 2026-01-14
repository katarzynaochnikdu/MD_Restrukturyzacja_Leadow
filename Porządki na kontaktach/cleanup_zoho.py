"""Główny skrypt czyszczenia danych w Zoho CRM."""

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

# Import modułów
from modules.token_manager import TokenManager
from modules.zoho_api_client import ZohoAPIClient
from modules.account_scorer import AccountScorer
from modules.account_merger import AccountMerger
from modules.contact_cleaner import ContactCleaner
from modules.company_assigner import CompanyAssigner
from modules.data_sanitizer import DataSanitizer
from modules.phone_formatter import PhoneFormatter


class ZohoDataCleanup:
    """Główna klasa zarządzająca procesem czyszczenia danych."""
    
    def __init__(self, mode: str, dry_run: bool, limit: int, config_path: str = "Referencyjne/config.json"):
        self.mode = mode
        self.dry_run = dry_run
        self.limit = limit
        self.config_path = config_path
        
        # Utworzenie folderu roboczego
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_dir = Path(f"run_{timestamp}")
        self.run_dir.mkdir(exist_ok=True)
        
        # Konfiguracja loggera
        self.setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # Inicjalizacja komponentów
        self.token_manager = TokenManager(config_path)
        self.api_client: ZohoAPIClient = None
        
        self.logger.info("=" * 80)
        self.logger.info("ZOHO DATA CLEANUP - START")
        self.logger.info("=" * 80)
        self.logger.info("Tryb: %s", mode)
        self.logger.info("Dry-run: %s", dry_run)
        self.logger.info("Limit rekordów: %s", limit if limit > 0 else "wszystkie")
        self.logger.info("Folder roboczy: %s", self.run_dir.absolute())
        self.logger.info("=" * 80)
    
    def setup_logging(self) -> None:
        """Konfiguruje szczegółowe logowanie."""
        log_file = self.run_dir / "cleanup.log"
        
        # Format logu
        log_format = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
        date_format = "%Y-%m-%d %H:%M:%S"
        
        # Handler do pliku (szczegółowy)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(log_format, date_format))
        
        # Handler do konsoli (ważne info)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter(log_format, date_format))
        
        # Konfiguracja root loggera
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)
    
    def initialize_api(self) -> None:
        """Inicjalizuje połączenie z API Zoho."""
        self.logger.info("Inicjalizacja połączenia z API Zoho...")
        
        try:
            access_token = self.token_manager.get_access_token()
            self.api_client = ZohoAPIClient(access_token)
            
            # Test połączenia
            if self.api_client.test_connection():
                self.logger.info("✓ Połączenie z API Zoho nawiązane pomyślnie")
            else:
                self.logger.warning("⚠ Test połączenia nie powiódł się, ale kontynuuję...")
        
        except Exception as exc:
            self.logger.error("✗ Błąd inicjalizacji API: %s", exc)
            raise
    
    def backup_data(self, module_name: str, data: List[Dict[str, Any]]) -> None:
        """Zapisuje backup danych do pliku JSON."""
        backup_file = self.run_dir / f"backup_{module_name.lower()}.json"
        
        self.logger.info("Tworzę backup %s (%d rekordów) -> %s", module_name, len(data), backup_file)
        
        try:
            with open(backup_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            self.logger.info("✓ Backup zapisany pomyślnie")
        
        except Exception as exc:
            self.logger.error("✗ Błąd zapisu backupu: %s", exc)
            raise
    
    def _fetch_until_duplicates_found(self, max_duplicate_groups: int) -> List[Dict[str, Any]]:
        """Pobiera firmy partiami do znalezienia duplikatów (dla testów).
        
        Args:
            max_duplicate_groups: Docelowa liczba GRUP duplikatów do znalezienia
        
        Returns:
            Lista firm zawierająca grupy duplikatów
        """
        from modules.account_merger import AccountMerger
        from modules.account_scorer import AccountScorer
        
        merger = AccountMerger(self.api_client, AccountScorer(self.api_client))
        
        all_accounts = []
        page_token = None
        page_num = 1
        per_page = 200
        found_duplicates_count = 0
        
        self.logger.info("   Szukam do znalezienia przynajmniej %d grup duplikatów...", max_duplicate_groups)
        self.logger.info("   (Będę pobierać firmy dopóki nie znajdę wystarczającej liczby)")
        
        # Lista pól do pobrania (musi być zgodna z zoho_api_client.get_all_accounts)
        fields = [
            "id", "Account_Name", "Adres_w_rekordzie", "Nazwa_zwyczajowa", "Nazwa_handlowa_szyld",
            "Firma_NIP", "Firma_REGON", "Firma_KRS", "Status_REGON", "Domena_z_www", "Website",
            "Billing_Street", "Billing_Code", "Billing_City", "Billing_Gmina", "Billing_Powiat", "Billing_State", "Billing_Country",
            "Shipping_Street", "Shipping_Code", "Shipping_City", "Shipping_Gmina", "Shipping_Powiat", "Shipping_State", "Shipping_Country",
            "Mobile_phone_1", "Mobile_phone_2", "Mobile_phone_3",
            "Phone", "Phone_2", "Phone_3",
            "Firma_EMAIL1", "Firma_EMAIL2", "Firma_EMAIL3",
            "Parent_Account", "GROUP_1", "GROUP_2", "GROUP_3", "Tag"
        ]
        fields_param = ",".join(fields)
        
        while True:
            self.logger.info("   Pobieranie partii %d (per_page=%d)...", page_num, per_page)
            
            # Pobierz kolejną partię
            if page_token:
                url = f"{self.api_client.BASE_URL}/Accounts?fields={fields_param}&per_page={per_page}&page_token={page_token}"
            else:
                url = f"{self.api_client.BASE_URL}/Accounts?fields={fields_param}&per_page={per_page}&page={page_num}"
            
            raw = self.api_client._get(url)
            records = raw.get("data", [])
            
            if not records:
                self.logger.info("   Koniec danych w Zoho (brak więcej firm)")
                break
            
            all_accounts.extend(records)
            self.logger.info("   → Pobrano %d firm (łącznie: %d)", len(records), len(all_accounts))
            
            # Sprawdź duplikaty w dotychczas pobranych firmach
            duplicates = merger.find_duplicates(all_accounts)
            found_duplicates_count = len(duplicates)
            
            if found_duplicates_count > 0:
                # Zlicz ile firm jest w duplikatach
                firms_in_duplicates = sum(len(group) for group in duplicates.values())
                self.logger.info("   ✓ Znaleziono %d grup duplikatów (%d firm w duplikatach)", 
                               found_duplicates_count, firms_in_duplicates)
                
                # STOP jeśli znaleziono wystarczająco dużo GRUP duplikatów
                if found_duplicates_count >= max_duplicate_groups:
                    self.logger.info("   STOP: Znaleziono wystarczającą liczbę grup duplikatów (%d >= %d)", 
                                   found_duplicates_count, max_duplicate_groups)
                    break
                else:
                    self.logger.info("   Kontynuuję szukanie (mam %d grup, potrzebuję %d)...", 
                                   found_duplicates_count, max_duplicate_groups)
            
            # Sprawdź czy są kolejne strony
            info = raw.get("info", {})
            if not info.get("more_records", False):
                self.logger.info("   Koniec danych w Zoho")
                break
            
            # Pobierz page_token
            page_token = info.get("next_page_token")
            if not page_token:
                self.logger.info("   Brak page_token - kończę")
                break
            
            page_num += 1
        
        self.logger.info("   ✓ Końcowy wynik: %d firm, %d grup duplikatów", 
                        len(all_accounts), found_duplicates_count)
        
        return all_accounts
    
    def run_accounts_mode(self) -> None:
        """Tryb czyszczenia firm (Accounts) - ZOPTYMALIZOWANY dla dużych baz."""
        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info("TRYB: ACCOUNTS (czyszczenie firm)")
        self.logger.info("=" * 80)
        
        # 1. Pobierz firmy (zoptymalizowane dla limitu - SMART)
        if self.limit > 0 and self.limit <= 1000:
            # OPTYMALIZACJA: Dla małych limitów (testy) szukaj dopóki nie znajdziesz duplikatów
            self.logger.info("1. Pobieranie firm (do pierwszej grupy duplikatów, max %d)...", self.limit)
            self.logger.info("   ⚡ OPTYMALIZACJA: Przeszukiwanie do znalezienia duplikatów")
            accounts = self._fetch_until_duplicates_found(self.limit)
        else:
            # Dla pełnej bazy lub dużych limitów: pobierz wszystkie
            self.logger.info("1. Pobieranie wszystkich firm z Zoho...")
            self.logger.info("   ⚡ OPTYMALIZACJA: Pobieranie PEŁNYCH danych dla backupu")
            accounts = self.api_client.get_all_accounts()
            
            if self.limit > 0:
                accounts = accounts[:self.limit]
                self.logger.info("   Ograniczono do %d firm (limit)", self.limit)
        
        self.logger.info("   Pobrano %d firm", len(accounts))
        
        # 2. Backup
        self.backup_data("Accounts", accounts)
        
        # 3. Inicjalizacja modułów
        scorer = AccountScorer(self.api_client)
        merger = AccountMerger(self.api_client, scorer)
        
        # 4. SZYBKA identyfikacja duplikatów (bez scoringu!)
        self.logger.info("")
        self.logger.info("2. Identyfikacja duplikatów (bez scoringu - SZYBKO)...")
        duplicates = merger.find_duplicates(accounts)
        self.logger.info("   Znaleziono %d grup duplikatów", len(duplicates))
        
        # Zlicz firmy w duplikatach
        total_duplicates = sum(len(group) for group in duplicates.values())
        self.logger.info("   Łącznie %d firm do przetworzenia (zamiast %d)", total_duplicates, len(accounts))
        self.logger.info("   ⚡ OSZCZĘDNOŚĆ: Pominięto %d firm bez duplikatów", len(accounts) - total_duplicates)
        
        # 5. Oblicz scoring TYLKO dla firm w duplikatach
        self.logger.info("")
        self.logger.info("3. Obliczanie scoringu TYLKO dla duplikatów (%d firm)...", total_duplicates)
        
        # Zbierz wszystkie ID firm w duplikatach
        accounts_to_score = set()
        for group in duplicates.values():
            for account in group:
                accounts_to_score.add(account.get("id"))
        
        self.logger.info("   Liczę scoring dla %d unikalnych firm...", len(accounts_to_score))
        
        if len(accounts_to_score) > 0:
            # Szacowany czas: każdy scoring = ~10 API calls × 0.5s = ~5s
            estimated_seconds = len(accounts_to_score) * 5
            estimated_minutes = estimated_seconds / 60
            self.logger.info("   ⏱️  Szacowany czas: ~%.1f minut (%.0f sekund)", estimated_minutes, estimated_seconds)
        
        scores_cache = {}
        total_to_score = len(accounts_to_score)
        
        for i, account_id in enumerate(accounts_to_score, 1):
            # Progress co 10%
            if i == 1 or i % max(1, total_to_score // 10) == 0 or i == total_to_score:
                percent = (i / total_to_score) * 100
                self.logger.info("   [%d/%d - %.0f%%] Scoring dla firmy ID: %s", 
                               i, total_to_score, percent, account_id)
            
            try:
                score_result = scorer.calculate_full_score(account_id)
                scores_cache[account_id] = score_result
            except Exception as exc:
                self.logger.error("      ✗ Błąd obliczania scoringu: %s", exc)
                scores_cache[account_id] = None
        
        # Dodaj scoring do firm
        for account in accounts:
            account_id = account.get("id")
            if account_id in scores_cache:
                account["_score"] = scores_cache[account_id]
        
        # 6. Scalanie duplikatów
        self.logger.info("")
        self.logger.info("4. Scalanie duplikatów...")
        
        merge_results = []
        total_groups = len(duplicates)
        user_quit = False
        
        for group_num, (key, group) in enumerate(duplicates.items(), 1):
            if user_quit:
                break
            self.logger.info("   [Grupa %d/%d] %s (%d firm)", group_num, total_groups, key, len(group))
            
            # Sortuj po scoringu (malejąco)
            group_sorted = sorted(group, key=lambda x: x.get("_score", {}).get("total_score", 0), reverse=True)
            
            master = group_sorted[0]
            slaves = group_sorted[1:]
            
            master_id = master.get("id")
            master_name = master.get("Account_Name")
            master_score = master.get("_score", {}).get("total_score", 0)
            
            self.logger.info("      Master: %s (score: %d)", master_name, master_score)
            
            for slave in slaves:
                slave_id = slave.get("id")
                slave_name = slave.get("Account_Name")
                slave_score = slave.get("_score", {}).get("total_score", 0)
                
                self.logger.info("      Slave: %s (score: %d)", slave_name, slave_score)
                
                # INTERAKTYWNE POTWIERDZENIE w trybie --apply
                if not self.dry_run:
                    print("")
                    print("=" * 80)
                    print(f"⚠️  SCALANIE FIRM (GRUPA {group_num}/{total_groups})")
                    print("=" * 80)
                    print(f"Master (zachowaj): {master_name} (ID: {master_id}, score: {master_score})")
                    print(f"Slave (scal i usuń): {slave_name} (ID: {slave_id}, score: {slave_score})")
                    print("=" * 80)
                    print("Operacje które zostaną wykonane:")
                    print("  1. Skopiowanie pustych pól ze Slave do Master")
                    print("  2. Przeniesienie powiązań (Contacts, Leads, Deals, etc.)")
                    print("  3. Przeniesienie tagów")
                    print("  4. Usunięcie Slave (jeśli scoring < 5 i brak powiązań)")
                    print("=" * 80)
                    
                    response = input("Scalić te firmy? [T/n/p(omiń)/q(quit)]: ").strip().lower()
                    
                    if response in ["q", "quit", "exit"]:
                        self.logger.info("      ❌ Użytkownik przerwał scalanie (quit)")
                        print("Przerywam proces scalania...")
                        user_quit = True
                        break
                    elif response in ["n", "nie", "no"]:
                        self.logger.info("      ⏭️  Pominięto scalenie (użytkownik odrzucił)")
                        print("Pominięto to scalenie.")
                        continue
                    elif response in ["p", "pomij", "skip"]:
                        self.logger.info("      ⏭️  Pominięto scalenie (skip)")
                        print("Pominięto to scalenie.")
                        continue
                    elif not response or response in ["t", "tak", "y", "yes"]:
                        self.logger.info("      ✅ Użytkownik potwierdził scalenie")
                        print("Scalanie...")
                    else:
                        self.logger.info("      ⏭️  Nierozpoznana odpowiedź - pomijam")
                        print("Nierozpoznana odpowiedź - pomijam.")
                        continue
                
                # Scalaj
                result = merger.merge_accounts(master_id, slave_id, self.dry_run)
                
                merge_results.append({
                    "master_id": master_id,
                    "master_name": master_name,
                    "master_score": master_score,
                    "slave_id": slave_id,
                    "slave_name": slave_name,
                    "slave_score": slave_score,
                    "merged_fields": len(result.get("merged_fields", [])),
                    "transferred_relations": result.get("transferred_relations", {}),
                    "tags_transferred": result.get("tags_transferred", 0),
                    "deleted": result.get("deleted", False),
                    "success": result.get("success", False),
                    "error": result.get("error")
                })
        
        # 8. Raport
        self.save_accounts_report(merge_results)
        
        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info("TRYB ACCOUNTS ZAKOŃCZONY")
        self.logger.info("=" * 80)
        self.logger.info("Pobrano firm: %d", len(accounts))
        self.logger.info("Znaleziono grup duplikatów: %d", total_groups)
        self.logger.info("Przetworzone firmy (scoring): %d (zamiast %d)", total_to_score, len(accounts))
        self.logger.info("⚡ OSZCZĘDNOŚĆ CZASU: ~%.0f%% (pominięto %d firm)", 
                        ((len(accounts) - total_to_score) / len(accounts) * 100) if len(accounts) > 0 else 0,
                        len(accounts) - total_to_score)
        self.logger.info("Scalono par firm: %d", len(merge_results))
        successful_merges = len([r for r in merge_results if r["success"]])
        deleted_accounts = len([r for r in merge_results if r["deleted"]])
        self.logger.info("Pomyślne scalenia: %d", successful_merges)
        self.logger.info("Usunięte firmy: %d", deleted_accounts)
        self.logger.info("=" * 80)
    
    def run_contacts_mode(self) -> None:
        """Tryb czyszczenia kontaktów (Contacts) - ZOPTYMALIZOWANY dla dużych baz."""
        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info("TRYB: CONTACTS (czyszczenie kontaktów)")
        self.logger.info("=" * 80)
        
        # 1. Pobierz wszystkie kontakty
        self.logger.info("1. Pobieranie wszystkich kontaktów z Zoho...")
        self.logger.info("   ⚡ OPTYMALIZACJA: Pobieranie PEŁNYCH danych dla backupu")
        contacts = self.api_client.get_all_contacts()
        
        if self.limit > 0:
            contacts = contacts[:self.limit]
            self.logger.info("   Ograniczono do %d kontaktów (limit)", self.limit)
        
        self.logger.info("   Pobrano %d kontaktów", len(contacts))
        self.logger.info("   ⏱️  Szacowany czas przetwarzania: ~%.1f minut (0.5s × %d = %d sek)", 
                        (len(contacts) * 0.5) / 60, len(contacts), len(contacts) * 0.5)
        
        # 2. Backup
        self.backup_data("Contacts", contacts)
        
        # 3. Inicjalizacja modułów
        sanitizer = DataSanitizer()
        phone_formatter = PhoneFormatter()
        cleaner = ContactCleaner(sanitizer, phone_formatter)
        assigner = CompanyAssigner(self.api_client)
        
        # 4. Czyszczenie kontaktów
        self.logger.info("")
        self.logger.info("2. Czyszczenie kontaktów...")
        
        clean_results = []
        assign_results = []
        manual_review = []
        
        total_contacts = len(contacts)
        
        for i, contact in enumerate(contacts, 1):
            contact_id = contact.get("id")
            first_name = contact.get("First_Name", "")
            last_name = contact.get("Last_Name", "")
            full_name = f"{first_name} {last_name}".strip()
            
            # Progress co 10%
            if i == 1 or i % max(1, total_contacts // 10) == 0 or i == total_contacts:
                percent = (i / total_contacts) * 100
                self.logger.info("   [%d/%d - %.0f%%] %s...", i, total_contacts, percent, full_name or contact_id)
            
            try:
                # A. Czyszczenie (emaile + telefony)
                clean_result = cleaner.clean_contact(contact)
                clean_results.append(clean_result)
                
                # B. Przypisanie firmy
                assign_result = assigner.assign_or_verify_company(contact)
                assign_results.append(assign_result)
                
                # C. Jeśli wieloznaczność - dodaj do manual review
                if assign_result["action"] == "multiple_matches":
                    manual_review.append(assign_result)
                
                # D. Aktualizuj kontakt (jeśli były zmiany)
                all_changes = {}
                
                # Zmiany z czyszczenia
                if clean_result["has_changes"]:
                    all_changes.update(clean_result["all_changes"])
                
                # Zmiany z przypisania firmy
                if assign_result["action"] in ["assigned", "removed"]:
                    if assign_result["action"] == "assigned":
                        all_changes["Account_Name"] = assign_result["company_id"]
                    else:  # removed
                        all_changes["Account_Name"] = None
                
                if all_changes and not self.dry_run:
                    success = self.api_client.update_contact(contact_id, all_changes)
                    if success:
                        self.logger.info("      ✓ Zaktualizowano kontakt")
                    else:
                        self.logger.error("      ✗ Błąd aktualizacji kontaktu")
            
            except Exception as exc:
                self.logger.error("      ✗ Błąd przetwarzania kontaktu: %s", exc, exc_info=True)
        
        # 5. Raporty
        self.save_contacts_report(clean_results, assign_results)
        self.save_manual_review_report(manual_review)
        
        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info("TRYB CONTACTS ZAKOŃCZONY")
        self.logger.info("=" * 80)
        self.logger.info("Pobrano kontaktów: %d", total_contacts)
        contacts_with_changes = len([r for r in clean_results if r["has_changes"]])
        self.logger.info("Wyczyszczono kontaktów: %d", contacts_with_changes)
        self.logger.info("Usuniętych duplikatów emaili: %d", sum(r["email_dups_removed"] for r in clean_results))
        self.logger.info("Usuniętych duplikatów tel. komórkowych: %d", sum(r["mobile_dups_removed"] for r in clean_results))
        self.logger.info("Usuniętych duplikatów tel. stacjonarnych: %d", sum(r["landline_dups_removed"] for r in clean_results))
        self.logger.info("Przypisano firm: %d", len([r for r in assign_results if r["action"] == "assigned"]))
        self.logger.info("Zweryfikowano firm: %d", len([r for r in assign_results if r["action"] == "verified"]))
        self.logger.info("Wymaga ręcznej weryfikacji: %d", len(manual_review))
        self.logger.info("=" * 80)
    
    def save_accounts_report(self, results: List[Dict[str, Any]]) -> None:
        """Zapisuje raport scalenia firm."""
        report_file = self.run_dir / "accounts_merged.csv"
        
        self.logger.info("Zapisuję raport do: %s", report_file)
        
        try:
            with open(report_file, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=[
                    "Master_ID", "Master_Name", "Master_Score",
                    "Slave_ID", "Slave_Name", "Slave_Score",
                    "Merged_Fields", "Transferred_Relations", "Tags_Transferred", "Deleted", "Success", "Error"
                ])
                writer.writeheader()
                
                for result in results:
                    writer.writerow({
                        "Master_ID": result["master_id"],
                        "Master_Name": result["master_name"],
                        "Master_Score": result["master_score"],
                        "Slave_ID": result["slave_id"],
                        "Slave_Name": result["slave_name"],
                        "Slave_Score": result["slave_score"],
                        "Merged_Fields": result["merged_fields"],
                        "Transferred_Relations": json.dumps(result["transferred_relations"]),
                        "Tags_Transferred": result.get("tags_transferred", 0),
                        "Deleted": result["deleted"],
                        "Success": result["success"],
                        "Error": result.get("error", "")
                    })
            
            self.logger.info("✓ Raport zapisany")
        
        except Exception as exc:
            self.logger.error("✗ Błąd zapisu raportu: %s", exc)
    
    def save_contacts_report(self, clean_results: List[Dict[str, Any]], assign_results: List[Dict[str, Any]]) -> None:
        """Zapisuje raport czyszczenia kontaktów."""
        report_file = self.run_dir / "contacts_cleaned.csv"
        
        self.logger.info("Zapisuję raport do: %s", report_file)
        
        try:
            with open(report_file, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=[
                    "Contact_ID", "Full_Name",
                    "Email_Dups_Removed", "Mobile_Dups_Removed", "Landline_Dups_Removed",
                    "Company_Assigned", "Company_Action", "Changes"
                ])
                writer.writeheader()
                
                for clean_result in clean_results:
                    contact_id = clean_result["contact_id"]
                    
                    # Znajdź odpowiedni assign_result
                    assign_result = next((r for r in assign_results if r["contact_id"] == contact_id), None)
                    
                    writer.writerow({
                        "Contact_ID": contact_id,
                        "Full_Name": clean_result["full_name"],
                        "Email_Dups_Removed": clean_result["email_dups_removed"],
                        "Mobile_Dups_Removed": clean_result["mobile_dups_removed"],
                        "Landline_Dups_Removed": clean_result["landline_dups_removed"],
                        "Company_Assigned": assign_result["company_name"] if assign_result else "",
                        "Company_Action": assign_result["action"] if assign_result else "",
                        "Changes": clean_result["summary"]
                    })
            
            self.logger.info("✓ Raport zapisany")
        
        except Exception as exc:
            self.logger.error("✗ Błąd zapisu raportu: %s", exc)
    
    def save_manual_review_report(self, manual_review: List[Dict[str, Any]]) -> None:
        """Zapisuje raport kontaktów wymagających ręcznej weryfikacji."""
        report_file = self.run_dir / "contacts_manual_review.csv"
        
        self.logger.info("Zapisuję raport ręcznej weryfikacji do: %s", report_file)
        
        try:
            with open(report_file, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=[
                    "Contact_ID", "Full_Name", "Email_Domain", "Matching_Companies"
                ])
                writer.writeheader()
                
                for item in manual_review:
                    companies_str = "; ".join([
                        f"{c['name']} (ID: {c['id']})"
                        for c in item["matching_companies"]
                    ])
                    
                    writer.writerow({
                        "Contact_ID": item["contact_id"],
                        "Full_Name": item["full_name"],
                        "Email_Domain": item["domain"],
                        "Matching_Companies": companies_str
                    })
            
            self.logger.info("✓ Raport zapisany")
        
        except Exception as exc:
            self.logger.error("✗ Błąd zapisu raportu: %s", exc)
    
    def run(self) -> None:
        """Główna metoda uruchamiająca proces czyszczenia."""
        try:
            # Inicjalizacja API
            self.initialize_api()
            
            # Wybór trybu
            if self.mode == "accounts":
                self.run_accounts_mode()
            elif self.mode == "contacts":
                self.run_contacts_mode()
            else:
                raise ValueError(f"Nieprawidłowy tryb: {self.mode}")
            
            self.logger.info("")
            self.logger.info("=" * 80)
            self.logger.info("ZOHO DATA CLEANUP - ZAKOŃCZONE POMYŚLNIE")
            self.logger.info("=" * 80)
        
        except Exception as exc:
            self.logger.error("")
            self.logger.error("=" * 80)
            self.logger.error("BŁĄD KRYTYCZNY: %s", exc, exc_info=True)
            self.logger.error("=" * 80)
            raise


def interactive_menu() -> Dict[str, Any]:
    """Interaktywne menu konfiguracji uruchomienia."""
    print("=" * 80)
    print("ZOHO DATA CLEANUP - KONFIGURACJA")
    print("=" * 80)
    print()
    
    # 1. Wybór trybu
    print("1. TRYB DZIAŁANIA:")
    print("   a) accounts - czyszczenie i scalanie firm")
    print("   b) contacts - czyszczenie kontaktów (emaile/telefony/firmy)")
    print()
    
    while True:
        mode_choice = input("Wybierz tryb [a/b]: ").strip().lower()
        if mode_choice in ["a", "accounts"]:
            mode = "accounts"
            break
        elif mode_choice in ["b", "contacts"]:
            mode = "contacts"
            break
        else:
            print("❌ Nieprawidłowy wybór. Wpisz 'a' lub 'b'.")
    
    print(f"✓ Wybrano: {mode}")
    print()
    
    # 2. Dry-run czy apply
    print("2. ZAPISYWANIE ZMIAN:")
    print("   a) dry-run - TYLKO ANALIZA (bez zmian w Zoho) [ZALECANE dla testów]")
    print("   b) apply - FAKTYCZNE ZMIANY w Zoho ⚠️")
    print()
    
    while True:
        apply_choice = input("Wybierz [a/b] (domyślnie: a): ").strip().lower()
        if not apply_choice or apply_choice in ["a", "dry-run"]:
            dry_run = True
            apply = False
            break
        elif apply_choice in ["b", "apply"]:
            dry_run = False
            apply = True
            break
        else:
            print("❌ Nieprawidłowy wybór. Wpisz 'a' lub 'b'.")
    
    print(f"✓ Wybrano: {'dry-run (bez zmian)' if dry_run else 'apply (z zapisem)'}")
    print()
    
    # 3. Limit rekordów
    print("3. LIMIT REKORDÓW:")
    print("   0 - wszystkie rekordy (cała baza)")
    print("   1 - ZALECANE dla pierwszego testu")
    print("   10 - ZALECANE dla drugiego testu")
    print("   N - dowolna liczba rekordów")
    print()
    
    while True:
        limit_input = input("Podaj limit (domyślnie: 1): ").strip()
        if not limit_input:
            limit = 1
            break
        try:
            limit = int(limit_input)
            if limit < 0:
                print("❌ Limit musi być >= 0")
                continue
            break
        except ValueError:
            print("❌ Podaj liczbę całkowitą")
    
    print(f"✓ Wybrano: {limit if limit > 0 else 'wszystkie rekordy'}")
    print()
    
    # 4. Podsumowanie i potwierdzenie
    print("=" * 80)
    print("PODSUMOWANIE KONFIGURACJI:")
    print("=" * 80)
    print(f"Tryb:          {mode}")
    print(f"Dry-run:       {'TAK (bez zmian)' if dry_run else 'NIE (z zapisem) ⚠️'}")
    print(f"Limit:         {limit if limit > 0 else 'wszystkie rekordy'}")
    print("=" * 80)
    print()
    
    if not dry_run:
        print("⚠️  UWAGA: Wybrano tryb APPLY - zmiany zostaną zapisane do Zoho CRM! ⚠️")
        print()
    
    return {
        "mode": mode,
        "dry_run": dry_run,
        "apply": apply,
        "limit": limit
    }


def choose_mode() -> str:
    """Wybór trybu pracy: CLI lub Web GUI."""
    print("")
    print("=" * 80)
    print("ZOHO DATA CLEANUP")
    print("=" * 80)
    print("")
    print("Wybierz tryb pracy:")
    print("")
    print("  [1] Tryb terminalowy (CLI) - klasyczny, z potwierdzeniami w konsoli")
    print("  [2] Web GUI w przeglądarce - interaktywny interfejs graficzny")
    print("")
    print("=" * 80)
    
    while True:
        choice = input("Wybór (1/2, domyślnie: 1): ").strip()
        
        if not choice or choice == "1":
            return "cli"
        elif choice == "2":
            return "gui"
        else:
            print("❌ Nieprawidłowy wybór. Wpisz 1 lub 2.")


def main():
    """Punkt wejścia programu."""
    parser = argparse.ArgumentParser(
        description="Zoho Data Cleanup - czyszczenie i deduplikacja danych w Zoho CRM",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Przykłady użycia:
  python cleanup_zoho.py                                    # Interaktywne menu
  python cleanup_zoho.py --interactive                      # Interaktywne menu
  python cleanup_zoho.py --mode accounts --dry-run --limit 1
  python cleanup_zoho.py --mode contacts --apply --limit 100
  python cleanup_zoho.py --mode accounts --apply
        """
    )
    
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Uruchom interaktywne menu konfiguracji"
    )
    
    parser.add_argument(
        "--gui",
        action="store_true",
        help="Uruchom Web GUI w przeglądarce"
    )
    
    parser.add_argument(
        "--mode",
        choices=["accounts", "contacts"],
        help="Tryb działania: accounts (czyszczenie firm) lub contacts (czyszczenie kontaktów)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Tryb symulacji (bez zapisów do Zoho)"
    )
    
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Zastosuj zmiany (faktyczna aktualizacja w Zoho)"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit rekordów do przetworzenia (0 = wszystkie)"
    )
    
    parser.add_argument(
        "--config",
        default="Referencyjne/config.json",
        help="Ścieżka do pliku config.json (domyślnie: Referencyjne/config.json)"
    )
    
    args = parser.parse_args()
    
    # Jeśli użyto --gui, uruchom Web GUI
    if args.gui:
        print("\n🌐 Uruchamianie Web GUI...")
        try:
            import gui_server
            gui_server.start_gui(config_path=args.config)
        except ImportError:
            print("\n❌ BŁĄD: Brak modułu Flask!")
            print("Zainstaluj: pip install flask")
            sys.exit(1)
        sys.exit(0)
    
    # Wybór trybu jeśli nie podano argumentów
    if not args.mode and not args.interactive:
        mode_choice = choose_mode()
        if mode_choice == "gui":
            try:
                import gui_server
                gui_server.start_gui(config_path=args.config)
            except ImportError:
                print("\n❌ BŁĄD: Brak modułu Flask!")
                print("Zainstaluj: pip install flask")
                sys.exit(1)
            sys.exit(0)
        # Jeśli CLI, kontynuuj do menu interaktywnego
        args.interactive = True
    
    # Jeśli nie podano --mode lub użyto --interactive, uruchom menu
    if args.interactive or not args.mode:
        config = interactive_menu()
        args.mode = config["mode"]
        args.dry_run = config["dry_run"]
        args.apply = config["apply"]
        args.limit = config["limit"]
    
    # Walidacja argumentów
    if args.dry_run and args.apply:
        print("BŁĄD: Nie można użyć jednocześnie --dry-run i --apply")
        sys.exit(1)
    
    if not args.dry_run and not args.apply:
        # Domyślnie dry-run
        args.dry_run = True
    
    # Potwierdzenie od użytkownika
    print("=" * 80)
    print("ZOHO DATA CLEANUP")
    print("=" * 80)
    print(f"Tryb: {args.mode}")
    print(f"Dry-run: {'TAK' if args.dry_run else 'NIE'}")
    print(f"Limit rekordów: {args.limit if args.limit > 0 else 'wszystkie'}")
    print("=" * 80)
    
    if not args.dry_run:
        print("\n⚠ UWAGA: Zmiany zostaną zapisane do Zoho CRM! ⚠\n")
    
    response = input("Kontynuować? [T/n]: ").strip().lower()
    if response and response not in ["t", "tak", "y", "yes"]:
        print("Anulowano.")
        sys.exit(0)
    
    # Uruchom czyszczenie
    cleanup = ZohoDataCleanup(
        mode=args.mode,
        dry_run=args.dry_run,
        limit=args.limit,
        config_path=args.config
    )
    
    cleanup.run()


if __name__ == "__main__":
    main()

