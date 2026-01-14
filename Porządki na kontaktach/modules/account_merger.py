"""Scalanie duplikatów firm (Accounts)."""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from .zoho_api_client import ZohoAPIClient
    from .account_scorer import AccountScorer


class AccountMerger:
    """Identyfikuje i scala duplikaty firm."""
    
    def __init__(self, api_client: ZohoAPIClient, scorer: AccountScorer) -> None:
        self.api_client = api_client
        self.scorer = scorer
        self.logger = logging.getLogger(__name__)
    
    def normalize_name(self, name: Optional[str]) -> str:
        """Normalizuje nazwę firmy (trim, lowercase, single spaces)."""
        if not name:
            return ""
        
        # Trim, lowercase
        normalized = str(name).strip().lower()
        
        # Single spaces (usuń wielokrotne spacje)
        normalized = re.sub(r'\s+', ' ', normalized)
        
        return normalized
    
    def find_duplicates(self, accounts: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Identyfikuje duplikaty firm.
        
        Grupuje firmy po:
        - Account_Name (znormalizowana nazwa)
        - Firma_NIP (jeśli wypełnione)
        
        WYKLUCZA pary rodzic/dziecko.
        
        Returns:
            Dict mapping klucza (normalized_name lub NIP) do listy duplikatów
        """
        self.logger.info("🔍 Rozpoczynam identyfikację duplikatów w %d firmach...", len(accounts))
        
        # Grupuj po nazwie
        by_name: Dict[str, List[Dict[str, Any]]] = {}
        empty_names_count = 0
        
        for account in accounts:
            account_id = account.get("id")
            account_name = account.get("Account_Name", "")
            
            if not account_name or str(account_name).strip() == "":
                empty_names_count += 1
                self.logger.debug("  Firma bez nazwy: ID=%s", account_id)
                continue
            
            normalized_name = self.normalize_name(account_name)
            
            if not normalized_name:
                empty_names_count += 1
                self.logger.debug("  Firma z pustą nazwą po normalizacji: ID=%s, nazwa='%s'", account_id, account_name)
                continue
            
            if normalized_name not in by_name:
                by_name[normalized_name] = []
            
            by_name[normalized_name].append(account)
        
        if empty_names_count > 0:
            self.logger.warning("  ⚠ Pominięto %d firm bez nazwy", empty_names_count)
        
        self.logger.info("  Zgrupowano %d unikalnych nazw", len(by_name))
        
        # Grupuj po NIP
        by_nip: Dict[str, List[Dict[str, Any]]] = {}
        
        for account in accounts:
            nip = account.get("Firma_NIP", "")
            if nip and str(nip).strip():
                nip_clean = re.sub(r'[^0-9]', '', str(nip))
                
                if nip_clean:
                    if nip_clean not in by_nip:
                        by_nip[nip_clean] = []
                    
                    by_nip[nip_clean].append(account)
        
        # Filtruj duplikaty (tylko grupy z więcej niż 1 firmą)
        duplicates: Dict[str, List[Dict[str, Any]]] = {}
        
        # DEBUG: Sprawdź ile jest potencjalnych duplikatów przed filtrowaniem
        potential_by_name = sum(1 for group in by_name.values() if len(group) > 1)
        self.logger.info("  Potencjalne duplikaty po nazwie (przed filtrem parent/child): %d grup", potential_by_name)
        
        # Duplikaty po nazwie
        for name, group in by_name.items():
            if len(group) > 1:
                self.logger.debug("  Grupa '%s': %d firm (przed filtrem)", name[:50], len(group))
                # Wyklucz pary rodzic/dziecko
                filtered_group = self._filter_parent_child_pairs(group)
                self.logger.debug("  Grupa '%s': %d firm (PO filtrze parent/child)", name[:50], len(filtered_group))
                if len(filtered_group) > 1:
                    duplicates[f"name:{name}"] = filtered_group
                    self.logger.info("  ✓ Znaleziono %d duplikatów dla nazwy: %s", len(filtered_group), name[:100])
        
        # DEBUG: Sprawdź ile jest potencjalnych duplikatów po NIP
        potential_by_nip = sum(1 for group in by_nip.values() if len(group) > 1)
        self.logger.info("  Potencjalne duplikaty po NIP (przed filtrem parent/child): %d grup", potential_by_nip)
        
        # Duplikaty po NIP
        for nip, group in by_nip.items():
            if len(group) > 1:
                self.logger.debug("  Grupa NIP '%s': %d firm (przed filtrem)", nip, len(group))
                filtered_group = self._filter_parent_child_pairs(group)
                self.logger.debug("  Grupa NIP '%s': %d firm (PO filtrze parent/child)", nip, len(filtered_group))
                if len(filtered_group) > 1:
                    duplicates[f"nip:{nip}"] = filtered_group
                    self.logger.info("  ✓ Znaleziono %d duplikatów dla NIP: %s", len(filtered_group), nip)
        
        self.logger.info("✓ Znaleziono %d grup duplikatów", len(duplicates))
        
        return duplicates
    
    def _filter_parent_child_pairs(self, accounts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Wyklucza firmy będące w relacji rodzic/dziecko LUB rodzeństwo (siblings)."""
        if len(accounts) <= 1:
            return accounts
        
        # Zbierz wszystkie ID
        account_ids = {acc.get("id") for acc in accounts}
        
        # Zbierz wszystkich rodziców (parent_id)
        parents_in_group = set()
        for account in accounts:
            parent_id = account.get("Parent_Account")
            if isinstance(parent_id, dict):
                parent_id = parent_id.get("id")
            if parent_id:
                parents_in_group.add(parent_id)
        
        # Filtruj firmy, które są rodzicem/dzieckiem/rodzeństwem innej firmy z grupy
        filtered = []
        
        for account in accounts:
            account_id = account.get("id")
            parent_id = account.get("Parent_Account")
            account_name = account.get("Account_Name", "UNKNOWN")
            
            # Jeśli Parent_Account jest dict, wyciągnij ID
            if isinstance(parent_id, dict):
                parent_id = parent_id.get("id")
            
            # WYKLUCZENIE 1: Sprawdź czy rodzic jest w tej samej grupie (dziecko)
            if parent_id and parent_id in account_ids:
                self.logger.debug("    Wykluczam firmę %s (jest dzieckiem firmy z grupy)", account_name)
                continue
            
            # WYKLUCZENIE 2: Sprawdź czy ta firma jest rodzicem innej firmy z grupy
            is_parent_in_group = False
            for other in accounts:
                if other.get("id") == account_id:
                    continue
                
                other_parent = other.get("Parent_Account")
                if isinstance(other_parent, dict):
                    other_parent = other_parent.get("id")
                
                if other_parent == account_id:
                    is_parent_in_group = True
                    break
            
            if is_parent_in_group:
                self.logger.debug("    Wykluczam firmę %s (jest rodzicem firmy z grupy)", account_name)
                continue
            
            # WYKLUCZENIE 3: Sprawdź czy firmy mają wspólnego rodzica (siblings)
            if parent_id and parent_id in parents_in_group:
                # Sprawdź czy są inne firmy w grupie z tym samym rodzicem
                siblings_count = 0
                for other in accounts:
                    if other.get("id") == account_id:
                        continue
                    
                    other_parent = other.get("Parent_Account")
                    if isinstance(other_parent, dict):
                        other_parent = other_parent.get("id")
                    
                    if other_parent == parent_id:
                        siblings_count += 1
                
                if siblings_count > 0:
                    self.logger.debug("    Wykluczam firmę %s (ma wspólnego rodzica z inną firmą - rodzeństwo/filie)", 
                                    account_name)
                    continue
            
            filtered.append(account)
        
        return filtered
    
    def merge_accounts(self, master_id: str, slave_id: str, dry_run: bool = True) -> Dict[str, Any]:
        """Scala dwie firmy: kopiuje dane ze slave do master i usuwa slave.
        
        Args:
            master_id: ID firmy docelowej (z wyższym scoringiem)
            slave_id: ID firmy źródłowej (do usunięcia)
            dry_run: Jeśli True, tylko symulacja (bez zapisów)
        
        Returns:
            {
                "success": bool,
                "merged_fields": List[str],
                "transferred_relations": Dict[str, int],
                "deleted": bool,
                "error": Optional[str]
            }
        """
        self.logger.info("=== START: Scalanie firm ===")
        self.logger.info("  Master ID: %s", master_id)
        self.logger.info("  Slave ID: %s", slave_id)
        self.logger.info("  Dry-run: %s", dry_run)
        
        result = {
            "success": False,
            "merged_fields": [],
            "transferred_relations": {},
            "deleted": False,
            "error": None
        }
        
        try:
            # Pobierz obie firmy
            master = self.api_client.get_account_by_id(master_id)
            slave = self.api_client.get_account_by_id(slave_id)
            
            if not master or not slave:
                error_msg = f"Nie znaleziono firmy: master={master is not None}, slave={slave is not None}"
                self.logger.error(error_msg)
                result["error"] = error_msg
                return result
            
            self.logger.info("  Master: %s", master.get("Account_Name"))
            self.logger.info("  Slave: %s", slave.get("Account_Name"))
            
            # 1. Scalanie pól (kopiuj puste pola z slave do master)
            merged_fields = self._merge_fields(master, slave)
            result["merged_fields"] = merged_fields
            
            if merged_fields and not dry_run:
                # Przygotuj update
                update_data = {}
                for field_name in merged_fields:
                    update_data[field_name] = slave.get(field_name)
                
                self.logger.info("  Aktualizuję master z %d polami...", len(update_data))
                success = self.api_client.update_account(master_id, update_data)
                
                if not success:
                    result["error"] = "Błąd aktualizacji master"
                    return result
            
            # 2. Przenoszenie powiązań ze slave do master
            transferred = self._transfer_relations(master_id, slave_id, dry_run)
            result["transferred_relations"] = transferred
            
            # 2b. Przenoszenie tagów ze slave do master
            tags_transferred = self._transfer_tags(master_id, slave_id, dry_run)
            result["tags_transferred"] = tags_transferred
            
            # 3. Sprawdź ponownie scoring slave po przeniesieniu
            slave_score_after = self.scorer.calculate_full_score(slave_id)
            total_score = slave_score_after["total_score"]
            
            self.logger.info("  Scoring slave po przeniesieniu: %d", total_score)
            
            # 4. Usuń slave jeśli scoring < 5 i brak powiązań
            if total_score < 5:
                # Sprawdź czy slave nie ma już powiązań
                has_relations = False
                
                # Sprawdź kontakty
                try:
                    contacts = self.api_client.search_records_by_criteria("Contacts", f"Account_Name:equals:{slave_id}")
                    if contacts:
                        has_relations = True
                        self.logger.info("  Slave ma jeszcze %d powiązanych kontaktów", len(contacts))
                except Exception:
                    pass
                
                # Sprawdź relacje parent/child z master
                has_parent_child = self._check_parent_child_relation(master, slave)
                
                if not has_relations and not has_parent_child:
                    self.logger.info("  Usuwam slave (scoring=%d < 5, brak powiązań)...", total_score)
                    
                    if not dry_run:
                        deleted = self.api_client.delete_account(slave_id)
                        result["deleted"] = deleted
                        
                        if not deleted:
                            result["error"] = "Błąd usuwania slave"
                            return result
                    else:
                        result["deleted"] = True  # Symulacja
                else:
                    self.logger.info("  Nie usuwam slave (ma powiązania lub relację parent/child)")
                    result["deleted"] = False
            else:
                self.logger.info("  Nie usuwam slave (scoring=%d >= 5)", total_score)
                result["deleted"] = False
            
            result["success"] = True
            self.logger.info("=== KONIEC: Scalanie firm (sukces) ===")
            
        except Exception as exc:
            self.logger.error("Błąd podczas scalania firm: %s", exc, exc_info=True)
            result["error"] = str(exc)
        
        return result
    
    def _merge_fields(self, master: Dict[str, Any], slave: Dict[str, Any]) -> List[str]:
        """Kopiuje puste pola z slave do master (z deduplikacją).
        
        Returns:
            Lista nazw pól do aktualizacji
        """
        self.logger.info("  Analiza pól do scalenia...")
        
        merged_fields = []
        
        # Wszystkie możliwe pola do scalenia
        all_fields = [
            "Account_Name", "Adres_w_rekordzie", "Nazwa_zwyczajowa", "Nazwa_handlowa_szyld",
            "Firma_NIP", "Firma_REGON", "Firma_KRS", "Status_REGON",
            "Billing_Street", "Billing_Code", "Billing_City", "Billing_Gmina", 
            "Billing_Powiat", "Billing_State", "Billing_Country",
            "Shipping_Street", "Shipping_Code", "Shipping_City", "Shipping_Gmina", 
            "Shipping_Powiat", "Shipping_State", "Shipping_Country",
            "Website", "Parent_Account",
            "GROUP_1", "GROUP_2", "GROUP_3"
        ]
        
        # Podstawowe pola
        for field in all_fields:
            master_value = master.get(field)
            slave_value = slave.get(field)
            
            # Jeśli master jest pusty a slave ma wartość
            if (not master_value or str(master_value).strip() == "") and slave_value:
                merged_fields.append(field)
                self.logger.debug("    ✓ Kopiuję pole %s: %s", field, str(slave_value)[:50])
        
        # Telefony (z deduplikacją)
        phone_fields = [
            "Mobile_phone_1", "Mobile_phone_2", "Mobile_phone_3",
            "Phone", "Phone_2", "Phone_3"
        ]
        
        for field in phone_fields:
            master_value = master.get(field)
            slave_value = slave.get(field)
            
            if slave_value and not self._is_duplicate_phone(slave_value, master, phone_fields):
                if not master_value or str(master_value).strip() == "":
                    merged_fields.append(field)
                    self.logger.debug("    ✓ Kopiuję telefon %s: %s", field, slave_value)
        
        # Emaile (z deduplikacją)
        email_fields = ["Firma_EMAIL1", "Firma_EMAIL2", "Firma_EMAIL3"]
        
        for field in email_fields:
            master_value = master.get(field)
            slave_value = slave.get(field)
            
            if slave_value and not self._is_duplicate_email(slave_value, master, email_fields):
                if not master_value or str(master_value).strip() == "":
                    merged_fields.append(field)
                    self.logger.debug("    ✓ Kopiuję email %s: %s", field, slave_value)
        
        self.logger.info("  Znaleziono %d pól do scalenia", len(merged_fields))
        
        return merged_fields
    
    def _is_duplicate_phone(self, phone: str, account: Dict[str, Any], phone_fields: List[str]) -> bool:
        """Sprawdza czy telefon już istnieje w firmie (porównanie cyfr)."""
        phone_clean = re.sub(r'[^0-9]', '', str(phone))
        
        for field in phone_fields:
            existing = account.get(field)
            if existing:
                existing_clean = re.sub(r'[^0-9]', '', str(existing))
                if phone_clean == existing_clean:
                    return True
        
        return False
    
    def _is_duplicate_email(self, email: str, account: Dict[str, Any], email_fields: List[str]) -> bool:
        """Sprawdza czy email już istnieje w firmie (case-insensitive)."""
        email_lower = str(email).strip().lower()
        
        for field in email_fields:
            existing = account.get(field)
            if existing:
                existing_lower = str(existing).strip().lower()
                if email_lower == existing_lower:
                    return True
        
        return False
    
    def _transfer_relations(self, master_id: str, slave_id: str, dry_run: bool) -> Dict[str, int]:
        """Przenosi powiązania ze slave do master.
        
        Returns:
            Dict z liczbą przeniesionych rekordów dla każdego modułu
        """
        self.logger.info("  Przenoszenie powiązań ze slave do master...")
        
        transferred = {}
        
        # Kontakty (Account_Name, Polecenie_firma, Polecenie_partner)
        contacts_account = self._transfer_module_records(
            "Contacts", "Account_Name", slave_id, master_id, dry_run
        )
        contacts_polecenie_firma = self._transfer_module_records(
            "Contacts", "Polecenie_firma", slave_id, master_id, dry_run
        )
        contacts_polecenie_partner = self._transfer_module_records(
            "Contacts", "Polecenie_partner", slave_id, master_id, dry_run
        )
        transferred["Contacts"] = contacts_account + contacts_polecenie_firma + contacts_polecenie_partner
        
        # Leads (Firma_w_bazie, Polecenie_firma, Polecenie_partner, Converted_Account)
        leads_firma_w_bazie = self._transfer_module_records(
            "Leads", "Firma_w_bazie", slave_id, master_id, dry_run
        )
        leads_polecenie_firma = self._transfer_module_records(
            "Leads", "Polecenie_firma", slave_id, master_id, dry_run
        )
        leads_polecenie_partner = self._transfer_module_records(
            "Leads", "Polecenie_partner", slave_id, master_id, dry_run
        )
        leads_converted = self._transfer_module_records(
            "Leads", "Converted_Account", slave_id, master_id, dry_run
        )
        transferred["Leads"] = leads_firma_w_bazie + leads_polecenie_firma + leads_polecenie_partner + leads_converted
        
        # Marketing_Leads (Firma_w_bazie, Polecenie_firma, Polecenie_partner)
        ml_firma = self._transfer_module_records(
            "Marketing_Leads", "Firma_w_bazie", slave_id, master_id, dry_run
        )
        ml_polecenie_f = self._transfer_module_records(
            "Marketing_Leads", "Polecenie_firma", slave_id, master_id, dry_run
        )
        ml_polecenie_p = self._transfer_module_records(
            "Marketing_Leads", "Polecenie_partner", slave_id, master_id, dry_run
        )
        transferred["Marketing_Leads"] = ml_firma + ml_polecenie_f + ml_polecenie_p
        
        # EDU_Leads (Firma_w_bazie, Polecenie_firma, Polecenie_partner, Firma_zewnetrzna_przejmujaca)
        edu_firma = self._transfer_module_records(
            "EDU_Leads", "Firma_w_bazie", slave_id, master_id, dry_run
        )
        edu_polecenie_f = self._transfer_module_records(
            "EDU_Leads", "Polecenie_firma", slave_id, master_id, dry_run
        )
        edu_polecenie_p = self._transfer_module_records(
            "EDU_Leads", "Polecenie_partner", slave_id, master_id, dry_run
        )
        edu_zewnetrzna = self._transfer_module_records(
            "EDU_Leads", "Firma_zewnetrzna_przejmujaca", slave_id, master_id, dry_run
        )
        transferred["EDU_Leads"] = edu_firma + edu_polecenie_f + edu_polecenie_p + edu_zewnetrzna
        
        # Klienci (Firma_ASU, Firma_Platnik)
        klienci_asu = self._transfer_module_records(
            "Klienci", "Firma_ASU", slave_id, master_id, dry_run
        )
        klienci_platnik = self._transfer_module_records(
            "Klienci", "Firma_Platnik", slave_id, master_id, dry_run
        )
        transferred["Klienci"] = klienci_asu + klienci_platnik
        
        # Quotes, Invoices, Sales_Orders (Account_Name)
        transferred["Quotes"] = self._transfer_module_records(
            "Quotes", "Account_Name", slave_id, master_id, dry_run
        )
        transferred["Invoices"] = self._transfer_module_records(
            "Invoices", "Account_Name", slave_id, master_id, dry_run
        )
        transferred["Sales_Orders"] = self._transfer_module_records(
            "Sales_Orders", "Account_Name", slave_id, master_id, dry_run
        )
        
        # USER_Historia (Firma)
        transferred["USER_Historia"] = self._transfer_module_records(
            "USER_Historia", "Firma", slave_id, master_id, dry_run
        )
        
        # Campaigns (Polecenie_firma, Polecenie_partner)
        campaigns_firma = self._transfer_module_records(
            "Campaigns", "Polecenie_firma", slave_id, master_id, dry_run
        )
        campaigns_partner = self._transfer_module_records(
            "Campaigns", "Polecenie_partner", slave_id, master_id, dry_run
        )
        transferred["Campaigns"] = campaigns_firma + campaigns_partner
        
        # Deals (przez search - dodatkowe pola poza Account_Name)
        deals_polecenie_firma = self._transfer_module_records(
            "Deals", "Polecenie_firma", slave_id, master_id, dry_run
        )
        deals_polecenie_partner = self._transfer_module_records(
            "Deals", "Polecenie_partner", slave_id, master_id, dry_run
        )
        deals_rodzic = self._transfer_module_records(
            "Deals", "Rodzic_firmy_Deala", slave_id, master_id, dry_run
        )
        transferred["Deals_extra"] = deals_polecenie_firma + deals_polecenie_partner + deals_rodzic
        
        # DODATKOWE MODUŁY (z API ZOHO MEDIDESK)
        
        # Numery_ofert (Firma)
        transferred["Numery_ofert"] = self._transfer_module_records(
            "Numery_ofert", "Firma", slave_id, master_id, dry_run
        )
        
        # Dane_liczbowe_instalacji (Firma_odbiorca_oferty, Firma_platnik)
        dli_odbiorca = self._transfer_module_records(
            "Dane_liczbowe_instalacji", "Firma_odbiorca_oferty", slave_id, master_id, dry_run
        )
        dli_platnik = self._transfer_module_records(
            "Dane_liczbowe_instalacji", "Firma_platnik", slave_id, master_id, dry_run
        )
        transferred["Dane_liczbowe_instalacji"] = dli_odbiorca + dli_platnik
        
        # BILETY_AMOZ_X (Firma_Platnika_AMOZ_X, Firma_Uczestnika_AMOZ_X)
        bilety_platnik = self._transfer_module_records(
            "BILETY_AMOZ_X", "Firma_Platnika_AMOZ_X", slave_id, master_id, dry_run
        )
        bilety_uczestnik = self._transfer_module_records(
            "BILETY_AMOZ_X", "Firma_Uczestnika_AMOZ_X", slave_id, master_id, dry_run
        )
        transferred["BILETY_AMOZ_X"] = bilety_platnik + bilety_uczestnik
        
        # CallCenter (Partner)
        transferred["CallCenter"] = self._transfer_module_records(
            "CallCenter", "Partner", slave_id, master_id, dry_run
        )
        
        # REGONxFIRMA (Firmy)
        transferred["REGONxFIRMA"] = self._transfer_module_records(
            "REGONxFIRMA", "Firmy", slave_id, master_id, dry_run
        )
        
        # CustomerEmails (Platnik_rejestrowy)
        transferred["CustomerEmails"] = self._transfer_module_records(
            "CustomerEmails", "Platnik_rejestrowy", slave_id, master_id, dry_run
        )
        
        # Dane_instalacji (Placowka)
        transferred["Dane_instalacji"] = self._transfer_module_records(
            "Dane_instalacji", "Placowka", slave_id, master_id, dry_run
        )
        
        # PPartnerzy (Firma)
        transferred["PPartnerzy"] = self._transfer_module_records(
            "PPartnerzy", "Firma", slave_id, master_id, dry_run
        )
        
        # FirmaxDKF (Firma)
        transferred["FirmaxDKF"] = self._transfer_module_records(
            "FirmaxDKF", "Firma", slave_id, master_id, dry_run
        )
        
        # TTP (Firma_do_TTP, Dodatkowe_placowki)
        ttp_firma = self._transfer_module_records(
            "TTP", "Firma_do_TTP", slave_id, master_id, dry_run
        )
        ttp_dodatkowe = self._transfer_module_records(
            "TTP", "Dodatkowe_placowki", slave_id, master_id, dry_run
        )
        transferred["TTP"] = ttp_firma + ttp_dodatkowe
        
        # Historyczne_Dane_Firm (Nowa_Firma)
        transferred["Historyczne_Dane_Firm"] = self._transfer_module_records(
            "Historyczne_Dane_Firm", "Nowa_Firma", slave_id, master_id, dry_run
        )
        
        # Ankiety_Spotkan (Firma)
        transferred["Ankiety_Spotkan"] = self._transfer_module_records(
            "Ankiety_Spotkan", "Firma", slave_id, master_id, dry_run
        )
        
        # Dane_lokalizacji (Nazwa_lokalizacji)
        transferred["Dane_lokalizacji"] = self._transfer_module_records(
            "Dane_lokalizacji", "Nazwa_lokalizacji", slave_id, master_id, dry_run
        )
        
        # Lokalizacje (Placowka_medyczna)
        transferred["Lokalizacje"] = self._transfer_module_records(
            "Lokalizacje", "Placowka_medyczna", slave_id, master_id, dry_run
        )
        
        # ZohoSign Documents (zohosign__Account)
        transferred["ZohoSign_Documents"] = self._transfer_module_records(
            "zohosign__ZohoSign_Documents", "zohosign__Account", slave_id, master_id, dry_run
        )
        
        # Accounts powiązania poleceniowe (Polecenie_firma, Polecenie_partner, Firma_polecajaca_Partnera)
        acc_polecenie_f = self._transfer_module_records(
            "Accounts", "Polecenie_firma", slave_id, master_id, dry_run
        )
        acc_polecenie_p = self._transfer_module_records(
            "Accounts", "Polecenie_partner", slave_id, master_id, dry_run
        )
        acc_firma_partnera = self._transfer_module_records(
            "Accounts", "Firma_polecajaca_Partnera", slave_id, master_id, dry_run
        )
        transferred["Accounts_Polecenia"] = acc_polecenie_f + acc_polecenie_p + acc_firma_partnera
        
        self.logger.info("  Przeniesiono łącznie: %s", transferred)
        
        return transferred
    
    def _transfer_module_records(self, module_name: str, field_name: str, 
                                 old_id: str, new_id: str, dry_run: bool) -> int:
        """Przenosi rekordy z jednego modułu (zmienia field_name z old_id na new_id)."""
        try:
            records = self.api_client.search_records_by_criteria(
                module_name, f"{field_name}:equals:{old_id}"
            )
            
            if not records:
                return 0
            
            self.logger.info("    Przenoszę %d rekordów z %s...", len(records), module_name)
            
            if dry_run:
                return len(records)
            
            # Aktualizuj każdy rekord
            count = 0
            for record in records:
                record_id = record.get("id")
                update_data = {field_name: new_id}
                
                # Użyj odpowiedniej metody update (zależnie od modułu)
                try:
                    if module_name == "Contacts":
                        success = self.api_client.update_contact(record_id, update_data)
                    else:
                        # Dla innych modułów użyj uniwersalnej metody
                        url = f"{self.api_client.BASE_URL}/{module_name}/{record_id}"
                        payload = {"data": [update_data]}
                        response = self.api_client._put(url, payload)
                        success = response.get("data", [{}])[0].get("code") == "SUCCESS"
                    
                    if success:
                        count += 1
                except Exception as exc:
                    self.logger.warning("      Błąd aktualizacji rekordu %s: %s", record_id, exc)
            
            self.logger.info("    ✓ Przeniesiono %d/%d rekordów", count, len(records))
            return count
        
        except Exception as exc:
            self.logger.warning("    Błąd przenoszenia z %s: %s", module_name, exc)
            return 0
    
    def _transfer_tags(self, master_id: str, slave_id: str, dry_run: bool) -> int:
        """Przenosi tagi ze slave do master (z deduplikacją).
        
        Returns:
            Liczba przeniesionych tagów
        """
        self.logger.info("  Przenoszenie tagów ze slave do master...")
        
        try:
            # Pobierz tagi z obu firm
            master_tags = self.api_client.get_account_tags(master_id)
            slave_tags = self.api_client.get_account_tags(slave_id)
            
            if not slave_tags:
                self.logger.debug("    Slave nie ma tagów do przeniesienia")
                return 0
            
            # Deduplikacja (case-insensitive)
            master_tags_lower = {tag.lower() for tag in master_tags}
            tags_to_add = []
            
            for tag in slave_tags:
                if tag.lower() not in master_tags_lower:
                    tags_to_add.append(tag)
            
            if not tags_to_add:
                self.logger.debug("    Wszystkie tagi ze slave już są w master")
                return 0
            
            self.logger.info("    Przenoszę %d tagów: %s", len(tags_to_add), tags_to_add)
            
            if dry_run:
                return len(tags_to_add)
            
            # Dodaj tagi do master
            success = self.api_client.add_tags_to_account(master_id, tags_to_add)
            
            if success:
                self.logger.info("    ✓ Przeniesiono %d tagów", len(tags_to_add))
                return len(tags_to_add)
            else:
                self.logger.error("    ✗ Błąd przenoszenia tagów")
                return 0
        
        except Exception as exc:
            self.logger.warning("    Błąd przenoszenia tagów: %s", exc)
            return 0
    
    def _check_parent_child_relation(self, master: Dict[str, Any], slave: Dict[str, Any]) -> bool:
        """Sprawdza czy między master i slave istnieje relacja rodzic/dziecko."""
        master_id = master.get("id")
        slave_id = slave.get("id")
        
        # Czy slave jest dzieckiem master?
        slave_parent = slave.get("Parent_Account")
        if isinstance(slave_parent, dict):
            slave_parent = slave_parent.get("id")
        if slave_parent == master_id:
            return True
        
        # Czy master jest dzieckiem slave?
        master_parent = master.get("Parent_Account")
        if isinstance(master_parent, dict):
            master_parent = master_parent.get("id")
        if master_parent == slave_id:
            return True
        
        return False

