"""Scoring firm (Accounts) - przepisana logika z AccountCalculateScore.txt."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .zoho_api_client import ZohoAPIClient


class AccountScorer:
    """Oblicza scoring firmy według logiki z Deluge."""
    
    def __init__(self, api_client: ZohoAPIClient) -> None:
        self.api_client = api_client
        self.logger = logging.getLogger(__name__)
    
    def _get_important_fields(self, account: Dict[str, Any]) -> List[Dict[str, str]]:
        """Zwraca listę ważnych pól do scoringu."""
        fields = []
        
        # Podstawowe dane identyfikacyjne
        for field in ["Account_Name", "Adres_w_rekordzie", "Nazwa_zwyczajowa", 
                     "Nazwa_handlowa_szyld", "Firma_NIP", "Firma_REGON", "Firma_KRS", "Status_REGON"]:
            fields.append({"name": field, "label": field})
        
        # Dane adresowe siedziby
        for field in ["Billing_Street", "Billing_Code", "Billing_City", "Billing_Gmina", 
                     "Billing_Powiat", "Billing_State", "Billing_Country"]:
            fields.append({"name": field, "label": f"{field} (siedziba)"})
        
        # Dane adresowe filii
        for field in ["Shipping_Street", "Shipping_Code", "Shipping_City", "Shipping_Gmina", 
                     "Shipping_Powiat", "Shipping_State", "Shipping_Country"]:
            fields.append({"name": field, "label": f"{field} (filia)"})
        
        # Telefony komórkowe i zakresy
        mobile_phones = [
            ("Mobile_phone_1", "Telefon komórkowy 1"),
            ("Medical_service_mobile_phone_1", "Zakres usług TKom1"),
            ("Mobile_phone_2", "Telefon komórkowy 2"),
            ("Medical_service_mobile_phone_2", "Zakres usług TKom2"),
            ("Mobile_phone_3", "Telefon komórkowy 3"),
            ("Medical_service_mobile_phone_3", "Zakres usług TKom3"),
        ]
        for field_name, label in mobile_phones:
            fields.append({"name": field_name, "label": label})
        
        # Telefony stacjonarne i zakresy
        landline_phones = [
            ("Phone", "Telefon stacjonarny 1"),
            ("Medical_service_phone_1", "Zakres usług TStac1"),
            ("Phone_2", "Telefon stacjonarny 2"),
            ("Medical_service_phone_2", "Zakres usług TStac2"),
            ("Phone_3", "Telefon stacjonarny 3"),
            ("Medical_service_phone_3", "Zakres usług TStac3"),
        ]
        for field_name, label in landline_phones:
            fields.append({"name": field_name, "label": label})
        
        # E-maile i zakresy
        emails = [
            ("Firma_EMAIL1", "Email 1"),
            ("Medical_service_email_1", "Zakres usług Email1"),
            ("Firma_EMAIL2", "Email 2"),
            ("Medical_service_email_2", "Zakres usług Email2"),
            ("Firma_EMAIL3", "Email 3"),
            ("Medical_service_email_3", "Zakres usług Email3"),
        ]
        for field_name, label in emails:
            fields.append({"name": field_name, "label": label})
        
        # Internet
        fields.append({"name": "Website", "label": "Strona internetowa"})
        
        # Relacje
        fields.append({"name": "Parent_Account", "label": "Firma nadrzędna"})
        
        # Grupy
        for field in ["GROUP_1", "GROUP_2", "GROUP_3"]:
            fields.append({"name": field, "label": field})
        
        return fields
    
    def _should_skip_field(self, field_name: str, field_label: str, account: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """Sprawdza czy pole powinno być pominięte w scoringu.
        
        Returns:
            (should_skip, reason)
        """
        adres_typ = account.get("Adres_w_rekordzie", "")
        
        # Pomijanie adresu siedziby jeśli nie dotyczy
        if field_name.startswith("Billing_"):
            if adres_typ in ["Siedziba", "Siedziba i Filia"]:
                return False, None
            else:
                return True, f"Adres_w_rekordzie nie zawiera siedziby (jest: {adres_typ})"
        
        # Pomijanie adresu filii jeśli nie dotyczy
        if field_name.startswith("Shipping_"):
            if adres_typ == "Filia":
                return False, None
            else:
                return True, f"Adres_w_rekordzie nie zawiera filii (jest: {adres_typ})"
        
        # Sprawdzenie, czy to pole zakresu usług i czy ma odpowiadające pole główne
        service_field_mapping = {
            "Medical_service_mobile_phone_1": "Mobile_phone_1",
            "Medical_service_mobile_phone_2": "Mobile_phone_2",
            "Medical_service_mobile_phone_3": "Mobile_phone_3",
            "Medical_service_phone_1": "Phone",
            "Medical_service_phone_2": "Phone_2",
            "Medical_service_phone_3": "Phone_3",
            "Medical_service_email_1": "Firma_EMAIL1",
            "Medical_service_email_2": "Firma_EMAIL2",
            "Medical_service_email_3": "Firma_EMAIL3",
        }
        
        if field_name in service_field_mapping:
            main_field = service_field_mapping[field_name]
            main_value = account.get(main_field)
            if not main_value or main_value == "":
                return True, f"Brak odpowiadającego pola głównego ({main_field})"
        
        # Pomijanie obszaru placówki
        if field_name.startswith("Obszar_placowki"):
            return True, "Obszar placówki nie jest brany pod uwagę w scoringu"
        
        # Pomijanie zakresów usług (już sprawdzone wyżej, ale dla pewności)
        if field_name.startswith("Medical_service_"):
            return True, "Zakres usług nie jest brany pod uwagę w scoringu"
        
        return False, None
    
    def calculate_details_score(self, account: Dict[str, Any]) -> Dict[str, Any]:
        """Oblicza scoring wypełnienia pól (AccountScoreDetale).
        
        Returns:
            {
                "score": int,
                "filled_fields": List[str]
            }
        """
        self.logger.info("Analiza wypełnionych pól dla firmy: %s", account.get("Account_Name", "UNKNOWN"))
        
        score = 0
        filled_fields = []
        
        important_fields = self._get_important_fields(account)
        
        for field in important_fields:
            field_name = field["name"]
            field_label = field["label"]
            field_value = account.get(field_name)
            
            # Sprawdź czy pole powinno być pominięte
            should_skip, skip_reason = self._should_skip_field(field_name, field_label, account)
            if should_skip:
                self.logger.debug("  Pomijam pole %s: %s", field_label, skip_reason)
                continue
            
            # Sprawdź czy pole jest wypełnione
            if field_value is not None and str(field_value).strip() != "":
                score += 1
                filled_fields.append(field_label)
                self.logger.debug("  ✓ Wypełnione: %s = %s", field_label, str(field_value)[:50])
        
        self.logger.info("  Wynik scoringu pól: %d wypełnionych pól", score)
        
        return {
            "score": score,
            "filled_fields": filled_fields
        }
    
    def calculate_modules_score(self, account_id: str) -> Dict[str, Any]:
        """Oblicza scoring powiązań z modułami.
        
        Returns:
            {
                "modules_count": int,  # Liczba modułów z powiązaniami
                "records_count": int,  # Liczba rekordów powiązanych
                "modules": List[str]   # Nazwy powiązanych modułów
            }
        """
        self.logger.info("Analiza powiązań z modułami dla firmy ID: %s", account_id)
        
        modules_count = 0
        records_count = 0
        modules_list = []
        
        # Leads (Firma_w_bazie + Polecenie_firma + Polecenie_partner + Converted_Account)
        self.logger.debug("  Sprawdzam powiązania z Leads...")
        try:
            leads_firma = self.api_client.search_records_by_criteria("Leads", f"Firma_w_bazie:equals:{account_id}")
            leads_polecenie_f = self.api_client.search_records_by_criteria("Leads", f"Polecenie_firma:equals:{account_id}")
            leads_polecenie_p = self.api_client.search_records_by_criteria("Leads", f"Polecenie_partner:equals:{account_id}")
            leads_converted = self.api_client.search_records_by_criteria("Leads", f"Converted_Account:equals:{account_id}")
            
            total_leads = len(leads_firma) + len(leads_polecenie_f) + len(leads_polecenie_p) + len(leads_converted)
            if total_leads > 0:
                modules_count += 1
                records_count += total_leads
                modules_list.append("Leads")
                self.logger.debug("    → Znaleziono %d Leadów (Firma_w_bazie=%d, Polec_firma=%d, Polec_partner=%d, Converted=%d)", 
                                total_leads, len(leads_firma), len(leads_polecenie_f), len(leads_polecenie_p), len(leads_converted))
        except Exception as exc:
            self.logger.warning("    Błąd sprawdzania Leads: %s", exc)
        
        # Marketing_Leads (Firma_w_bazie + Polecenie_firma + Polecenie_partner)
        self.logger.debug("  Sprawdzam powiązania z Marketing_Leads...")
        try:
            ml_firma = self.api_client.search_records_by_criteria("Marketing_Leads", f"Firma_w_bazie:equals:{account_id}")
            ml_polecenie_f = self.api_client.search_records_by_criteria("Marketing_Leads", f"Polecenie_firma:equals:{account_id}")
            ml_polecenie_p = self.api_client.search_records_by_criteria("Marketing_Leads", f"Polecenie_partner:equals:{account_id}")
            
            total_ml = len(ml_firma) + len(ml_polecenie_f) + len(ml_polecenie_p)
            if total_ml > 0:
                modules_count += 1
                records_count += total_ml
                modules_list.append("Marketing_Leads")
                self.logger.debug("    → Znaleziono %d Marketing_Leads (Firma_w_bazie=%d, Polec_firma=%d, Polec_partner=%d)", 
                                total_ml, len(ml_firma), len(ml_polecenie_f), len(ml_polecenie_p))
        except Exception as exc:
            self.logger.warning("    Błąd sprawdzania Marketing_Leads: %s", exc)
        
        # EDU_Leads (Firma_w_bazie + Polecenie_firma + Polecenie_partner + Firma_zewnetrzna_przejmujaca)
        self.logger.debug("  Sprawdzam powiązania z EDU_Leads...")
        try:
            edu_firma = self.api_client.search_records_by_criteria("EDU_Leads", f"Firma_w_bazie:equals:{account_id}")
            edu_polecenie_f = self.api_client.search_records_by_criteria("EDU_Leads", f"Polecenie_firma:equals:{account_id}")
            edu_polecenie_p = self.api_client.search_records_by_criteria("EDU_Leads", f"Polecenie_partner:equals:{account_id}")
            edu_zewnetrzna = self.api_client.search_records_by_criteria("EDU_Leads", f"Firma_zewnetrzna_przejmujaca:equals:{account_id}")
            
            total_edu = len(edu_firma) + len(edu_polecenie_f) + len(edu_polecenie_p) + len(edu_zewnetrzna)
            if total_edu > 0:
                modules_count += 1
                records_count += total_edu
                modules_list.append("EDU_Leads")
                self.logger.debug("    → Znaleziono %d EDU_Leads (Firma_w_bazie=%d, Polec_firma=%d, Polec_partner=%d, Zewn=%d)", 
                                total_edu, len(edu_firma), len(edu_polecenie_f), len(edu_polecenie_p), len(edu_zewnetrzna))
        except Exception as exc:
            self.logger.warning("    Błąd sprawdzania EDU_Leads: %s", exc)
        
        # Contacts (Account_Name + Polecenie_firma + Polecenie_partner)
        self.logger.debug("  Sprawdzam powiązania z Contacts...")
        try:
            contacts_account = self.api_client.search_records_by_criteria("Contacts", f"Account_Name:equals:{account_id}")
            contacts_polecenie_f = self.api_client.search_records_by_criteria("Contacts", f"Polecenie_firma:equals:{account_id}")
            contacts_polecenie_p = self.api_client.search_records_by_criteria("Contacts", f"Polecenie_partner:equals:{account_id}")
            
            total_contacts = len(contacts_account) + len(contacts_polecenie_f) + len(contacts_polecenie_p)
            if total_contacts > 0:
                modules_count += 1
                records_count += total_contacts
                modules_list.append("Contacts")
                self.logger.debug("    → Znaleziono %d Contacts (Account=%d, Polec_firma=%d, Polec_partner=%d)", 
                                total_contacts, len(contacts_account), len(contacts_polecenie_f), len(contacts_polecenie_p))
        except Exception as exc:
            self.logger.warning("    Błąd sprawdzania Contacts: %s", exc)
        
        # Deals (Account_Name przez related + dodatkowe pola przez search)
        self.logger.debug("  Sprawdzam powiązania z Deals...")
        try:
            deals_related = self.api_client.get_related_records("Deals", "Accounts", account_id)
            deals_polecenie_f = self.api_client.search_records_by_criteria("Deals", f"Polecenie_firma:equals:{account_id}")
            deals_polecenie_p = self.api_client.search_records_by_criteria("Deals", f"Polecenie_partner:equals:{account_id}")
            deals_rodzic = self.api_client.search_records_by_criteria("Deals", f"Rodzic_firmy_Deala:equals:{account_id}")
            
            total_deals = len(deals_related) + len(deals_polecenie_f) + len(deals_polecenie_p) + len(deals_rodzic)
            if total_deals > 0:
                modules_count += 1
                records_count += total_deals
                modules_list.append("Deals")
                self.logger.debug("    → Znaleziono %d Deals (related=%d, Polec_firma=%d, Polec_partner=%d, Rodzic=%d)", 
                                total_deals, len(deals_related), len(deals_polecenie_f), len(deals_polecenie_p), len(deals_rodzic))
        except Exception as exc:
            self.logger.debug("    Brak powiązań z Deals (lub błąd: %s)", exc)
        
        # Pozostałe moduły standardowe (Notes, Tasks, Calls, Events, Quotes, Invoices, Sales_Orders)
        standard_modules = ["Notes", "Tasks", "Calls", "Events", "Quotes", "Invoices", "Sales_Orders"]
        for module in standard_modules:
            self.logger.debug("  Sprawdzam powiązania z %s...", module)
            try:
                related = self.api_client.get_related_records(module, "Accounts", account_id)
                if related:
                    modules_count += 1
                    records_count += len(related)
                    modules_list.append(module)
                    self.logger.debug("    → Znaleziono %d rekordów w %s", len(related), module)
            except Exception as exc:
                self.logger.debug("    Brak powiązań z %s (lub błąd: %s)", module, exc)
        
        # Sprawdzenie czy firma jest rodzicem dla innych firm
        self.logger.debug("  Sprawdzam firmy potomne...")
        try:
            child_accounts = self.api_client.search_records_by_criteria("Accounts", f"Parent_Account:equals:{account_id}")
            if child_accounts:
                modules_count += 1
                records_count += len(child_accounts)
                modules_list.append("Child_Accounts")
                self.logger.debug("    → Znaleziono %d firm potomnych", len(child_accounts))
        except Exception as exc:
            self.logger.warning("    Błąd sprawdzania firm potomnych: %s", exc)
        
        # Sprawdzenie powiązań poleceniowych w Accounts (Polecenie_firma, Polecenie_partner, Firma_polecajaca_Partnera)
        self.logger.debug("  Sprawdzam powiązania poleceniowe Accounts...")
        try:
            acc_polecenie_f = self.api_client.search_records_by_criteria("Accounts", f"Polecenie_firma:equals:{account_id}")
            acc_polecenie_p = self.api_client.search_records_by_criteria("Accounts", f"Polecenie_partner:equals:{account_id}")
            acc_firma_partnera = self.api_client.search_records_by_criteria("Accounts", f"Firma_polecajaca_Partnera:equals:{account_id}")
            
            total_acc_polecenia = len(acc_polecenie_f) + len(acc_polecenie_p) + len(acc_firma_partnera)
            if total_acc_polecenia > 0:
                modules_count += 1
                records_count += total_acc_polecenia
                modules_list.append("Accounts_Polecenia")
                self.logger.debug("    → Znaleziono %d powiązań poleceniowych Accounts (Polec_f=%d, Polec_p=%d, Firma_partnera=%d)", 
                                total_acc_polecenia, len(acc_polecenie_f), len(acc_polecenie_p), len(acc_firma_partnera))
        except Exception as exc:
            self.logger.warning("    Błąd sprawdzania powiązań poleceniowych Accounts: %s", exc)
        
        # USER_Historia (Firma)
        self.logger.debug("  Sprawdzam powiązania z USER_Historia...")
        try:
            user_historia = self.api_client.search_records_by_criteria("USER_Historia", f"Firma:equals:{account_id}")
            if user_historia:
                modules_count += 1
                records_count += len(user_historia)
                modules_list.append("USER_Historia")
                self.logger.debug("    → Znaleziono %d rekordów w USER_Historia", len(user_historia))
        except Exception as exc:
            self.logger.debug("    Brak powiązań z USER_Historia (lub błąd: %s)", exc)
        
        # Campaigns (Polecenie_firma, Polecenie_partner)
        self.logger.debug("  Sprawdzam powiązania z Campaigns...")
        try:
            campaigns_firma = self.api_client.search_records_by_criteria("Campaigns", f"Polecenie_firma:equals:{account_id}")
            campaigns_partner = self.api_client.search_records_by_criteria("Campaigns", f"Polecenie_partner:equals:{account_id}")
            
            total_campaigns = len(campaigns_firma) + len(campaigns_partner)
            if total_campaigns > 0:
                modules_count += 1
                records_count += total_campaigns
                modules_list.append("Campaigns")
                self.logger.debug("    → Znaleziono %d rekordów w Campaigns (Polec_firma=%d, Polec_partner=%d)", 
                                total_campaigns, len(campaigns_firma), len(campaigns_partner))
        except Exception as exc:
            self.logger.debug("    Brak powiązań z Campaigns (lub błąd: %s)", exc)
        
        # DODATKOWE MODUŁY - sprawdzanie tylko jeśli istnieją (większość może nie mieć powiązań)
        additional_modules_checks = [
            ("Numery_ofert", "Firma"),
            ("BILETY_AMOZ_X", "Firma_Platnika_AMOZ_X"),
            ("BILETY_AMOZ_X", "Firma_Uczestnika_AMOZ_X"),
            ("CallCenter", "Partner"),
            ("CustomerEmails", "Platnik_rejestrowy"),
            ("Dane_instalacji", "Placowka"),
            ("PPartnerzy", "Firma"),
            ("TTP", "Firma_do_TTP"),
            ("Ankiety_Spotkan", "Firma"),
            ("Lokalizacje", "Placowka_medyczna"),
        ]
        
        for module, field in additional_modules_checks:
            try:
                records = self.api_client.search_records_by_criteria(module, f"{field}:equals:{account_id}")
                if records:
                    modules_count += 1
                    records_count += len(records)
                    if module not in modules_list:
                        modules_list.append(module)
                    self.logger.debug("    → Znaleziono %d rekordów w %s.%s", len(records), module, field)
            except Exception:
                pass  # Ignoruj błędy - moduł może nie istnieć lub brak uprawnień
        
        self.logger.info("  Wynik scoringu modułów: %d modułów, %d rekordów", modules_count, records_count)
        
        return {
            "modules_count": modules_count,
            "records_count": records_count,
            "modules": modules_list
        }
    
    def calculate_companies_score(self, account_id: str, account: Dict[str, Any]) -> Dict[str, Any]:
        """Oblicza scoring powiązań z innymi firmami (rodzic/potomkowie).
        
        Returns:
            {
                "score": int,
                "relations": List[str]
            }
        """
        self.logger.info("Analiza powiązań z innymi firmami dla ID: %s", account_id)
        
        score = 0
        relations = []
        
        # Sprawdź czy firma ma rodzica
        parent_id = account.get("Parent_Account")
        if parent_id:
            # Parent_Account może być dict z 'id' i 'name' lub samym ID
            if isinstance(parent_id, dict):
                parent_id = parent_id.get("id")
            
            if parent_id:
                score += 1
                relations.append("Rodzic")
                self.logger.debug("  ✓ Firma ma firmę nadrzędną (Parent_Account)")
        
        # Sprawdź czy firma ma potomków
        try:
            child_accounts = self.api_client.search_records_by_criteria("Accounts", f"Parent_Account:equals:{account_id}")
            if child_accounts:
                score += len(child_accounts)
                relations.append("Potomkowie")
                self.logger.debug("  ✓ Znaleziono %d firm potomnych", len(child_accounts))
        except Exception as exc:
            self.logger.warning("  Błąd sprawdzania firm potomnych: %s", exc)
        
        self.logger.info("  Wynik scoringu firm powiązanych: %d", score)
        
        return {
            "score": score,
            "relations": relations
        }
    
    def calculate_clients_score(self, account_id: str) -> int:
        """Oblicza scoring powiązań w module Klienci (Firma_ASU, Firma_Platnik).
        
        Returns:
            Liczba powiązań w module Klienci
        """
        self.logger.info("Analiza powiązań w module Klienci dla ID: %s", account_id)
        
        total_count = 0
        unique_ids = set()
        
        # Szukaj jako Firma_ASU
        try:
            klient_asu = self.api_client.search_records_by_criteria("Klienci", f"Firma_ASU:equals:{account_id}")
            for klient in klient_asu:
                unique_ids.add(klient.get("id"))
            self.logger.debug("  Znaleziono %d powiązań jako Firma_ASU", len(klient_asu))
        except Exception as exc:
            self.logger.warning("  Błąd sprawdzania Firma_ASU: %s", exc)
        
        # Szukaj jako Firma_Platnik
        try:
            klient_platnik = self.api_client.search_records_by_criteria("Klienci", f"Firma_Platnik:equals:{account_id}")
            for klient in klient_platnik:
                unique_ids.add(klient.get("id"))
            self.logger.debug("  Znaleziono %d powiązań jako Firma_Platnik", len(klient_platnik))
        except Exception as exc:
            self.logger.warning("  Błąd sprawdzania Firma_Platnik: %s", exc)
        
        total_count = len(unique_ids)
        self.logger.info("  Wynik scoringu Klienci: %d unikalnych powiązań", total_count)
        
        return total_count
    
    def calculate_full_score(self, account_id: str) -> Dict[str, Any]:
        """Oblicza pełny scoring firmy (wszystkie komponenty).
        
        Returns:
            {
                "account_id": str,
                "account_name": str,
                "AccountScoreDetale": int,
                "AccountWypelnionePola": List[str],
                "AccountScorePowiazaniaModuly": int,
                "AccountScorePowiazaniaRekordyModulow": int,
                "AccountPowiazania": List[str],
                "AccountScoreFirmyPowiazane": int,
                "AccountFirmyPowiazane": List[str],
                "AccountScoreKlienci": int,
                "total_score": int
            }
        """
        self.logger.info("=== START: Scoring firmy dla ID: %s ===", account_id)
        
        # Pobierz dane firmy
        account = self.api_client.get_account_by_id(account_id)
        if not account:
            self.logger.error("BŁĄD: Nie znaleziono firmy o ID: %s", account_id)
            raise ValueError(f"Nie znaleziono firmy o ID: {account_id}")
        
        account_name = account.get("Account_Name", "UNKNOWN")
        self.logger.info("Znaleziono firmę: %s", account_name)
        
        # 1. Scoring wypełnionych pól
        details_result = self.calculate_details_score(account)
        account_score_detale = details_result["score"]
        account_wypelnione_pola = details_result["filled_fields"]
        
        # 2. Scoring powiązań z modułami
        modules_result = self.calculate_modules_score(account_id)
        account_score_powiazania_moduly = modules_result["modules_count"]
        account_score_powiazania_rekordy_modulow = modules_result["records_count"]
        account_powiazania = modules_result["modules"]
        
        # 3. Scoring powiązań z firmami
        companies_result = self.calculate_companies_score(account_id, account)
        account_score_firmy_powiazane = companies_result["score"]
        account_firmy_powiazane = companies_result["relations"]
        
        # 4. Scoring powiązań w module Klienci
        account_score_klienci = self.calculate_clients_score(account_id)
        
        # Oblicz całkowity scoring
        total_score = (
            account_score_detale +
            account_score_powiazania_moduly +
            account_score_powiazania_rekordy_modulow +
            account_score_firmy_powiazane +
            account_score_klienci
        )
        
        result = {
            "account_id": account_id,
            "account_name": account_name,
            "AccountScoreDetale": account_score_detale,
            "AccountWypelnionePola": account_wypelnione_pola,
            "AccountScorePowiazaniaModuly": account_score_powiazania_moduly,
            "AccountScorePowiazaniaRekordyModulow": account_score_powiazania_rekordy_modulow,
            "AccountPowiazania": account_powiazania,
            "AccountScoreFirmyPowiazane": account_score_firmy_powiazane,
            "AccountFirmyPowiazane": account_firmy_powiazane,
            "AccountScoreKlienci": account_score_klienci,
            "total_score": total_score
        }
        
        self.logger.info("=== PODSUMOWANIE SCORINGU FIRMY ===")
        self.logger.info("1. Ilość wypełnionych pól: %d", account_score_detale)
        self.logger.info("2. Liczba modułów z powiązaniami: %d", account_score_powiazania_moduly)
        self.logger.info("3. Liczba rekordów powiązanych w modułach: %d", account_score_powiazania_rekordy_modulow)
        self.logger.info("4. Liczba powiązanych firm (rodzic/potomkowie): %d", account_score_firmy_powiazane)
        self.logger.info("5. Liczba powiązań w module Klienci: %d", account_score_klienci)
        self.logger.info("6. CAŁKOWITY SCORING: %d", total_score)
        self.logger.info("=== KONIEC SCORINGU FIRMY ===")
        
        return result

