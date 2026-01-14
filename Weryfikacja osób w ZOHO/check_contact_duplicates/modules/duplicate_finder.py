"""Moduł do wyszukiwania potencjalnych duplikatów kontaktów."""

from __future__ import annotations

import logging
from typing import Dict, List

from .contact_search import ContactSearchBuilder
from .data_sanitizer import DataSanitizer


class DuplicateFinder:
    """Buduje i wykonuje zapytania wyszukiwania potencjalnych duplikatów."""

    def __init__(self, api_client) -> None:
        self.api_client = api_client
        self.logger = logging.getLogger(__name__)
        self.builder = ContactSearchBuilder()
        self.sanitizer = DataSanitizer()

    def find_duplicates(self, contact_data: Dict[str, str]) -> List[str]:
        self.logger.info("=" * 80)
        self.logger.info("🔎 SZUKAM DUPLIKATÓW dla kontaktu: %s", contact_data)
        
        sanitized = self.sanitizer.sanitize_contact_data(contact_data)
        self.logger.info("  Dane po sanityzacji: %s", sanitized)

        first = sanitized.get("First_Name")
        last = sanitized.get("Last_Name")
        email = sanitized.get("Email") or sanitized.get("Secondary_Email") or sanitized.get("Email_3")
        phone = (
            sanitized.get("Home_Phone")
            or sanitized.get("Mobile")
            or sanitized.get("Telefon_komorkowy_3")
            or sanitized.get("Phone")
            or sanitized.get("Other_Phone")
            or sanitized.get("Telefon_stacjonarny_3")
        )
        account = sanitized.get("Account_Name")
        
        self.logger.info("  Ekstraktowane wartości:")
        self.logger.info("    - Imię: %s", first or "(brak)")
        self.logger.info("    - Nazwisko: %s", last or "(brak)")
        self.logger.info("    - Email: %s", email or "(brak)")
        self.logger.info("    - Telefon: %s", phone or "(brak)")
        self.logger.info("    - Firma: %s", account or "(brak)")

        all_ids: List[str] = []

        # Grupa 1: Imię + Nazwisko + email/telefon
        if first and last:
            self.logger.info("  📋 Grupa 1: Sprawdzam Imię + Nazwisko + kontakt")
            if email:
                self.logger.info("    → Sprawdzam: %s %s + email %s", first, last, email)
                clause = self.builder.build_first_last_email(first, last, email)
                all_ids.extend(self.builder.execute_single(self.api_client, clause))
            if phone:
                self.logger.info("    → Sprawdzam: %s %s + telefon %s", first, last, phone)
                clauses = self.builder.build_first_last_phone(first, last, phone)
                all_ids.extend(self.builder.execute_criteria_list(self.api_client, clauses))

        # Grupa 2: Nazwisko + Firma + kontakt
        if last and account:
            self.logger.info("  📋 Grupa 2: Sprawdzam Nazwisko + Firma + kontakt")
            if email:
                self.logger.info("    → Sprawdzam: %s (firma: %s) + email %s", last, account, email)
                clause = self.builder.build_company_email("Last_Name", last, account, email)
                all_ids.extend(self.builder.execute_single(self.api_client, clause))
            if phone:
                self.logger.info("    → Sprawdzam: %s (firma: %s) + telefon %s", last, account, phone)
                clauses = self.builder.build_company_phone("Last_Name", last, account, phone)
                all_ids.extend(self.builder.execute_criteria_list(self.api_client, clauses))

        # Grupa 3: Imię + Firma + kontakt
        if first and account:
            self.logger.info("  📋 Grupa 3: Sprawdzam Imię + Firma + kontakt")
            if email:
                self.logger.info("    → Sprawdzam: %s (firma: %s) + email %s", first, account, email)
                clause = self.builder.build_company_email("First_Name", first, account, email)
                all_ids.extend(self.builder.execute_single(self.api_client, clause))
            if phone:
                self.logger.info("    → Sprawdzam: %s (firma: %s) + telefon %s", first, account, phone)
                clauses = self.builder.build_company_phone("First_Name", first, account, phone)
                all_ids.extend(self.builder.execute_criteria_list(self.api_client, clauses))

        # Grupa 4: Imię + Nazwisko + Firma
        if first and last and account:
            self.logger.info("  📋 Grupa 4: Sprawdzam Imię + Nazwisko + Firma")
            self.logger.info("    → Sprawdzam: %s %s (firma: %s)", first, last, account)
            clause = self.builder.build_full_account(first, last, account)
            all_ids.extend(self.builder.execute_single(self.api_client, clause))

        # Grupa 5: tylko nazwisko + kontakt
        if not first and last:
            self.logger.info("  📋 Grupa 5: Sprawdzam tylko Nazwisko + kontakt (brak imienia)")
            if email:
                self.logger.info("    → Sprawdzam: %s + email %s", last, email)
                clause = (
                    f"(Last_Name:equals:{last}) and ("
                    f"(Email:equals:{email}) or (Secondary_Email:equals:{email}) or (Email_3:equals:{email}))"
                )
                all_ids.extend(self.builder.execute_single(self.api_client, clause))
            if phone:
                self.logger.info("    → Sprawdzam: %s + telefon %s", last, phone)
                # Nie używaj build_company_phone - zawiera Account_Name, którego nie mamy
                clauses = self.builder.build_last_name_phone_only(last, phone)
                all_ids.extend(self.builder.execute_criteria_list(self.api_client, clauses))

        # Grupa 6: tylko imię + kontakt
        if first and not last:
            self.logger.info("  📋 Grupa 6: Sprawdzam tylko Imię + kontakt (brak nazwiska)")
            if email:
                self.logger.info("    → Sprawdzam: %s + email %s", first, email)
                clause = (
                    f"(First_Name:equals:{first}) and ("
                    f"(Email:equals:{email}) or (Secondary_Email:equals:{email}) or (Email_3:equals:{email}))"
                )
                all_ids.extend(self.builder.execute_single(self.api_client, clause))
            if phone:
                self.logger.info("    → Sprawdzam: %s + telefon %s", first, phone)
                # Nie używaj build_company_phone - zawiera Account_Name, którego nie mamy
                # Zamiast tego zbuduj proste zapytanie tylko z imieniem i telefonem
                clauses = self.builder.build_first_name_phone_only(first, phone)
                all_ids.extend(self.builder.execute_criteria_list(self.api_client, clauses))

        # Dedup
        deduped: List[str] = []
        for record_id in all_ids:
            if record_id and record_id not in deduped:
                deduped.append(record_id)
        
        self.logger.info("  ✅ WYNIK: Znaleziono %d unikalnych duplikatów: %s", len(deduped), deduped)
        self.logger.info("=" * 80)
        return deduped
