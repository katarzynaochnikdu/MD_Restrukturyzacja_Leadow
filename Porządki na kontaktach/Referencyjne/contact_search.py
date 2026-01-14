"""Budowanie kryteriów wyszukiwania kontaktów w Zoho CRM."""

from __future__ import annotations

import logging
from typing import Dict, List

from .phone_formatter import PhoneFormatter


class ContactSearchBuilder:
    """Generuje kryteria wyszukiwania wg logiki RetrieveAllContactDup."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.phone_formatter = PhoneFormatter()

    # helper functions to build criteria lists
    def _build_email_clause(self, email: str, prefix: str = "") -> str:
        if prefix:
            prefix = f"{prefix} and "
        return (
            f"{prefix}(Email:equals:{email}) or "
            f"(Secondary_Email:equals:{email}) or "
            f"(Email_3:equals:{email})"
        )

    def build_first_last_email(self, first_name: str, last_name: str, email: str) -> str:
        return (
            f"(First_Name:equals:{first_name}) and "
            f"(Last_Name:equals:{last_name}) and ("
            f"(Email:equals:{email}) or "
            f"(Secondary_Email:equals:{email}) or "
            f"(Email_3:equals:{email})"
            ")"
        )

    def build_first_last_phone(self, first_name: str, last_name: str, phone: str) -> List[str]:
        clean = self.phone_formatter.format_phone(phone, "clean")
        if not clean:
            return []
        
        # Przygotuj formaty
        mobile_formatted = self.phone_formatter.format_phone(phone, "mobile")  # XXX XXX XXX
        stacjonarny_formatted = self.phone_formatter.format_phone(phone, "stacjonarny")  # XX XXX XX XX
        
        clauses = []
        
        # Zapytanie 1: Wszystkie pola w formacie bez spacji (XXXXXXXXX)
        all_fields = [
            "Home_Phone", "Mobile", "Telefon_komorkowy_3",
            "Phone", "Other_Phone", "Telefon_stacjonarny_3"
        ]
        phone_conditions = " or ".join([f"({field}:equals:{clean})" for field in all_fields])
        clauses.append(
            f"(First_Name:equals:{first_name}) and "
            f"(Last_Name:equals:{last_name}) and "
            f"({phone_conditions})"
        )
        
        # Zapytanie 2: Pola komórkowe w formacie ze spacjami (XXX XXX XXX)
        mobile_fields = ["Home_Phone", "Mobile", "Telefon_komorkowy_3"]
        mobile_conditions = " or ".join([f"({field}:equals:{mobile_formatted})" for field in mobile_fields])
        clauses.append(
            f"(First_Name:equals:{first_name}) and "
            f"(Last_Name:equals:{last_name}) and "
            f"({mobile_conditions})"
        )
        
        # Zapytanie 3: Pola stacjonarne w formacie ze spacjami (XX XXX XX XX)
        stacjonarny_fields = ["Phone", "Other_Phone", "Telefon_stacjonarny_3"]
        stacjonarny_conditions = " or ".join([f"({field}:equals:{stacjonarny_formatted})" for field in stacjonarny_fields])
        clauses.append(
            f"(First_Name:equals:{first_name}) and "
            f"(Last_Name:equals:{last_name}) and "
            f"({stacjonarny_conditions})"
        )
        
        return clauses

    def build_company_email(self, name_field: str, name_value: str, account_name: str, email: str) -> str:
        return (
            f"({name_field}:equals:{name_value}) and "
            f"(Account_Name:equals:{account_name}) and ("
            f"(Email:equals:{email}) or "
            f"(Secondary_Email:equals:{email}) or "
            f"(Email_3:equals:{email})"
            ")"
        )

    def build_company_phone(
        self, name_field: str, name_value: str, account_name: str, phone: str
    ) -> List[str]:
        clean = self.phone_formatter.format_phone(phone, "clean")
        if not clean:
            return []
        
        # Przygotuj formaty
        mobile_formatted = self.phone_formatter.format_phone(phone, "mobile")  # XXX XXX XXX
        stacjonarny_formatted = self.phone_formatter.format_phone(phone, "stacjonarny")  # XX XXX XX XX
        
        clauses: List[str] = []
        
        # Zapytanie 1: Wszystkie pola w formacie bez spacji (XXXXXXXXX)
        all_fields = [
            "Home_Phone", "Mobile", "Telefon_komorkowy_3",
            "Phone", "Other_Phone", "Telefon_stacjonarny_3"
        ]
        phone_conditions = " or ".join([f"({field}:equals:{clean})" for field in all_fields])
        clauses.append(
            f"({name_field}:equals:{name_value}) and "
            f"(Account_Name:equals:{account_name}) and "
            f"({phone_conditions})"
        )
        
        # Zapytanie 2: Pola komórkowe w formacie ze spacjami (XXX XXX XXX)
        mobile_fields = ["Home_Phone", "Mobile", "Telefon_komorkowy_3"]
        mobile_conditions = " or ".join([f"({field}:equals:{mobile_formatted})" for field in mobile_fields])
        clauses.append(
            f"({name_field}:equals:{name_value}) and "
            f"(Account_Name:equals:{account_name}) and "
            f"({mobile_conditions})"
        )
        
        # Zapytanie 3: Pola stacjonarne w formacie ze spacjami (XX XXX XX XX)
        stacjonarny_fields = ["Phone", "Other_Phone", "Telefon_stacjonarny_3"]
        stacjonarny_conditions = " or ".join([f"({field}:equals:{stacjonarny_formatted})" for field in stacjonarny_fields])
        clauses.append(
            f"({name_field}:equals:{name_value}) and "
            f"(Account_Name:equals:{account_name}) and "
            f"({stacjonarny_conditions})"
        )
        
        return clauses

    def build_first_account(self, first_name: str, account_name: str) -> List[str]:
        clauses: List[str] = []
        clauses.append(
            f"(First_Name:equals:{first_name}) and "
            f"(Account_Name:equals:{account_name})"
        )
        return clauses

    def build_full_account(self, first_name: str, last_name: str, account_name: str) -> str:
        return (
            f"(First_Name:equals:{first_name}) and "
            f"(Last_Name:equals:{last_name}) and "
            f"(Account_Name:equals:{account_name})"
        )

    def build_first_name_phone_only(self, first_name: str, phone: str) -> List[str]:
        """Buduje zapytania tylko: Imię + Telefon (bez Account_Name)."""
        clean = self.phone_formatter.format_phone(phone, "clean")
        if not clean:
            return []
        
        mobile_formatted = self.phone_formatter.format_phone(phone, "mobile")
        stacjonarny_formatted = self.phone_formatter.format_phone(phone, "stacjonarny")
        
        clauses = []
        all_fields = ["Home_Phone", "Mobile", "Telefon_komorkowy_3", "Phone", "Other_Phone", "Telefon_stacjonarny_3"]
        
        # Format bez spacji
        phone_conditions = " or ".join([f"({field}:equals:{clean})" for field in all_fields])
        clauses.append(f"(First_Name:equals:{first_name}) and ({phone_conditions})")
        
        # Pola komórkowe ze spacjami
        mobile_conditions = " or ".join([f"({f}:equals:{mobile_formatted})" for f in ["Home_Phone", "Mobile", "Telefon_komorkowy_3"]])
        clauses.append(f"(First_Name:equals:{first_name}) and ({mobile_conditions})")
        
        # Pola stacjonarne ze spacjami
        stacjonarny_conditions = " or ".join([f"({f}:equals:{stacjonarny_formatted})" for f in ["Phone", "Other_Phone", "Telefon_stacjonarny_3"]])
        clauses.append(f"(First_Name:equals:{first_name}) and ({stacjonarny_conditions})")
        
        return clauses

    def build_last_name_phone_only(self, last_name: str, phone: str) -> List[str]:
        """Buduje zapytania tylko: Nazwisko + Telefon (bez Account_Name)."""
        clean = self.phone_formatter.format_phone(phone, "clean")
        if not clean:
            return []
        
        mobile_formatted = self.phone_formatter.format_phone(phone, "mobile")
        stacjonarny_formatted = self.phone_formatter.format_phone(phone, "stacjonarny")
        
        clauses = []
        all_fields = ["Home_Phone", "Mobile", "Telefon_komorkowy_3", "Phone", "Other_Phone", "Telefon_stacjonarny_3"]
        
        # Format bez spacji
        phone_conditions = " or ".join([f"({field}:equals:{clean})" for field in all_fields])
        clauses.append(f"(Last_Name:equals:{last_name}) and ({phone_conditions})")
        
        # Pola komórkowe ze spacjami
        mobile_conditions = " or ".join([f"({f}:equals:{mobile_formatted})" for f in ["Home_Phone", "Mobile", "Telefon_komorkowy_3"]])
        clauses.append(f"(Last_Name:equals:{last_name}) and ({mobile_conditions})")
        
        # Pola stacjonarne ze spacjami
        stacjonarny_conditions = " or ".join([f"({f}:equals:{stacjonarny_formatted})" for f in ["Phone", "Other_Phone", "Telefon_stacjonarny_3"]])
        clauses.append(f"(Last_Name:equals:{last_name}) and ({stacjonarny_conditions})")
        
        return clauses

    def execute_criteria_list(self, api_client, criteria_list: List[str]) -> List[str]:
        logger = logging.getLogger(__name__)
        ids: List[str] = []
        logger.info("      ⤷ Wykonuję %d wariantów zapytań...", len(criteria_list))
        for idx, criteria in enumerate(criteria_list, start=1):
            logger.debug("        [%d/%d] Kryterium: %s", idx, len(criteria_list), criteria)
            ids.extend(api_client.search_contacts_by_criteria(criteria))
        logger.info("      ⤷ Łącznie znaleziono %d wyników ze wszystkich wariantów", len(ids))
        return ids

    def execute_single(self, api_client, criteria: str) -> List[str]:
        return api_client.search_contacts_by_criteria(criteria)
