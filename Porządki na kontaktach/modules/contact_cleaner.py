"""Czyszczenie kontaktów - deduplikacja emaili i telefonów."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .data_sanitizer import DataSanitizer
    from .phone_formatter import PhoneFormatter


class ContactCleaner:
    """Czyści kontakty - deduplikuje emaile i telefony, formatuje."""
    
    def __init__(self, sanitizer: DataSanitizer, phone_formatter: PhoneFormatter) -> None:
        self.sanitizer = sanitizer
        self.phone_formatter = phone_formatter
        self.logger = logging.getLogger(__name__)
    
    def clean_emails(self, contact: Dict[str, Any]) -> Dict[str, Any]:
        """Czyści emaile kontaktu: normalizacja, deduplikacja, przepakowanie.
        
        Returns:
            {
                "changes": Dict[str, str],  # field -> new_value
                "removed_duplicates": int,
                "removed_excess": List[str]
            }
        """
        self.logger.debug("  Czyszczenie emaili...")
        
        email_fields = ["Email", "Secondary_Email", "Email_3"]
        
        # 1. Pobierz emaile i znormalizuj (lowercase, trim)
        emails_raw = []
        for field in email_fields:
            value = contact.get(field)
            if value:
                normalized = self.sanitizer.sanitize_email(value)
                if normalized:
                    emails_raw.append(normalized)
        
        self.logger.debug("    Emaile przed: %s", emails_raw)
        
        # 2. Deduplikacja (zachowaj kolejność)
        emails_unique = []
        seen = set()
        for email in emails_raw:
            if email not in seen:
                emails_unique.append(email)
                seen.add(email)
        
        removed_duplicates = len(emails_raw) - len(emails_unique)
        
        # 3. Zapisz nadmiarowe emaile (>3)
        removed_excess = []
        if len(emails_unique) > 3:
            removed_excess = emails_unique[3:]
            emails_unique = emails_unique[:3]
            self.logger.warning("    ⚠ Kontakt ma %d unikalnych emaili - usuwam nadmiarowe: %s",
                              len(emails_raw), removed_excess)
        
        self.logger.debug("    Emaile po: %s", emails_unique)
        
        # 4. Przepakowanie (wypełnij od slot 1)
        changes = {}
        
        for i, field in enumerate(email_fields):
            if i < len(emails_unique):
                new_value = emails_unique[i]
            else:
                new_value = None
            
            # Porównaj z obecną wartością
            old_value = contact.get(field)
            if old_value:
                old_value_normalized = self.sanitizer.sanitize_email(old_value)
            else:
                old_value_normalized = None
            
            if new_value != old_value_normalized:
                changes[field] = new_value
        
        return {
            "changes": changes,
            "removed_duplicates": removed_duplicates,
            "removed_excess": removed_excess
        }
    
    def clean_mobile_phones(self, contact: Dict[str, Any]) -> Dict[str, Any]:
        """Czyści telefony komórkowe: sanityzacja, deduplikacja, formatowanie.
        
        Returns:
            {
                "changes": Dict[str, str],
                "removed_duplicates": int,
                "removed_excess": List[str]
            }
        """
        self.logger.debug("  Czyszczenie telefonów komórkowych...")
        
        mobile_fields = ["Mobile", "Home_Phone", "Telefon_komorkowy_3"]
        
        # 1. Sanityzacja (tylko cyfry)
        phones_sanitized = []
        for field in mobile_fields:
            value = contact.get(field)
            if value:
                sanitized = self.sanitizer.sanitize_phone(value)
                if sanitized:
                    phones_sanitized.append(sanitized)
        
        self.logger.debug("    Telefony przed (cyfry): %s", phones_sanitized)
        
        # 2. Deduplikacja (porównanie cyfr)
        phones_unique = []
        seen = set()
        for phone in phones_sanitized:
            if phone not in seen:
                phones_unique.append(phone)
                seen.add(phone)
        
        removed_duplicates = len(phones_sanitized) - len(phones_unique)
        
        # 3. Formatowanie (XXX XXX XXX dla 9 cyfr)
        phones_formatted = []
        for phone in phones_unique:
            formatted = self.phone_formatter.format_phone(phone, "mobile")
            if formatted:
                phones_formatted.append(formatted)
            else:
                # Nietypowa długość - zostaw same cyfry
                phones_formatted.append(phone)
        
        # 4. Zapisz nadmiarowe telefony (>3)
        removed_excess = []
        if len(phones_formatted) > 3:
            removed_excess = phones_formatted[3:]
            phones_formatted = phones_formatted[:3]
            self.logger.warning("    ⚠ Kontakt ma %d unikalnych tel. komórkowych - usuwam: %s",
                              len(phones_sanitized), removed_excess)
        
        self.logger.debug("    Telefony po (sformatowane): %s", phones_formatted)
        
        # 5. Przepakowanie (wypełnij od slot 1)
        changes = {}
        
        for i, field in enumerate(mobile_fields):
            if i < len(phones_formatted):
                new_value = phones_formatted[i]
            else:
                new_value = None
            
            # Porównaj z obecną wartością (porównaj cyfry)
            old_value = contact.get(field)
            if old_value:
                old_value_sanitized = self.sanitizer.sanitize_phone(old_value)
            else:
                old_value_sanitized = None
            
            if new_value:
                new_value_sanitized = self.sanitizer.sanitize_phone(new_value)
            else:
                new_value_sanitized = None
            
            if new_value_sanitized != old_value_sanitized:
                changes[field] = new_value
        
        return {
            "changes": changes,
            "removed_duplicates": removed_duplicates,
            "removed_excess": removed_excess
        }
    
    def clean_landline_phones(self, contact: Dict[str, Any]) -> Dict[str, Any]:
        """Czyści telefony stacjonarne: sanityzacja, deduplikacja, formatowanie.
        
        Returns:
            {
                "changes": Dict[str, str],
                "removed_duplicates": int,
                "removed_excess": List[str]
            }
        """
        self.logger.debug("  Czyszczenie telefonów stacjonarnych...")
        
        landline_fields = ["Phone", "Other_Phone", "Telefon_stacjonarny_3"]
        
        # 1. Sanityzacja (tylko cyfry)
        phones_sanitized = []
        for field in landline_fields:
            value = contact.get(field)
            if value:
                sanitized = self.sanitizer.sanitize_phone(value)
                if sanitized:
                    phones_sanitized.append(sanitized)
        
        self.logger.debug("    Telefony przed (cyfry): %s", phones_sanitized)
        
        # 2. Deduplikacja (porównanie cyfr)
        phones_unique = []
        seen = set()
        for phone in phones_sanitized:
            if phone not in seen:
                phones_unique.append(phone)
                seen.add(phone)
        
        removed_duplicates = len(phones_sanitized) - len(phones_unique)
        
        # 3. Formatowanie (XX XXX XX XX dla 9 cyfr)
        phones_formatted = []
        for phone in phones_unique:
            formatted = self.phone_formatter.format_phone(phone, "stacjonarny")
            if formatted:
                phones_formatted.append(formatted)
            else:
                # Nietypowa długość - zostaw same cyfry
                phones_formatted.append(phone)
        
        # 4. Zapisz nadmiarowe telefony (>3)
        removed_excess = []
        if len(phones_formatted) > 3:
            removed_excess = phones_formatted[3:]
            phones_formatted = phones_formatted[:3]
            self.logger.warning("    ⚠ Kontakt ma %d unikalnych tel. stacjonarnych - usuwam: %s",
                              len(phones_sanitized), removed_excess)
        
        self.logger.debug("    Telefony po (sformatowane): %s", phones_formatted)
        
        # 5. Przepakowanie (wypełnij od slot 1)
        changes = {}
        
        for i, field in enumerate(landline_fields):
            if i < len(phones_formatted):
                new_value = phones_formatted[i]
            else:
                new_value = None
            
            # Porównaj z obecną wartością (porównaj cyfry)
            old_value = contact.get(field)
            if old_value:
                old_value_sanitized = self.sanitizer.sanitize_phone(old_value)
            else:
                old_value_sanitized = None
            
            if new_value:
                new_value_sanitized = self.sanitizer.sanitize_phone(new_value)
            else:
                new_value_sanitized = None
            
            if new_value_sanitized != old_value_sanitized:
                changes[field] = new_value
        
        return {
            "changes": changes,
            "removed_duplicates": removed_duplicates,
            "removed_excess": removed_excess
        }
    
    def clean_contact(self, contact: Dict[str, Any]) -> Dict[str, Any]:
        """Czyści kontakt - emaile + telefony komórkowe + stacjonarne.
        
        Returns:
            {
                "contact_id": str,
                "full_name": str,
                "has_changes": bool,
                "all_changes": Dict[str, Any],
                "email_dups_removed": int,
                "mobile_dups_removed": int,
                "landline_dups_removed": int,
                "summary": str
            }
        """
        contact_id = contact.get("id", "UNKNOWN")
        first_name = contact.get("First_Name", "")
        last_name = contact.get("Last_Name", "")
        full_name = f"{first_name} {last_name}".strip()
        
        self.logger.info("🧹 Czyszczę kontakt: %s (ID: %s)", full_name, contact_id)
        
        # 1. Czyszczenie emaili
        email_result = self.clean_emails(contact)
        
        # 2. Czyszczenie telefonów komórkowych
        mobile_result = self.clean_mobile_phones(contact)
        
        # 3. Czyszczenie telefonów stacjonarnych
        landline_result = self.clean_landline_phones(contact)
        
        # Połącz wszystkie zmiany
        all_changes = {}
        all_changes.update(email_result["changes"])
        all_changes.update(mobile_result["changes"])
        all_changes.update(landline_result["changes"])
        
        has_changes = len(all_changes) > 0
        
        # Podsumowanie
        summary_parts = []
        if email_result["removed_duplicates"] > 0:
            summary_parts.append(f"Email: {len(contact.get('Email', '') or '')}→{len(email_result['changes'])}")
        if mobile_result["removed_duplicates"] > 0:
            summary_parts.append(f"Mobile: {mobile_result['removed_duplicates']} duplikatów")
        if landline_result["removed_duplicates"] > 0:
            summary_parts.append(f"Landline: {landline_result['removed_duplicates']} duplikatów")
        
        summary = "; ".join(summary_parts) if summary_parts else "Brak zmian"
        
        if has_changes:
            self.logger.info("  ✓ Znaleziono zmiany: %s", summary)
        else:
            self.logger.debug("  Brak zmian do wprowadzenia")
        
        return {
            "contact_id": contact_id,
            "full_name": full_name,
            "has_changes": has_changes,
            "all_changes": all_changes,
            "email_dups_removed": email_result["removed_duplicates"],
            "mobile_dups_removed": mobile_result["removed_duplicates"],
            "landline_dups_removed": landline_result["removed_duplicates"],
            "summary": summary
        }

