"""Sanityzacja wartości z modułu Contacts (placeholdery, spacje itp.)."""

import logging
import re
from typing import Any, Dict, Optional


PLACEHOLDERS = {
    "[b.imienia]",
    "[b.nazwiska]",
    "[b.telefonu]",
    "[b.emailu]",
}


class DataSanitizer:
    """Czyści podstawowe pola kontaktu zgodnie z logiką Deluge."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def sanitize_name(self, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        if text.lower() in {p.lower() for p in PLACEHOLDERS}:
            return None
        return text

    def sanitize_email(self, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip().lower()
        if not text or text in {p.lower() for p in PLACEHOLDERS}:
            return None
        if "@" not in text:
            self.logger.debug("Pomijam email bez @: %s", text)
            return None
        return text

    def sanitize_phone(self, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        
        # Konwertuj float/int na string (usuń .0 z pandas)
        if isinstance(value, (int, float)):
            # Dla float: int() usuwa część dziesiętną, potem str()
            text = str(int(value))
        else:
            text = str(value).strip()
        
        if not text or text.lower() in {p.lower() for p in PLACEHOLDERS}:
            return None
        digits = re.sub(r"[^0-9]", "", text)
        return digits or None

    def sanitize_nip(self, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        digits = re.sub(r"[^0-9]", "", str(value))
        return digits or None

    def sanitize_contact_data(self, contact: Dict[str, Any]) -> Dict[str, Any]:
        sanitized: Dict[str, Any] = {}

        if "First_Name" in contact:
            sanitized["First_Name"] = self.sanitize_name(contact.get("First_Name"))
        if "Last_Name" in contact:
            sanitized["Last_Name"] = self.sanitize_name(contact.get("Last_Name"))

        for email_field in ["Email", "Secondary_Email", "Email_3"]:
            if email_field in contact:
                sanitized[email_field] = self.sanitize_email(contact.get(email_field))

        for phone_field in [
            "Home_Phone",
            "Mobile",
            "Telefon_komorkowy_3",
            "Phone",
            "Other_Phone",
            "Telefon_stacjonarny_3",
        ]:
            if phone_field in contact:
                sanitized[phone_field] = self.sanitize_phone(contact.get(phone_field))

        if "NIP" in contact:
            sanitized["NIP"] = self.sanitize_nip(contact.get("NIP"))

        if "Account_Name" in contact:
            account = contact.get("Account_Name")
            if isinstance(account, dict):
                sanitized["Account_Name"] = account.get("name")
            else:
                sanitized["Account_Name"] = self.sanitize_name(account)

        # Przepisz pozostałe pola bez modyfikacji
        for key, value in contact.items():
            if key not in sanitized:
                sanitized[key] = value

        return sanitized


def is_placeholder(value: Optional[str]) -> bool:
    if value is None:
        return True
    text = str(value).strip().lower()
    return not text or text in {p.lower() for p in PLACEHOLDERS}


def has_value(value: Optional[str]) -> bool:
    return not is_placeholder(value)
