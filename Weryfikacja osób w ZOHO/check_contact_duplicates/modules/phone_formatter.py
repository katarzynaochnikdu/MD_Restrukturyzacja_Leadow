"""
Formatowanie i czyszczenie numerów telefonów zgodnie z logiką Zoho/Deluge.
"""

import logging
import re
from typing import Optional


class PhoneFormatter:
    """Udostępnia metody do czyszczenia i formatowania numerów telefonów."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def format_phone(self, phone: Optional[str], format_type: str = "clean") -> str:
        """Czyści i formatuje numer telefonu.

        Args:
            phone: Surowy numer telefonu (może zawierać spacje, +48 itp.)
            format_type: Jeden z "clean", "mobile", "stacjonarny".

        Returns:
            Sformatowany numer telefonu lub pusty string jeśli numer jest niepoprawny.
        """

        if not phone:
            return ""

        phone_str = str(phone).strip()
        if not phone_str:
            return ""

        clean_phone = re.sub(r"[^0-9]", "", phone_str)

        if clean_phone.startswith("48") and len(clean_phone) == 11:
            clean_phone = clean_phone[2:]

        if len(clean_phone) != 9:
            self.logger.debug("Nieprawidłowa długość numeru: %s", clean_phone)
            return ""

        if format_type == "mobile":
            return f"{clean_phone[0:3]} {clean_phone[3:6]} {clean_phone[6:9]}"

        if format_type == "stacjonarny":
            return (
                f"{clean_phone[0:2]} {clean_phone[2:5]} "
                f"{clean_phone[5:7]} {clean_phone[7:9]}"
            )

        return clean_phone

    def get_all_formats(self, phone: Optional[str]) -> dict:
        """Zwraca wszystkie obsługiwane formaty telefonu."""

        clean = self.format_phone(phone, "clean")
        if not clean:
            return {"clean": "", "mobile": "", "stacjonarny": ""}

        return {
            "clean": clean,
            "mobile": self.format_phone(phone, "mobile"),
            "stacjonarny": self.format_phone(phone, "stacjonarny"),
        }


def format_phone_number(phone: Optional[str], format_type: str = "clean") -> str:
    """Funkcja pomocnicza - skrót do PhoneFormatter.format_phone."""

    return PhoneFormatter().format_phone(phone, format_type)
