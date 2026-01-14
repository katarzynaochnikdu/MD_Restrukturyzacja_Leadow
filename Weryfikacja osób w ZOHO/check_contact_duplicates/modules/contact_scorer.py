"""Uproszczony scoring kontaktów na podstawie kompletności danych."""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from .data_sanitizer import DataSanitizer


IMPORTANT_FIELDS = [
    "First_Name",
    "Last_Name",
    "Stanowisko",
    "Title",
    "Email",
    "Secondary_Email",
    "Email_3",
    "Home_Phone",
    "Mobile",
    "Telefon_komorkowy_3",
    "Phone",
    "Other_Phone",
    "Telefon_stacjonarny_3",
    "Kontakty_wplyw_na_zakupy",
]


class ContactScorer:
    """Oblicza podstawowy scoring kontaktu."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.sanitizer = DataSanitizer()

    def calculate_score(self, contact: Dict[str, Any]) -> Dict[str, Any]:
        sanitized = self.sanitizer.sanitize_contact_data(contact)

        filled_fields: List[str] = []
        for field in IMPORTANT_FIELDS:
            if sanitized.get(field):
                filled_fields.append(field)

        has_account = False
        account_name = None
        account = contact.get("Account_Name")
        if isinstance(account, dict):
            account_name = account.get("name")
            has_account = bool(account.get("id"))
        elif account:
            account_name = account
            has_account = True

        score = len(filled_fields)
        if has_account:
            score += 5

        result = {
            "filled_fields": filled_fields,
            "has_account": has_account,
            "account_name": account_name,
            "overall_score": score,
        }

        self.logger.debug(
            "Scoring kontaktu %s: %s",
            contact.get("id"),
            result,
        )
        return result

    def compare_contacts(
        self, contact_a: Dict[str, Any], contact_b: Dict[str, Any]
    ) -> Dict[str, Any]:
        score_a = self.calculate_score(contact_a)["overall_score"]
        score_b = self.calculate_score(contact_b)["overall_score"]
        better_id = contact_a.get("id") if score_a >= score_b else contact_b.get("id")
        return {
            "better_contact_id": better_id,
            "contact_a_score": score_a,
            "contact_b_score": score_b,
        }
