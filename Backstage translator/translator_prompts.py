# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import List


SYSTEM_PHASE_1 = """Tłumacz UI eventowego EN→PL. Krótko, konkretnie. Formy męskie. Zachowaj {...} i HTML 1:1. Tylko JSON."""


def build_user_phase_1(source_en: str, placeholders: List[str]) -> str:
    if placeholders:
        ph_note = f"PLACEHOLDERS (zachowaj): {', '.join(placeholders)}"
    else:
        ph_note = "Brak placeholderów."
    
    return f"""Tłumacz EN→PL dla UI:
"{source_en}"

{ph_note}

Zasady:
- Formy męskie (Zalogowałeś, Twój, Uczestnik)
- Ticket→Bilet, Event→wydarzenie (małe!), Sign in→Zaloguj się, OTP→kod OTP (zawsze!), Lounge→Strefa lounge, Designation→Stanowisko
- "Letter" i "Legal" (formaty papieru) - NIE tłumacz, zostaw jak są
- Małe litery w środku zdania (bilet, wydarzenie, uczestnik) - NIE kapitalizuj
- Zachowaj {{...}} i HTML dokładnie
- Zwięźle dla UI

JSON (bez komentarzy):
{{"translation":"...", "confidence":1-5, "confidence_reason":"...", "placeholders_found":[], "placeholders_preserved":true, "html_preserved":true, "issues":[]}}"""


SYSTEM_PHASE_3 = """QA tłumaczeń UI. Sprawdź {...}, HTML, formy męskie. Tylko JSON."""


def build_user_phase_3(source_en: str, proposal_pl: str, confidence: int, reason: str, placeholders_en: List[str]) -> str:
    ph_list = ", ".join(placeholders_en) if placeholders_en else "brak"
    return f"""QA tłumaczenia UI:
EN: "{source_en}"
PL: "{proposal_pl}"
Confidence: {confidence}/5
Placeholders: {ph_list}

Sprawdź:
1. {{...}} identyczne?
2. HTML 1:1?
3. Formy męskie OK?
4. Naturalny PL?

Jeśli OK → "BEZ_ZMIAN", jeśli nie → popraw.

JSON:
{{"final_translation":"...", "final_confidence":1-5, "final_reason":"...", "fixed_issues":[]}}"""

