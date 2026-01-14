"""
Moduł do ujednoliconej ekstrakcji źródeł z rekordów Zoho CRM.

Zamienia ~20 różnych pól źródłowych na spójną strukturę 4-poziomową:
- I tier:   Inbound / Outbound
- II tier:  Kanał (Facebook, Strona internetowa, Webinar, Polecenie, Akwizycja, etc.)
- III tier: Szczegół kanału (nazwa webinaru, nazwa ebooka, typ polecenia, etc.)
- IV tier:  Dodatkowy szczegół (nazwa konkretnego partnera/firmy)

OPCJA A: Agreguje bardziej - dla kanałów Outbound ignoruje nadmiarowe szczegóły w III tier.

Używane pola źródłowe w Zoho:
- Outbound_Inbound (Marketing_Leads)
- Zrodlo_inbound, Zrodlo_outbound (Leads, Deals)
- Lead_Source
- Strona_internetowa_medidesk
- Facebook_source
- Webinar, Webinar_medidesk, Webinar_zewnetrzny
- Konferencja, Konferencja_medidesk, Konferencja_zewnetrzna
- Polecenie (picklist: typ polecenia), Polecenie_firma, Polecenie_partner, Polecenie_pracownik (lookup)
- Partner_polecajacy, Firma_polecajaca, Pracownik_polecajacy (lookup w Leads)
- II_poziom_zrodla, III_poziom_zrodla, IV_poziom_zrodla (Ankiety_Spotkan)
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class SourceTiers:
    """Ujednolicona struktura 4 poziomów źródła."""
    tier_1: str  # Inbound / Outbound / brak danych
    tier_2: str  # Kanał
    tier_3: str  # Szczegół
    tier_4: str  # Dodatkowy szczegół (opcjonalnie)

    def __post_init__(self):
        # Jeśli kompletnie brak danych na wszystkich tierach -> ustaw Outbound / Akwizycja telefoniczna
        if all(
            t in (None, "", "brak danych")
            for t in (self.tier_1, self.tier_2, self.tier_3, self.tier_4)
        ):
            self.tier_1 = "Outbound"
            self.tier_2 = "Akwizycja telefoniczna"
            self.tier_3 = "Akwizycja telefoniczna"
            self.tier_4 = "Akwizycja telefoniczna"
            return
        
        # Jeśli brak głębszej informacji, skopiuj nazwę z poprzedniego tieru
        if (self.tier_3 in (None, "", "brak danych")) and (self.tier_2 not in (None, "", "brak danych")):
            self.tier_3 = self.tier_2
        if (self.tier_4 in (None, "", "brak danych")) and (self.tier_3 not in (None, "", "brak danych")):
            self.tier_4 = self.tier_3
    
    def as_tuple(self):
        return (self.tier_1, self.tier_2, self.tier_3, self.tier_4)
    
    def as_dict(self):
        return {
            "I_tier": self.tier_1,
            "II_tier": self.tier_2,
            "III_tier": self.tier_3,
            "IV_tier": self.tier_4,
        }
    
    def __str__(self):
        parts = [self.tier_1, self.tier_2]
        if self.tier_3 and self.tier_3 != "brak danych":
            parts.append(self.tier_3)
        if self.tier_4 and self.tier_4 != "brak danych":
            parts.append(self.tier_4)
        return " / ".join(parts)


def _extract_lookup_name(value: Any) -> Optional[str]:
    """Wyciąga nazwę z pola lookup (dict lub string)."""
    if value is None:
        return None
    if isinstance(value, dict):
        return value.get("name")
    if isinstance(value, str) and value and value != "-None-":
        return value
    return None


def _clean_value(value: Optional[str]) -> Optional[str]:
    """Czyści wartość (usuwa -None-, puste stringi)."""
    if not value or value == "-None-" or value.strip() == "":
        return None
    return value.strip()


def _parse_polecenie_type(polecenie_raw: Optional[str]) -> Optional[str]:
    """
    Parsuje wartość z picklist Polecenie i zwraca czysty typ.
    Np. "Partner polecający (Partner)" -> "Partner polecający"
    """
    if not polecenie_raw:
        return None
    # Usuń część w nawiasach
    val = polecenie_raw.split("(")[0].strip()
    return val if val else None


def extract_source_from_marketing_lead(record: Dict[str, Any]) -> SourceTiers:
    """
    Ekstrahuje źródło z rekordu Marketing_Leads.
    
    Pola używane:
    - Outbound_Inbound → I tier
    - Strona_internetowa_medidesk, Facebook_source, Webinar*, Konferencja* → II/III tier
    - Polecenie (picklist) → II/III tier, Polecenie_* (lookup) → IV tier
    """
    # I tier
    outbound_inbound = _clean_value(record.get("Outbound_Inbound"))
    if outbound_inbound:
        if "outbound" in outbound_inbound.lower():
            tier_1 = "Outbound"
        elif "inbound" in outbound_inbound.lower():
            tier_1 = "Inbound"
        else:
            tier_1 = outbound_inbound
    else:
        tier_1 = "brak danych"
    
    tier_2 = "brak danych"
    tier_3 = "brak danych"
    tier_4 = "brak danych"
    
    # Strona internetowa
    strona = _clean_value(record.get("Strona_internetowa_medidesk"))
    if strona:
        tier_2 = "Strona internetowa medidesk"
        tier_3 = strona
        return SourceTiers(tier_1, tier_2, tier_3, tier_4)
    
    # Facebook
    facebook = _clean_value(record.get("Facebook_source"))
    if facebook:
        tier_2 = "Facebook"
        tier_3 = facebook
        return SourceTiers(tier_1, tier_2, tier_3, tier_4)
    
    # Webinar
    webinar = _clean_value(record.get("Webinar_medidesk")) or _clean_value(record.get("Webinar"))
    webinar_zewn = _clean_value(record.get("Webinar_zewnetrzny"))
    if webinar:
        tier_2 = "Webinary medidesk"
        tier_3 = webinar
        return SourceTiers(tier_1, tier_2, tier_3, tier_4)
    elif webinar_zewn:
        tier_2 = "Webinar zewnętrzny"
        tier_3 = webinar_zewn
        return SourceTiers(tier_1, tier_2, tier_3, tier_4)
    
    # Konferencja
    konf = _clean_value(record.get("Konferencja_medidesk")) or _clean_value(record.get("Konferencja"))
    konf_zewn = _clean_value(record.get("Konferencja_zewnetrzna"))
    if konf:
        tier_2 = "Konferencja medidesk"
        tier_3 = konf
        return SourceTiers(tier_1, tier_2, tier_3, tier_4)
    elif konf_zewn:
        tier_2 = "Konferencja zewnętrzna"
        tier_3 = konf_zewn
        return SourceTiers(tier_1, tier_2, tier_3, tier_4)
    
    # Polecenie - POPRAWIONA LOGIKA
    polecenie_type = _parse_polecenie_type(_clean_value(record.get("Polecenie")))
    polecenie_firma = _extract_lookup_name(record.get("Polecenie_firma"))
    polecenie_partner = _extract_lookup_name(record.get("Polecenie_partner"))
    polecenie_pracownik = _extract_lookup_name(record.get("Polecenie_pracownik"))
    
    if polecenie_type:
        tier_2 = "Polecenie"
        tier_3 = polecenie_type  # "Partner polecający", "Firma polecająca", "Pracownik medidesk"
        # IV tier = konkretna nazwa z lookup
        if "partner" in polecenie_type.lower() and polecenie_partner:
            tier_4 = polecenie_partner
        elif "firma" in polecenie_type.lower() and polecenie_firma:
            tier_4 = polecenie_firma
        elif "pracownik" in polecenie_type.lower() and polecenie_pracownik:
            tier_4 = polecenie_pracownik
        return SourceTiers(tier_1, tier_2, tier_3, tier_4)
    elif polecenie_partner:
        tier_2 = "Polecenie"
        tier_3 = "Partner polecający"
        tier_4 = polecenie_partner
        return SourceTiers(tier_1, tier_2, tier_3, tier_4)
    elif polecenie_firma:
        tier_2 = "Polecenie"
        tier_3 = "Firma polecająca"
        tier_4 = polecenie_firma
        return SourceTiers(tier_1, tier_2, tier_3, tier_4)
    elif polecenie_pracownik:
        tier_2 = "Polecenie"
        tier_3 = "Pracownik medidesk"
        tier_4 = polecenie_pracownik
        return SourceTiers(tier_1, tier_2, tier_3, tier_4)
    
    # Fallback na Lead_Source
    lead_source = _clean_value(record.get("Lead_Source"))
    if lead_source:
        tier_2 = lead_source
    
    return SourceTiers(tier_1, tier_2, tier_3, tier_4)


def extract_source_from_lead(record: Dict[str, Any]) -> SourceTiers:
    """
    Ekstrahuje źródło z rekordu Leads.
    
    OPCJA A: Agreguje bardziej - dla Outbound NIE dodaje szczegółów do III tier.
    
    Pola używane:
    - Zrodlo_inbound, Zrodlo_outbound → I/II tier
    - Dla Inbound: Strona_internetowa_medidesk, Facebook_source, Webinar*, Konferencja* → III tier
    - Polecenie → II/III tier, Partner_polecajacy etc. → IV tier
    """
    # I tier i II tier z Zrodlo_inbound/outbound
    zrodlo_in = _clean_value(record.get("Zrodlo_inbound"))
    zrodlo_out = _clean_value(record.get("Zrodlo_outbound"))
    
    # === OUTBOUND: agreguj, NIE dodawaj szczegółów do III tier ===
    if zrodlo_out:
        tier_1 = "Outbound"
        tier_2 = zrodlo_out  # np. "Akwizycja telefoniczna", "Reinkarnacja"
        tier_3 = "brak danych"  # CELOWO puste - agregacja
        tier_4 = "brak danych"
        return SourceTiers(tier_1, tier_2, tier_3, tier_4)
    
    # === INBOUND: szczegóły mają sens ===
    if zrodlo_in:
        tier_1 = "Inbound"
        tier_2 = zrodlo_in
    else:
        tier_1 = "brak danych"
        tier_2 = "brak danych"
    
    tier_3 = "brak danych"
    tier_4 = "brak danych"
    
    # Strona internetowa - ZAWSZE nadpisuje tier_2
    strona = _clean_value(record.get("Strona_internetowa_medidesk"))
    if strona:
        tier_2 = "Strona internetowa medidesk"
        tier_3 = strona
        return SourceTiers(tier_1, tier_2, tier_3, tier_4)
    
    # Facebook - ZAWSZE nadpisuje tier_2
    facebook = _clean_value(record.get("Facebook_source"))
    if facebook:
        tier_2 = "Facebook"
        tier_3 = facebook
        return SourceTiers(tier_1, tier_2, tier_3, tier_4)
    
    # Webinar - ZAWSZE nadpisuje tier_2
    webinar = _clean_value(record.get("Webinar_medidesk")) or _clean_value(record.get("Webinar"))
    webinar_zewn = _clean_value(record.get("Webinar_zewnetrzny"))
    if webinar:
        tier_2 = "Webinary medidesk"
        tier_3 = webinar
        return SourceTiers(tier_1, tier_2, tier_3, tier_4)
    elif webinar_zewn:
        tier_2 = "Webinar zewnętrzny"
        tier_3 = webinar_zewn
        return SourceTiers(tier_1, tier_2, tier_3, tier_4)
    
    # Konferencja - ZAWSZE nadpisuje tier_2
    konf = _clean_value(record.get("Konferencja_medidesk")) or _clean_value(record.get("Konferencja"))
    konf_zewn = _clean_value(record.get("Konferencja_zewnetrzna"))
    if konf:
        tier_2 = "Konferencja"
        tier_3 = konf
        return SourceTiers(tier_1, tier_2, tier_3, tier_4)
    elif konf_zewn:
        tier_2 = "Konferencja zewnętrzna"
        tier_3 = konf_zewn
        return SourceTiers(tier_1, tier_2, tier_3, tier_4)
    
    # Polecenie - POPRAWIONA LOGIKA
    polecenie_type = _parse_polecenie_type(_clean_value(record.get("Polecenie")))
    partner = _extract_lookup_name(record.get("Partner_polecajacy"))
    firma = _extract_lookup_name(record.get("Firma_polecajaca"))
    pracownik = _extract_lookup_name(record.get("Pracownik_polecajacy"))
    
    if tier_2 == "Polecenie" or polecenie_type or partner or firma or pracownik:
        tier_2 = "Polecenie"
        if polecenie_type:
            tier_3 = polecenie_type
            if "partner" in polecenie_type.lower() and partner:
                tier_4 = partner
            elif "firma" in polecenie_type.lower() and firma:
                tier_4 = firma
            elif "pracownik" in polecenie_type.lower() and pracownik:
                tier_4 = pracownik
        elif partner:
            tier_3 = "Partner polecający"
            tier_4 = partner
        elif firma:
            tier_3 = "Firma polecająca"
            tier_4 = firma
        elif pracownik:
            tier_3 = "Pracownik medidesk"
            tier_4 = pracownik
        return SourceTiers(tier_1, tier_2, tier_3, tier_4)
    
    return SourceTiers(tier_1, tier_2, tier_3, tier_4)


def extract_source_from_deal(record: Dict[str, Any]) -> SourceTiers:
    """
    Ekstrahuje źródło z rekordu Deals.
    Logika analogiczna do Leads.
    """
    return extract_source_from_lead(record)


def extract_source_from_ankieta(record: Dict[str, Any]) -> SourceTiers:
    """
    Ekstrahuje źródło z rekordu Ankiety_Spotkan.
    
    Pola używane:
    - Outbound_Inbound → I tier
    - II_poziom_zrodla → II tier
    - III_poziom_zrodla → III tier
    - IV_poziom_zrodla → IV tier
    """
    outbound_inbound = _clean_value(record.get("Outbound_Inbound"))
    if outbound_inbound:
        if "outbound" in outbound_inbound.lower():
            tier_1 = "Outbound"
        elif "inbound" in outbound_inbound.lower():
            tier_1 = "Inbound"
        else:
            tier_1 = outbound_inbound
    else:
        tier_1 = "brak danych"
    
    tier_2 = _clean_value(record.get("II_poziom_zrodla")) or "brak danych"
    tier_3 = _clean_value(record.get("III_poziom_zrodla")) or "brak danych"
    tier_4 = _clean_value(record.get("IV_poziom_zrodla")) or "brak danych"
    
    return SourceTiers(tier_1, tier_2, tier_3, tier_4)


def extract_source(record: Dict[str, Any], module: str) -> SourceTiers:
    """
    Uniwersalna funkcja do ekstrakcji źródła z dowolnego modułu.
    
    Args:
        record: Rekord z Zoho
        module: Nazwa modułu ("Marketing_Leads", "Leads", "Deals", "Ankiety_Spotkan")
    
    Returns:
        SourceTiers z 4 poziomami źródła
    """
    module_lower = module.lower()
    
    if "marketing" in module_lower:
        return extract_source_from_marketing_lead(record)
    elif "lead" in module_lower:
        return extract_source_from_lead(record)
    elif "deal" in module_lower:
        return extract_source_from_deal(record)
    elif "ankiet" in module_lower:
        return extract_source_from_ankieta(record)
    else:
        return extract_source_from_lead(record)


# ============================================================================
# Mapowanie do formatu arkuszy Excel
# ============================================================================

def source_to_mle_key(source: SourceTiers) -> tuple:
    """
    Konwertuje źródło do klucza używanego w arkuszu MLe.
    Format: (I_tier, II_tier, "OGÓŁEM", III_tier)
    """
    return (source.tier_1, source.tier_2, "OGÓŁEM", source.tier_3)


def source_to_mle_plus_leady_key(source: SourceTiers, uwagi: str) -> tuple:
    """
    Konwertuje źródło do klucza używanego w arkuszu MLe+ Leady.
    Format: (I_tier, II_tier, III_tier, Uwagi)
    
    Args:
        source: Źródło
        uwagi: "Wszelkie Leady (włączając MLe po konwersji na Leada)" lub "MLe bez konwersji na leada"
    """
    return (source.tier_1, source.tier_2, source.tier_3, uwagi)
