"""Dopasowanie kontaktów do firm na podstawie domeny emaila."""

from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional
from urllib.parse import urlparse


# Publiczne domeny email (wykluczamy z dopasowania)
PUBLIC_EMAIL_DOMAINS = {
    # Gmail/Google
    "gmail.com", "googlemail.com",
    # Microsoft
    "outlook.com", "hotmail.com", "live.com", "msn.com",
    # Yahoo
    "yahoo.com", "yahoo.pl", "ymail.com",
    # Polskie domeny
    "wp.pl", "o2.pl", "onet.pl", "onet.eu", "interia.pl", "interia.eu",
    "poczta.fm", "tlen.pl", "op.pl", "spoko.pl", "vp.pl",
    "gazeta.pl", "prokonto.pl", "wirtualna-polska.pl",
    # Inne popularne
    "aol.com", "icloud.com", "mail.com", "protonmail.com",
    "zoho.com", "yandex.com", "gmx.com", "gmx.net",
}


class CompanyMatcher:
    """Dopasowuje kontakty do firm na podstawie domeny emaila."""

    def __init__(self, api_client, ignored_domains_file: str = "check_contact_duplicates/ignored_domains.txt") -> None:
        self.api_client = api_client
        self.logger = logging.getLogger(__name__)
        self.ignored_domains_file = ignored_domains_file
        self.custom_ignored_domains = self._load_ignored_domains()

    def extract_email_domain(self, email: str) -> Optional[str]:
        """Wyciąga domenę z adresu email."""
        if not email or "@" not in email:
            return None
        
        domain = email.split("@")[-1].strip().lower()
        return domain if domain else None

    def normalize_website_to_domain(self, website: str) -> Optional[str]:
        """
        Normalizuje URL strony internetowej do czystej domeny.
        Implementacja logiki z formuły Deluge.
        
        Przykłady:
        - https://www.medidesk.com/kontakt → medidesk.com
        - www.example.pl → example.pl
        - http://subdomain.test.com/page → subdomain.test.com
        """
        if not website:
            return None
        
        website = website.strip()
        
        # Usuń protokół (https://, http://)
        if "://" in website:
            website = website.split("://", 1)[1]
        
        # Usuń www. z początku
        if website.lower().startswith("www."):
            website = website[4:]
        
        # Usuń ścieżkę (wszystko po pierwszym /)
        if "/" in website:
            website = website.split("/")[0]
        
        # Usuń port jeśli jest
        if ":" in website:
            website = website.split(":")[0]
        
        return website.lower() if website else None

    def _load_ignored_domains(self) -> set:
        """Wczytuje dodatkowe ignorowane domeny z pliku."""
        domains = set()
        try:
            with open(self.ignored_domains_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        domains.add(line.lower())
            if domains:
                self.logger.info("✓ Wczytano %d dodatkowych ignorowanych domen z pliku", len(domains))
        except FileNotFoundError:
            self.logger.debug("Plik ignorowanych domen nie istnieje: %s", self.ignored_domains_file)
        except Exception as exc:
            self.logger.warning("Błąd wczytywania ignorowanych domen: %s", exc)
        return domains
    
    def add_ignored_domain(self, domain: str) -> bool:
        """Dodaje domenę do listy ignorowanych i zapisuje do pliku."""
        domain = domain.lower().strip()
        if not domain:
            return False
        
        # Dodaj do zestawu w pamięci
        self.custom_ignored_domains.add(domain)
        
        # Dopisz do pliku
        try:
            with open(self.ignored_domains_file, 'a', encoding='utf-8') as f:
                f.write(f"{domain}\n")
            self.logger.info("✓ Dodano domenę '%s' do listy ignorowanych", domain)
            return True
        except Exception as exc:
            self.logger.error("✗ Błąd zapisu domeny do pliku: %s", exc)
            return False
    
    def is_public_domain(self, domain: str) -> bool:
        """Sprawdza czy domena jest publiczną domeną email lub na liście ignorowanych."""
        domain_lower = domain.lower()
        return domain_lower in PUBLIC_EMAIL_DOMAINS or domain_lower in self.custom_ignored_domains

    def find_company_by_domain(self, email_domain: str) -> Optional[Dict]:
        """
        Wyszukuje firmę w module Accounts po domenie.
        
        Kryteria:
        - Domena_z_www:equals:{email_domain}
        - Filtrowanie po stronie Pythona: Adres_w_rekordzie zawiera "Siedziba"
          (pole nie jest dostępne do search w API)
        
        Returns:
            Dict z danymi firmy lub None
        """
        if not email_domain or self.is_public_domain(email_domain):
            self.logger.debug("Pomijam wyszukiwanie dla publicznej domeny: %s", email_domain)
            return None
        
        self.logger.info("🏢 Szukam firmy dla domeny: %s", email_domain)
        
        # Zbuduj kryterium wyszukiwania - tylko po Domena_z_www
        # (Cecha_adresu_w_rekordzie nie jest dostępne do search w API)
        criteria = f"(Domena_z_www:equals:{email_domain})"
        
        self.logger.info("  Kryterium: %s", criteria)
        
        try:
            # Wyszukaj w module Accounts
            import urllib.parse
            encoded = urllib.parse.quote(criteria)
            url = f"{self.api_client.BASE_URL}/Accounts/search?criteria={encoded}"
            
            # Użyj metody _get z api_client
            raw = self.api_client._get(url)
            records = raw.get("data") or []
            
            if not records:
                self.logger.info("  ✗ Nie znaleziono firmy dla domeny: %s", email_domain)
                return None
            
            self.logger.info("  ✓ Znaleziono %d firm dla domeny %s", len(records), email_domain)
            
            # Filtruj po stronie Pythona: tylko firmy z Adres_w_rekordzie zawierającą "Siedziba"
            matching_companies = []
            for record in records:
                adres = record.get("Adres_w_rekordzie") or ""
                firma_name = record.get("Account_Name") or record.get("Name") or "Nieznana"
                if "Siedziba" in str(adres):
                    matching_companies.append(record)
                    self.logger.info("    ✓ Firma '%s' - Adres w rekordzie: '%s' (pasuje!)", firma_name, adres)
                else:
                    self.logger.info("    ⊘ Pomijam firmę '%s' - Adres w rekordzie: '%s' (brak 'Siedziba')", 
                                    firma_name, adres)
            
            if not matching_companies:
                self.logger.info("  ✗ Żadna firma nie ma 'Adres_w_rekordzie' zawierającego 'Siedziba'")
                return None
            
            if len(matching_companies) > 1:
                self.logger.warning("  ⚠ Znaleziono %d firm z 'Siedziba' - biorę pierwszą", 
                                  len(matching_companies))
            
            company = matching_companies[0]
            company_name = company.get("Account_Name") or company.get("Name") or "Nieznana"
            company_id = company.get("id")
            
            self.logger.info("  ✅ Wybrano firmę: %s (ID: %s)", company_name, company_id)
            return company
            
        except Exception as exc:
            self.logger.error("Błąd wyszukiwania firmy dla domeny %s: %s", email_domain, exc)
            return None

    def find_company_for_contact(self, contact_data: Dict) -> Optional[Dict]:
        """
        Znajduje firmę dla kontaktu na podstawie domeny emaila.
        Sprawdza wszystkie emaile (Email, Secondary_Email, Email_3) i bierze pierwszą niepubliczną domenę.
        
        Args:
            contact_data: Dane kontaktu (może zawierać Email, Secondary_Email, Email_3)
        
        Returns:
            Dict z danymi firmy lub None
        """
        # Sprawdź czy kontakt ma już przypisaną firmę
        if contact_data.get("Account_Name"):
            self.logger.debug("Kontakt ma już przypisaną firmę")
            return None
        
        # Zbierz wszystkie emaile
        emails = [
            contact_data.get("Email"),
            contact_data.get("Secondary_Email"),
            contact_data.get("Email_3")
        ]
        emails = [e for e in emails if e]  # usuń puste
        
        if not emails:
            self.logger.debug("Brak emaila w kontakcie - pomijam dopasowanie")
            return None
        
        # Sprawdź każdy email w kolejności
        for email in emails:
            domain = self.extract_email_domain(email)
            if not domain:
                continue
            
            # Pomiń publiczne domeny
            if self.is_public_domain(domain):
                self.logger.debug("Pomijam publiczną domenę: %s (z emaila: %s)", domain, email)
                continue
            
            # Wyszukaj firmę dla pierwszej niepublicznej domeny
            self.logger.info("Sprawdzam domenę firmową: %s (z emaila: %s)", domain, email)
            company = self.find_company_by_domain(domain)
            if company:
                return company
        
        # Wszystkie emaile są publiczne lub nie znaleziono firmy
        return None

