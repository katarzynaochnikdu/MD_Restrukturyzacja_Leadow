"""Przypisywanie firm do kontaktów na podstawie domen emaili."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING

if TYPE_CHECKING:
    from .zoho_api_client import ZohoAPIClient


# Lista publicznych domen do ignorowania
PUBLIC_DOMAINS = {
    # Gmail/Google
    "gmail.com", "googlemail.com",
    # Microsoft
    "outlook.com", "hotmail.com", "live.com", "msn.com",
    # Yahoo
    "yahoo.com", "yahoo.pl", "ymail.com",
    # Polskie
    "wp.pl", "o2.pl", "onet.pl", "onet.eu", "interia.pl", "interia.eu",
    "poczta.fm", "tlen.pl", "op.pl", "spoko.pl", "vp.pl",
    "gazeta.pl", "prokonto.pl",
    # Inne
    "aol.com", "icloud.com", "mail.com", "protonmail.com", "zoho.com",
}


class CompanyAssigner:
    """Przypisuje firmy do kontaktów na podstawie domen emaili."""
    
    def __init__(self, api_client: ZohoAPIClient) -> None:
        self.api_client = api_client
        self.logger = logging.getLogger(__name__)
    
    def extract_domain(self, email: Optional[str]) -> Optional[str]:
        """Wyciąga domenę z adresu email."""
        if not email or "@" not in email:
            return None
        
        parts = email.split("@")
        if len(parts) != 2:
            return None
        
        domain = parts[1].strip().lower()
        return domain if domain else None
    
    def is_public_domain(self, domain: str) -> bool:
        """Sprawdza czy domena jest publiczna (do pominięcia)."""
        return domain.lower() in PUBLIC_DOMAINS
    
    def find_companies_by_domain(self, domain: str) -> List[Dict[str, Any]]:
        """Szuka firm po domenie (Domena_z_www lub Website).
        
        Zwraca tylko firmy z Adres_w_rekordzie zawierającym "Siedziba".
        """
        self.logger.debug("    Szukam firm dla domeny: %s", domain)
        
        # Szukaj po Domena_z_www
        try:
            companies = self.api_client.search_accounts_by_criteria(f"Domena_z_www:equals:{domain}")
        except Exception as exc:
            self.logger.warning("    Błąd wyszukiwania po Domena_z_www: %s", exc)
            companies = []
        
        # Jeśli nie znaleziono, spróbuj po Website (contains domain)
        if not companies:
            try:
                # Website może zawierać http://, https://, www. itp.
                companies_website = self.api_client.search_accounts_by_criteria(f"Website:contains:{domain}")
                companies.extend(companies_website)
            except Exception as exc:
                self.logger.warning("    Błąd wyszukiwania po Website: %s", exc)
        
        # Filtruj: tylko firmy z "Siedziba" w Adres_w_rekordzie
        filtered = []
        for company in companies:
            adres_typ = company.get("Adres_w_rekordzie", "")
            if "Siedziba" in str(adres_typ):
                filtered.append(company)
            else:
                self.logger.debug("      Pomijam firmę %s (Adres_w_rekordzie: %s)", 
                                company.get("Account_Name"), adres_typ)
        
        self.logger.debug("    → Znaleziono %d firm (po filtrze: %d)", len(companies), len(filtered))
        
        return filtered
    
    def assign_or_verify_company(self, contact: Dict[str, Any]) -> Dict[str, Any]:
        """Przypisuje lub weryfikuje firmę dla kontaktu.
        
        Returns:
            {
                "contact_id": str,
                "full_name": str,
                "action": str,  # "assigned", "verified", "multiple_matches", "no_match", "skipped"
                "company_id": Optional[str],
                "company_name": Optional[str],
                "domain": Optional[str],
                "matching_companies": List[Dict],  # Dla multiple_matches
                "error": Optional[str]
            }
        """
        contact_id = contact.get("id", "UNKNOWN")
        first_name = contact.get("First_Name", "")
        last_name = contact.get("Last_Name", "")
        full_name = f"{first_name} {last_name}".strip()
        
        self.logger.info("🏢 Sprawdzam firmę dla kontaktu: %s (ID: %s)", full_name, contact_id)
        
        result = {
            "contact_id": contact_id,
            "full_name": full_name,
            "action": "skipped",
            "company_id": None,
            "company_name": None,
            "domain": None,
            "matching_companies": [],
            "error": None
        }
        
        # Sprawdź czy kontakt ma już przypisaną firmę
        account_name = contact.get("Account_Name")
        
        if account_name:
            # Kontakt ma już firmę - weryfikuj czy istnieje
            account_id = None
            if isinstance(account_name, dict):
                account_id = account_name.get("id")
                account_name_str = account_name.get("name", "")
            else:
                account_id = account_name
                account_name_str = str(account_name)
            
            self.logger.debug("  Kontakt ma firmę: %s (ID: %s)", account_name_str, account_id)
            
            if account_id:
                try:
                    # Sprawdź czy firma istnieje
                    company = self.api_client.get_account_by_id(str(account_id))
                    if company:
                        self.logger.debug("  ✓ Firma istnieje")
                        result["action"] = "verified"
                        result["company_id"] = str(account_id)
                        result["company_name"] = company.get("Account_Name")
                        return result
                    else:
                        self.logger.warning("  ✗ Firma nie istnieje - usuwam przypisanie")
                        result["action"] = "removed"
                        result["error"] = "Firma nie istnieje w systemie"
                        return result
                except Exception as exc:
                    self.logger.error("  Błąd weryfikacji firmy: %s", exc)
                    result["error"] = f"Błąd weryfikacji: {exc}"
                    return result
            else:
                self.logger.debug("  Kontakt ma firmę ale brak ID - usuwam")
                result["action"] = "removed"
                return result
        
        # Kontakt nie ma firmy - spróbuj przypisać po domenie emaila
        self.logger.debug("  Kontakt bez firmy - sprawdzam emaile...")
        
        # Zbierz domeny z wszystkich emaili
        email_fields = ["Email", "Secondary_Email", "Email_3"]
        domains = []
        
        for field in email_fields:
            email = contact.get(field)
            if email:
                domain = self.extract_domain(email)
                if domain and not self.is_public_domain(domain):
                    domains.append(domain)
                    self.logger.debug("    Email %s -> domena: %s", email, domain)
                elif domain and self.is_public_domain(domain):
                    self.logger.debug("    Email %s -> domena publiczna (%s) - pomijam", email, domain)
        
        if not domains:
            self.logger.debug("  Brak niepublicznych domen - brak przypisania")
            result["action"] = "no_match"
            return result
        
        # Deduplikacja domen
        unique_domains = []
        seen = set()
        for domain in domains:
            if domain not in seen:
                unique_domains.append(domain)
                seen.add(domain)
        
        # Szukaj firm dla każdej domeny
        all_matching_companies = []
        matched_domain = None
        
        for domain in unique_domains:
            companies = self.find_companies_by_domain(domain)
            if companies:
                all_matching_companies.extend(companies)
                matched_domain = domain
                break  # Użyj pierwszej domeny z wynikami
        
        if not all_matching_companies:
            self.logger.debug("  Brak firm dla domen: %s", unique_domains)
            result["action"] = "no_match"
            result["domain"] = ", ".join(unique_domains)
            return result
        
        # Deduplikacja firm (po ID)
        unique_companies = []
        seen_ids = set()
        for company in all_matching_companies:
            company_id = company.get("id")
            if company_id not in seen_ids:
                unique_companies.append(company)
                seen_ids.add(company_id)
        
        if len(unique_companies) == 1:
            # Dokładnie jedna firma - przypisz automatycznie
            company = unique_companies[0]
            company_id = company.get("id")
            company_name = company.get("Account_Name")
            
            self.logger.info("  ✓ Automatycznie przypisuję firmę: %s (domena: %s)", 
                           company_name, matched_domain)
            
            result["action"] = "assigned"
            result["company_id"] = company_id
            result["company_name"] = company_name
            result["domain"] = matched_domain
            return result
        
        elif len(unique_companies) > 1:
            # Wiele firm - wymaga ręcznej weryfikacji
            self.logger.warning("  ⚠ Wiele firm dla domeny %s: %s", 
                              matched_domain, [c.get("Account_Name") for c in unique_companies])
            
            result["action"] = "multiple_matches"
            result["domain"] = matched_domain
            result["matching_companies"] = [
                {
                    "id": c.get("id"),
                    "name": c.get("Account_Name"),
                    "adres_typ": c.get("Adres_w_rekordzie")
                }
                for c in unique_companies
            ]
            return result
        
        return result
    
    def apply_company_assignment(self, contact_id: str, company_id: Optional[str], dry_run: bool = True) -> bool:
        """Przypisuje firmę do kontaktu (lub usuwa przypisanie).
        
        Args:
            contact_id: ID kontaktu
            company_id: ID firmy do przypisania (None = usuń przypisanie)
            dry_run: Jeśli True, tylko symulacja
        
        Returns:
            True jeśli operacja powiodła się (lub dry_run)
        """
        if dry_run:
            self.logger.debug("  [DRY-RUN] Przypisanie firmy %s do kontaktu %s", company_id, contact_id)
            return True
        
        update_data = {
            "Account_Name": company_id
        }
        
        try:
            success = self.api_client.update_contact(contact_id, update_data)
            if success:
                self.logger.info("  ✅ Zaktualizowano kontakt %s", contact_id)
            else:
                self.logger.error("  ✗ Błąd aktualizacji kontaktu %s", contact_id)
            return success
        except Exception as exc:
            self.logger.error("  ✗ Błąd podczas aktualizacji kontaktu %s: %s", contact_id, exc)
            return False

