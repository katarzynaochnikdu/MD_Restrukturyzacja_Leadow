"""Rozszerzony klient Zoho CRM dla modułów Contacts i Accounts."""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
import urllib.parse
from typing import Any, Dict, List, Optional


class ZohoAPIClient:
    """Zawija wywołania REST do Zoho CRM (Contacts, Accounts, inne moduły)."""

    BASE_URL = "https://www.zohoapis.eu/crm/v3"

    def __init__(self, access_token: str) -> None:
        self.access_token = access_token
        self.logger = logging.getLogger(__name__)

    def _request(self, url: str, method: str = "GET", data: Optional[bytes] = None, 
                 retry_count: int = 0, max_retries: int = 3) -> Dict[str, Any]:
        """Uniwersalna metoda do wykonywania requestów do API Zoho."""
        self.logger.info("→ API Request: %s %s", method, url)
        if data:
            self.logger.debug("  Payload: %s", data.decode("utf-8")[:500])
        
        headers = {"Authorization": f"Zoho-oauthtoken {self.access_token}"}
        if data:
            headers["Content-Type"] = "application/json"
        
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        time.sleep(0.5)  # Rate limiting
        
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                status = response.status
                response_data = response.read().decode("utf-8")
                
                self.logger.info("← API Response: status=%d, data_length=%d", 
                                status, len(response_data) if response_data else 0)
                self.logger.debug("  Response preview: %s", response_data[:200] if response_data else "(puste)")
                
                if status == 204:
                    self.logger.info("  ✓ Brak wyników (204)")
                    return {"data": []}
                
                if not response_data or not response_data.strip():
                    if status == 200:
                        self.logger.info("  ✓ Pusta odpowiedź (200)")
                        return {"data": []}
                    else:
                        raise RuntimeError(f"Pusta odpowiedź z API Zoho (status: {status})")
                
                try:
                    parsed = json.loads(response_data)
                    if isinstance(parsed, dict):
                        data_count = len(parsed.get("data", []))
                        self.logger.info("  ✓ Sparsowano JSON: %d rekordów", data_count)
                    return parsed
                except json.JSONDecodeError as json_err:
                    self.logger.error("  ✗ Błąd parsowania JSON: %s", json_err)
                    raise RuntimeError(f"Niepoprawna odpowiedź JSON: {json_err}") from json_err
                    
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
            self.logger.error("HTTPError %s dla URL %s: %s", exc.code, url, body)
            error_detail = body
            try:
                if body:
                    error_json = json.loads(body)
                    error_msg = error_json.get("message", error_json.get("error", body))
                    error_detail = f"{error_msg} (code: {exc.code})"
            except (json.JSONDecodeError, AttributeError):
                pass
            raise RuntimeError(f"Błąd HTTP {exc.code}: {error_detail}") from exc
            
        except urllib.error.URLError as url_err:
            if retry_count < max_retries:
                wait_time = (retry_count + 1) * 2
                self.logger.warning("URLError (próba %d/%d): %s - ponawiam za %ds...",
                                  retry_count + 1, max_retries, url_err, wait_time)
                time.sleep(wait_time)
                return self._request(url, method, data, retry_count + 1, max_retries)
            else:
                self.logger.error("URLError po %d próbach: %s", max_retries, url_err)
                raise RuntimeError(f"Błąd połączenia z API Zoho: {url_err}") from url_err

    def _get(self, url: str, retry_count: int = 0, max_retries: int = 3) -> Dict[str, Any]:
        """GET request."""
        return self._request(url, "GET", None, retry_count, max_retries)
    
    def _put(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """PUT request."""
        data = json.dumps(payload).encode("utf-8")
        return self._request(url, "PUT", data)
    
    def _post(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """POST request."""
        data = json.dumps(payload).encode("utf-8")
        return self._request(url, "POST", data)
    
    def _delete(self, url: str) -> Dict[str, Any]:
        """DELETE request."""
        return self._request(url, "DELETE")

    # === CONTACTS ===
    
    def get_all_contacts(self, per_page: int = 200) -> List[Dict[str, Any]]:
        """Pobiera wszystkie kontakty z Zoho (paginacja automatyczna z page_token)."""
        self.logger.info("📥 Rozpoczynam pobieranie wszystkich kontaktów...")
        
        # Lista wszystkich potrzebnych pól dla Contacts
        fields = [
            "id", "First_Name", "Last_Name", "Account_Name",
            "Email", "Secondary_Email", "Email_3",
            "Mobile", "Home_Phone", "Telefon_komorkowy_3",
            "Phone", "Other_Phone", "Telefon_stacjonarny_3",
            "NIP", "Tag"
        ]
        fields_param = ",".join(fields)
        
        all_contacts = []
        page_token = None
        page_num = 1
        
        while True:
            self.logger.info("  Pobieranie strony %d (per_page=%d)...", page_num, per_page)
            
            # Zoho CRM v3: użyj page_token dla rekordów > 2000, inaczej page
            if page_token:
                url = f"{self.BASE_URL}/Contacts?fields={fields_param}&per_page={per_page}&page_token={page_token}"
            else:
                url = f"{self.BASE_URL}/Contacts?fields={fields_param}&per_page={per_page}&page={page_num}"
            
            raw = self._get(url)
            
            records = raw.get("data", [])
            if not records:
                self.logger.info("  Brak więcej rekordów - koniec paginacji")
                break
            
            all_contacts.extend(records)
            self.logger.info("  → Pobrano %d rekordów (łącznie: %d)", len(records), len(all_contacts))
            
            # Sprawdź czy są kolejne strony
            info = raw.get("info", {})
            if not info.get("more_records", False):
                self.logger.info("  Brak więcej stron (more_records=false)")
                break
            
            # Pobierz next_page_token dla kolejnej strony
            page_token = info.get("next_page_token")
            if not page_token:
                self.logger.warning("  more_records=true ale brak next_page_token - kończę")
                break
            
            page_num += 1
        
        self.logger.info("✓ Pobrano łącznie %d kontaktów", len(all_contacts))
        return all_contacts

    def search_contacts_by_criteria(self, criteria: str) -> List[str]:
        """Wyszukuje kontakty po kryterium i zwraca listę ID."""
        if not criteria:
            return []
        
        self.logger.info("🔍 Szukam kontaktów: %s", criteria)
        encoded = urllib.parse.quote(criteria)
        url = f"{self.BASE_URL}/Contacts/search?criteria=({encoded})"
        raw = self._get(url)
        records = raw.get("data") or []
        contact_ids = [record.get("id") for record in records if record.get("id")]
        self.logger.info("  → Znaleziono %d kontaktów", len(contact_ids))
        return contact_ids

    def get_contact_by_id(self, contact_id: str) -> Optional[Dict[str, Any]]:
        """Pobiera szczegóły kontaktu po ID."""
        self.logger.info("📥 Pobieram kontakt ID: %s", contact_id)
        url = f"{self.BASE_URL}/Contacts/{contact_id}"
        raw = self._get(url)
        data = raw.get("data")
        if not data:
            return None
        return data[0]

    def update_contact(self, contact_id: str, update_data: Dict[str, Any]) -> bool:
        """Aktualizuje kontakt w Zoho CRM."""
        self.logger.info("📝 Aktualizuję kontakt ID: %s", contact_id)
        url = f"{self.BASE_URL}/Contacts/{contact_id}"
        payload = {"data": [update_data]}
        
        try:
            parsed = self._put(url, payload)
            if parsed.get("data", [{}])[0].get("code") == "SUCCESS":
                self.logger.info("  ✅ Kontakt zaktualizowany pomyślnie")
                return True
            else:
                self.logger.error("  ✗ Błąd aktualizacji: %s", parsed)
                return False
        except Exception as exc:
            self.logger.error("  ✗ Błąd podczas aktualizacji: %s", exc)
            return False

    # === ACCOUNTS ===
    
    def get_all_accounts(self, per_page: int = 200) -> List[Dict[str, Any]]:
        """Pobiera wszystkie firmy z Zoho (paginacja automatyczna z page_token)."""
        self.logger.info("📥 Rozpoczynam pobieranie wszystkich firm...")
        
        # Lista wszystkich potrzebnych pól dla Accounts
        fields = [
            "id", "Account_Name", "Adres_w_rekordzie", "Nazwa_zwyczajowa", "Nazwa_handlowa_szyld",
            "Firma_NIP", "Firma_REGON", "Firma_KRS", "Status_REGON", "Domena_z_www", "Website",
            "Billing_Street", "Billing_Code", "Billing_City", "Billing_Gmina", "Billing_Powiat", "Billing_State", "Billing_Country",
            "Shipping_Street", "Shipping_Code", "Shipping_City", "Shipping_Gmina", "Shipping_Powiat", "Shipping_State", "Shipping_Country",
            "Mobile_phone_1", "Mobile_phone_2", "Mobile_phone_3",
            "Phone", "Phone_2", "Phone_3",
            "Firma_EMAIL1", "Firma_EMAIL2", "Firma_EMAIL3",
            "Parent_Account", "GROUP_1", "GROUP_2", "GROUP_3", "Tag"
        ]
        fields_param = ",".join(fields)
        
        all_accounts = []
        page_token = None
        page_num = 1
        
        while True:
            self.logger.info("  Pobieranie strony %d (per_page=%d)...", page_num, per_page)
            
            # Zoho CRM v3: użyj page_token dla rekordów > 2000, inaczej page
            if page_token:
                url = f"{self.BASE_URL}/Accounts?fields={fields_param}&per_page={per_page}&page_token={page_token}"
            else:
                url = f"{self.BASE_URL}/Accounts?fields={fields_param}&per_page={per_page}&page={page_num}"
            
            raw = self._get(url)
            
            records = raw.get("data", [])
            if not records:
                self.logger.info("  Brak więcej rekordów - koniec paginacji")
                break
            
            all_accounts.extend(records)
            self.logger.info("  → Pobrano %d rekordów (łącznie: %d)", len(records), len(all_accounts))
            
            info = raw.get("info", {})
            if not info.get("more_records", False):
                self.logger.info("  Brak więcej stron (more_records=false)")
                break
            
            # Pobierz next_page_token dla kolejnej strony
            page_token = info.get("next_page_token")
            if not page_token:
                self.logger.warning("  more_records=true ale brak next_page_token - kończę")
                break
            
            page_num += 1
        
        self.logger.info("✓ Pobrano łącznie %d firm", len(all_accounts))
        return all_accounts

    def search_accounts_by_criteria(self, criteria: str) -> List[Dict[str, Any]]:
        """Wyszukuje firmy po kryterium i zwraca pełne rekordy."""
        if not criteria:
            return []
        
        self.logger.info("🔍 Szukam firm: %s", criteria)
        encoded = urllib.parse.quote(criteria)
        url = f"{self.BASE_URL}/Accounts/search?criteria=({encoded})"
        raw = self._get(url)
        records = raw.get("data", [])
        self.logger.info("  → Znaleziono %d firm", len(records))
        return records

    def get_account_by_id(self, account_id: str) -> Optional[Dict[str, Any]]:
        """Pobiera szczegóły firmy po ID."""
        self.logger.info("📥 Pobieram firmę ID: %s", account_id)
        url = f"{self.BASE_URL}/Accounts/{account_id}"
        raw = self._get(url)
        data = raw.get("data")
        if not data:
            return None
        return data[0]

    def update_account(self, account_id: str, update_data: Dict[str, Any]) -> bool:
        """Aktualizuje firmę w Zoho CRM."""
        self.logger.info("📝 Aktualizuję firmę ID: %s", account_id)
        url = f"{self.BASE_URL}/Accounts/{account_id}"
        payload = {"data": [update_data]}
        
        try:
            parsed = self._put(url, payload)
            if parsed.get("data", [{}])[0].get("code") == "SUCCESS":
                self.logger.info("  ✅ Firma zaktualizowana pomyślnie")
                return True
            else:
                self.logger.error("  ✗ Błąd aktualizacji: %s", parsed)
                return False
        except Exception as exc:
            self.logger.error("  ✗ Błąd podczas aktualizacji: %s", exc)
            return False

    def delete_account(self, account_id: str) -> bool:
        """Usuwa firmę z Zoho CRM."""
        self.logger.info("🗑️  Usuwam firmę ID: %s", account_id)
        url = f"{self.BASE_URL}/Accounts/{account_id}"
        
        try:
            parsed = self._delete(url)
            if parsed.get("data", [{}])[0].get("code") == "SUCCESS":
                self.logger.info("  ✅ Firma usunięta pomyślnie")
                return True
            else:
                self.logger.error("  ✗ Błąd usuwania: %s", parsed)
                return False
        except Exception as exc:
            self.logger.error("  ✗ Błąd podczas usuwania: %s", exc)
            return False
    
    def get_account_tags(self, account_id: str) -> List[str]:
        """Pobiera tagi przypisane do firmy."""
        self.logger.debug("🏷️  Pobieram tagi dla firmy ID: %s", account_id)
        
        try:
            account = self.get_account_by_id(account_id)
            if not account:
                return []
            
            tags = account.get("Tag", [])
            if not tags:
                return []
            
            # Tag może być listą dict z 'name' lub prostą listą stringów
            tag_names = []
            if isinstance(tags, list):
                for tag in tags:
                    if isinstance(tag, dict):
                        tag_name = tag.get("name")
                        if tag_name:
                            tag_names.append(tag_name)
                    elif isinstance(tag, str):
                        tag_names.append(tag)
            
            self.logger.debug("  → Znaleziono %d tagów: %s", len(tag_names), tag_names)
            return tag_names
        
        except Exception as exc:
            self.logger.warning("  Błąd pobierania tagów: %s", exc)
            return []
    
    def add_tags_to_account(self, account_id: str, tag_names: List[str]) -> bool:
        """Dodaje tagi do firmy."""
        if not tag_names:
            return True
        
        self.logger.info("🏷️  Dodaję %d tagów do firmy ID: %s", len(tag_names), account_id)
        self.logger.debug("  Tagi: %s", tag_names)
        
        url = f"{self.BASE_URL}/Accounts/{account_id}/actions/add_tags"
        payload = {
            "tags": [{"name": tag_name} for tag_name in tag_names]
        }
        
        try:
            parsed = self._post(url, payload)
            if parsed.get("data", [{}])[0].get("code") == "SUCCESS":
                self.logger.info("  ✅ Tagi dodane pomyślnie")
                return True
            else:
                self.logger.error("  ✗ Błąd dodawania tagów: %s", parsed)
                return False
        except Exception as exc:
            self.logger.error("  ✗ Błąd podczas dodawania tagów: %s", exc)
            return False

    # === MODUŁY POWIĄZANE ===
    
    def get_related_records(self, module_name: str, parent_module: str, parent_id: str) -> List[Dict[str, Any]]:
        """Pobiera powiązane rekordy z innego modułu."""
        self.logger.info("🔗 Pobieram powiązane rekordy: %s -> %s (ID: %s)", parent_module, module_name, parent_id)
        
        # Minimalna lista pól (ID wystarczy do zliczenia)
        url = f"{self.BASE_URL}/{parent_module}/{parent_id}/{module_name}?fields=id"
        
        try:
            raw = self._get(url)
            records = raw.get("data", [])
            self.logger.info("  → Znaleziono %d powiązanych rekordów", len(records))
            return records
        except Exception as exc:
            # Jeśli błąd (np. brak uprawnień do Notes), zwróć pustą listę
            self.logger.debug("  → Błąd pobierania powiązanych rekordów: %s", exc)
            return []

    def search_records_by_criteria(self, module_name: str, criteria: str) -> List[Dict[str, Any]]:
        """Uniwersalne wyszukiwanie rekordów w dowolnym module."""
        if not criteria:
            return []
        
        self.logger.info("🔍 Szukam w %s: %s", module_name, criteria)
        encoded = urllib.parse.quote(criteria)
        url = f"{self.BASE_URL}/{module_name}/search?criteria=({encoded})"
        raw = self._get(url)
        records = raw.get("data", [])
        self.logger.info("  → Znaleziono %d rekordów", len(records))
        return records

    def test_connection(self) -> bool:
        """Testuje połączenie z API Zoho."""
        try:
            # Prosty test - pobierz 1 rekord z Accounts
            url = f"{self.BASE_URL}/Accounts?fields=id,Account_Name&per_page=1"
            raw = self._get(url)
            self.logger.info("✓ Test połączenia: Sukces - API Zoho działa poprawnie")
            return True
        except Exception as exc:
            self.logger.error("✗ Test połączenia: Błąd - %s", exc)
            return False

