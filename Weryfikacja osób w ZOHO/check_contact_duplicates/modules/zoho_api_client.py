"""Minimalny klient Zoho CRM wykorzystywany przez skrypt duplikujący."""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
import urllib.parse
from typing import Any, Dict, List


class ZohoAPIClient:
    """Zawija wywołania REST do modułu Contacts."""

    BASE_URL = "https://www.zohoapis.eu/crm/v3"

    def __init__(self, access_token: str) -> None:
        self.access_token = access_token
        self.logger = logging.getLogger(__name__)

    def _get(self, url: str, retry_count: int = 0, max_retries: int = 3) -> Dict[str, Any]:
        # Loguj każde zapytanie
        self.logger.info("→ API Request: GET %s", url)
        
        req = urllib.request.Request(
            url,
            headers={"Authorization": f"Zoho-oauthtoken {self.access_token}"},
            method="GET",
        )
        # Zwiększone opóźnienie dla rate limiting
        time.sleep(0.5)
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                status = response.status
                data = response.read().decode("utf-8")
                
                # Loguj odpowiedź (skrócony podgląd)
                data_preview = data[:200] if data else "(puste)"
                self.logger.info("← API Response: status=%d, data_length=%d, preview=%s", 
                                status, len(data) if data else 0, data_preview)
                
                # Status 204 (No Content) = brak wyników, to normalna sytuacja
                if status == 204:
                    self.logger.info("  ✓ Brak wyników (204) - zwracam pustą listę")
                    return {"data": []}
                
                # Sprawdź czy odpowiedź nie jest pusta (dla innych statusów)
                if not data or not data.strip():
                    if status == 200:
                        # Status 200 z pustą odpowiedzią też może oznaczać brak wyników
                        self.logger.info("  ✓ Pusta odpowiedź (200) - zwracam pustą listę")
                        return {"data": []}
                    else:
                        self.logger.error("  ✗ Pusta odpowiedź z nieoczekiwanym statusem: %s", status)
                        raise RuntimeError(f"Pusta odpowiedź z API Zoho (status: {status})")
                
                # Spróbuj sparsować JSON
                try:
                    parsed = json.loads(data)
                    # Loguj szczegóły odpowiedzi
                    if isinstance(parsed, dict):
                        data_count = len(parsed.get("data", []))
                        self.logger.info("  ✓ Sparsowano JSON: %d rekordów", data_count)
                    return parsed
                except json.JSONDecodeError as json_err:
                    self.logger.error("  ✗ Błąd parsowania JSON: %s, dane: %s", json_err, data[:500])
                    raise RuntimeError(
                        f"Niepoprawna odpowiedź JSON z API Zoho: {json_err}. "
                        f"Odpowiedź: {data[:200]}"
                    ) from json_err
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
            self.logger.error("HTTPError %s dla URL %s: %s", exc.code, url, body)
            # Spróbuj sparsować błąd jako JSON jeśli to możliwe
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
            # Retry dla błędów timeout/połączenia
            if retry_count < max_retries:
                wait_time = (retry_count + 1) * 2  # 2s, 4s, 6s
                self.logger.warning(
                    "URLError dla URL %s (próba %d/%d): %s - ponawiam za %ds...",
                    url, retry_count + 1, max_retries, url_err, wait_time
                )
                time.sleep(wait_time)
                return self._get(url, retry_count + 1, max_retries)
            else:
                self.logger.error("URLError dla URL %s po %d próbach: %s", url, max_retries, url_err)
                raise RuntimeError(f"Błąd połączenia z API Zoho po {max_retries} próbach: {url_err}") from url_err

    def search_contacts_by_criteria(self, criteria: str) -> List[str]:
        if not criteria:
            self.logger.info("search_contacts_by_criteria: puste kryterium, zwracam []")
            return []
        
        self.logger.info("🔍 Szukam kontaktów według kryterium: %s", criteria)
        encoded = urllib.parse.quote(criteria)
        url = f"{self.BASE_URL}/Contacts/search?criteria=({encoded})"
        raw = self._get(url)
        records = raw.get("data") or []
        contact_ids = [record.get("id") for record in records if record.get("id")]
        
        self.logger.info("  → Znaleziono %d kontaktów: %s", len(contact_ids), contact_ids)
        return contact_ids

    def get_contact_by_id(self, contact_id: str) -> Dict[str, Any] | None:
        self.logger.info("📥 Pobieram szczegóły kontaktu ID: %s", contact_id)
        url = f"{self.BASE_URL}/Contacts/{contact_id}"
        raw = self._get(url)
        data = raw.get("data")
        if not data:
            self.logger.warning("  → Brak danych dla kontaktu %s", contact_id)
            return None
        contact = data[0]
        self.logger.info("  → Pobrano: %s %s (email: %s)", 
                        contact.get("First_Name", ""), 
                        contact.get("Last_Name", ""),
                        contact.get("Email", "brak"))
        return contact

    def update_contact(self, contact_id: str, update_data: Dict[str, Any]) -> bool:
        """Aktualizuje kontakt w Zoho CRM."""
        self.logger.info("📝 Aktualizuję kontakt ID: %s", contact_id)
        self.logger.info("  Dane do aktualizacji: %s", update_data)
        
        url = f"{self.BASE_URL}/Contacts/{contact_id}"
        
        # Przygotuj request PUT
        import json as json_lib
        payload = json_lib.dumps({"data": [update_data]}).encode("utf-8")
        
        req = urllib.request.Request(
            url,
            data=payload,
            headers={
                "Authorization": f"Zoho-oauthtoken {self.access_token}",
                "Content-Type": "application/json"
            },
            method="PUT",
        )
        
        time.sleep(0.5)
        
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                status = response.status
                data = response.read().decode("utf-8")
                parsed = json.loads(data)
                
                self.logger.info("← Update Response: status=%d", status)
                self.logger.info("  Odpowiedź: %s", parsed)
                
                # Sprawdź sukces
                if parsed.get("data", [{}])[0].get("code") == "SUCCESS":
                    self.logger.info("  ✅ Kontakt zaktualizowany pomyślnie")
                    return True
                else:
                    self.logger.error("  ✗ Błąd aktualizacji: %s", parsed)
                    return False
                    
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
            self.logger.error("HTTPError %s podczas aktualizacji kontaktu %s: %s", exc.code, contact_id, body)
            return False
        except Exception as exc:
            self.logger.error("Błąd podczas aktualizacji kontaktu %s: %s", contact_id, exc)
            return False

    def get_field_metadata(self, module_name: str = "Contacts") -> Dict[str, Any]:
        """Pobiera metadane pól modułu (typy, dozwolone wartości, itd.)."""
        self.logger.info("📋 Pobieram metadane pól modułu: %s", module_name)
        url = f"{self.BASE_URL}/settings/fields?module={module_name}"
        raw = self._get(url)
        
        fields = raw.get("fields", [])
        self.logger.info("  → Pobrano informacje o %d polach", len(fields))
        return raw

    def get_picklist_values(self, field_api_name: str, module_name: str = "Contacts") -> List[str]:
        """Pobiera dozwolone wartości dla pola typu picklist."""
        self.logger.info("📝 Pobieram dozwolone wartości dla pola: %s", field_api_name)
        
        metadata = self.get_field_metadata(module_name)
        fields = metadata.get("fields", [])
        
        for field in fields:
            if field.get("api_name") == field_api_name:
                pick_list_values = field.get("pick_list_values", [])
                if pick_list_values:
                    values = [item.get("actual_value") or item.get("display_value") 
                             for item in pick_list_values]
                    self.logger.info("  → Znaleziono %d dozwolonych wartości", len(values))
                    return values
                else:
                    self.logger.warning("  → Pole %s nie ma zdefiniowanych wartości", field_api_name)
                    return []
        
        self.logger.error("  → Nie znaleziono pola: %s", field_api_name)
        return []

    def get_lookup_records(self, module_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Pobiera rekordy z modułu (dla pól lookup)."""
        self.logger.info("🔗 Pobieram rekordy z modułu: %s (limit: %d)", module_name, limit)
        url = f"{self.BASE_URL}/{module_name}?per_page={limit}"
        raw = self._get(url)
        
        records = raw.get("data", [])
        self.logger.info("  → Pobrano %d rekordów", len(records))
        return records

    def get_tags(self, module_name: str = "Contacts") -> List[str]:
        """Pobiera listę istniejących TAGów dla modułu."""
        self.logger.info("🏷️  Pobieram TAGi dla modułu: %s", module_name)
        url = f"{self.BASE_URL}/settings/tags?module={module_name}"
        
        try:
            raw = self._get(url)
            tags_data = raw.get("tags", [])
            tags = [tag.get("name") for tag in tags_data if tag.get("name")]
            self.logger.info("  → Pobrano %d TAGów", len(tags))
            return tags
        except Exception as exc:
            self.logger.error("Błąd pobierania TAGów: %s", exc)
            return []

    def create_contact(self, contact_data: Dict[str, Any]) -> Optional[str]:
        """Tworzy nowy kontakt w Zoho CRM. Zwraca ID utworzonego kontaktu lub None."""
        self.logger.info("➕ Tworzę nowy kontakt w Zoho")
        self.logger.info("  Dane kontaktu: %s", contact_data)
        
        url = f"{self.BASE_URL}/Contacts"
        
        import json as json_lib
        payload = json_lib.dumps({"data": [contact_data]}).encode("utf-8")
        
        req = urllib.request.Request(
            url,
            data=payload,
            headers={
                "Authorization": f"Zoho-oauthtoken {self.access_token}",
                "Content-Type": "application/json"
            },
            method="POST",
        )
        
        time.sleep(0.5)
        
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                status = response.status
                data = response.read().decode("utf-8")
                parsed = json.loads(data)
                
                self.logger.info("← Create Response: status=%d", status)
                self.logger.info("  Odpowiedź: %s", parsed)
                
                # Sprawdź sukces
                result = parsed.get("data", [{}])[0]
                if result.get("code") == "SUCCESS":
                    created_id = result.get("details", {}).get("id")
                    self.logger.info("  ✅ Kontakt utworzony pomyślnie (ID: %s)", created_id)
                    return created_id
                else:
                    self.logger.error("  ✗ Błąd tworzenia: %s", parsed)
                    return None
                    
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
            self.logger.error("HTTPError %s podczas tworzenia kontaktu: %s", exc.code, body)
            return None
        except Exception as exc:
            self.logger.error("Błąd podczas tworzenia kontaktu: %s", exc)
            return None

    def add_tags_to_contact(self, contact_id: str, tag_names: List[str]) -> bool:
        """Dodaje TAGi do kontaktu używając dedykowanego endpointa."""
        self.logger.info("🏷️  Dodaję TAGi do kontaktu ID: %s", contact_id)
        self.logger.info("  TAGi: %s", tag_names)
        
        url = f"{self.BASE_URL}/Contacts/{contact_id}/actions/add_tags"
        
        # Payload: lista tagów (tylko nazwy)
        import json as json_lib
        payload = json_lib.dumps({
            "tags": [{"name": tag_name} for tag_name in tag_names]
        }).encode("utf-8")
        
        req = urllib.request.Request(
            url,
            data=payload,
            headers={
                "Authorization": f"Zoho-oauthtoken {self.access_token}",
                "Content-Type": "application/json"
            },
            method="POST",
        )
        
        time.sleep(0.5)
        
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                status = response.status
                data = response.read().decode("utf-8")
                parsed = json.loads(data)
                
                self.logger.info("← Add Tags Response: status=%d", status)
                self.logger.info("  Odpowiedź: %s", parsed)
                
                # Sprawdź sukces
                if parsed.get("data", [{}])[0].get("code") == "SUCCESS":
                    self.logger.info("  ✅ TAGi dodane pomyślnie")
                    return True
                else:
                    self.logger.error("  ✗ Błąd dodawania TAGów: %s", parsed)
                    return False
                    
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
            self.logger.error("HTTPError %s podczas dodawania TAGów do %s: %s", exc.code, contact_id, body)
            return False
        except Exception as exc:
            self.logger.error("Błąd podczas dodawania TAGów do %s: %s", contact_id, exc)
            return False

    def test_connection(self) -> bool:
        """Testuje połączenie z API Zoho - sprawdza czy token działa."""
        try:
            # Użyj endpoint /users/actions/check który sprawdza tylko autentykację
            # Lub po prostu spróbuj pobrać organizację - prosty endpoint bez parametrów
            url = f"{self.BASE_URL}/org"
            raw = self._get(url)
            # Jeśli dotarliśmy tutaj bez błędu, połączenie działa
            org_name = raw.get("org", [{}])[0].get("company_name", "Nieznana")
            self.logger.info("✓ Test połączenia: Sukces - Połączono z organizacją: %s", org_name)
            self.logger.debug("Odpowiedź testowa: status OK, format JSON poprawny")
            return True
        except Exception as exc:
            self.logger.error("✗ Test połączenia: Błąd - %s", exc)
            # Nie przerywaj - jeśli token się odświeżył, prawdopodobnie działa
            self.logger.warning("Test połączenia nie powiódł się, ale kontynuuję (token został odświeżony)")
            return False
