"""Wykrywanie zwrotu grzecznościowego (Pan/Pani) na podstawie imienia przy użyciu AI."""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional


class SalutationDetector:
    """Używa OpenAI API do określenia zwrotu grzecznościowego na podstawie imienia."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.api_key = os.getenv("API_KEY_OPENAI_medidesk")
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.cache: dict[str, str] = {}  # Cache dla już sprawdzonych imion
        
        if not self.api_key:
            self.logger.warning("Brak API_KEY_OPENAI_medidesk - wykrywanie Salutation wyłączone")

    def detect_salutation(self, first_name: Optional[str]) -> Optional[str]:
        """
        Określa zwrot grzecznościowy (Pan/Pani) na podstawie imienia.
        
        Args:
            first_name: Imię osoby
            
        Returns:
            "Pan", "Pani" lub None jeśli nie można określić
        """
        if not first_name or not self.api_key:
            return None
        
        # Sprawdź cache
        first_name_clean = first_name.strip()
        if first_name_clean in self.cache:
            cached = self.cache[first_name_clean]
            self.logger.debug("Salutation dla '%s' z cache: %s", first_name_clean, cached)
            return cached
        
        # Wywołaj OpenAI API
        try:
            salutation = self._call_openai(first_name_clean)
            self.cache[first_name_clean] = salutation
            self.logger.info("✓ Określono Salutation dla '%s': %s", first_name_clean, salutation or "brak")
            return salutation
        except Exception as exc:
            self.logger.error("Błąd wykrywania Salutation dla '%s': %s", first_name_clean, exc)
            return None

    def _call_openai(self, first_name: str) -> Optional[str]:
        """Wywołuje OpenAI API do określenia płci."""
        url = "https://api.openai.com/v1/chat/completions"
        
        system_prompt = (
            "Jesteś ekspertem od polskich imion. Twoim zadaniem jest określić "
            "czy podane imię jest męskie czy żeńskie. "
            "Odpowiedz TYLKO jednym słowem: 'Pan' dla imion męskich lub 'Pani' dla imion żeńskich. "
            "Jeśli imię może być użyte dla obu płci lub nie możesz określić, odpowiedz 'Nieznane'."
        )
        
        user_prompt = f"Imię: {first_name}"
        
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0.1,
            "max_tokens": 10
        }
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
        data = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(url, data=data, headers=headers, method="POST")
        
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                response_data = response.read().decode("utf-8")
                result = json.loads(response_data)
                
                content = result["choices"][0]["message"]["content"].strip()
                
                # Normalizuj odpowiedź
                if "pan" in content.lower() and "pani" not in content.lower():
                    return "Pan"
                elif "pani" in content.lower():
                    return "Pani"
                else:
                    self.logger.debug("Niejednoznaczna odpowiedź AI dla '%s': %s", first_name, content)
                    return None
                    
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
            self.logger.error("HTTPError %s z OpenAI API: %s", exc.code, body)
            raise
        except Exception as exc:
            self.logger.error("Błąd wywołania OpenAI API: %s", exc)
            raise

