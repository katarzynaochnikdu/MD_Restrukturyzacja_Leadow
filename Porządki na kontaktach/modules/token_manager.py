"""Zarządzanie tokenem dostępu Zoho z cache."""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional


ACCOUNTS_TOKEN_URL = "https://accounts.zoho.eu/oauth/v2/token"
TOKEN_CACHE_FILE = ".zoho_token_cache.json"
TOKEN_LOCK_FILE = ".zoho_token_cache.lock"


class TokenManager:
    """Zarządza tokenem dostępu Zoho z cache w pliku."""
    
    def __init__(self, config_path: str = "Referencyjne/config.json") -> None:
        self.logger = logging.getLogger(__name__)
        self.config_path = config_path
        self.cache_path = TOKEN_CACHE_FILE
        
        # Wczytaj credentials z config.json
        self.client_id: Optional[str] = None
        self.client_secret: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self._load_config()
        
        # Cache tokenu
        self.access_token: Optional[str] = None
        self.token_expires_at: float = 0
    
    def _load_config(self) -> None:
        """Wczytuje credentials - priorytet: zmienne środowiskowe, potem config.json."""
        self.logger.info("Wczytuję konfigurację...")
        
        # PRIORYTET 1: Zmienne środowiskowe
        self.client_id = os.environ.get("ZOHO_MEDIDESK_CLIENT_ID")
        self.client_secret = os.environ.get("ZOHO_MEDIDESK_CLIENT_SECRET")
        self.refresh_token = os.environ.get("ZOHO_MEDIDESK_REFRESH_TOKEN")
        
        if self.client_id and self.client_secret and self.refresh_token:
            self.logger.info("✓ Wczytano credentials ze zmiennych środowiskowych")
            self.logger.debug("  ZOHO_MEDIDESK_CLIENT_ID: ✓")
            self.logger.debug("  ZOHO_MEDIDESK_CLIENT_SECRET: ✓")
            self.logger.debug("  ZOHO_MEDIDESK_REFRESH_TOKEN: ✓")
            return
        
        # PRIORYTET 2: Plik config.json
        self.logger.info("Zmienne środowiskowe niekompletne, sprawdzam config.json: %s", self.config_path)
        
        if not os.path.exists(self.config_path):
            self.logger.warning("⚠ Plik konfiguracji nie istnieje: %s", self.config_path)
            self.logger.info("Ustaw zmienne środowiskowe:")
            self.logger.info("  set ZOHO_MEDIDESK_CLIENT_ID=twoj_client_id")
            self.logger.info("  set ZOHO_MEDIDESK_CLIENT_SECRET=twoj_client_secret")
            self.logger.info("  set ZOHO_MEDIDESK_REFRESH_TOKEN=twoj_refresh_token")
            return
        
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                config = json.load(f)
            
            zoho_config = config.get("zoho", {})
            
            # Użyj z config.json tylko jeśli nie ma w zmiennych środowiskowych
            if not self.client_id:
                self.client_id = zoho_config.get("client_id")
            if not self.client_secret:
                self.client_secret = zoho_config.get("client_secret")
            if not self.refresh_token:
                self.refresh_token = zoho_config.get("refresh_token")
            
            if self.client_id and self.client_secret and self.refresh_token:
                self.logger.info("✓ Wczytano credentials z config.json")
            else:
                self.logger.warning("⚠ Niepełne credentials")
                self.logger.debug("  client_id: %s", "✓" if self.client_id else "✗")
                self.logger.debug("  client_secret: %s", "✓" if self.client_secret else "✗")
                self.logger.debug("  refresh_token: %s", "✓" if self.refresh_token else "✗")
                self.logger.info("")
                self.logger.info("Ustaw credentials poprzez:")
                self.logger.info("1. Zmienne środowiskowe (ZALECANE):")
                self.logger.info("   set ZOHO_MEDIDESK_CLIENT_ID=...")
                self.logger.info("   set ZOHO_MEDIDESK_CLIENT_SECRET=...")
                self.logger.info("   set ZOHO_MEDIDESK_REFRESH_TOKEN=...")
                self.logger.info("2. Lub plik Referencyjne/config.json")
        
        except Exception as exc:
            self.logger.error("Błąd wczytywania konfiguracji: %s", exc)
    
    def _load_token_cache(self) -> bool:
        """Wczytuje token z pliku cache (bezpieczne dla równoległych procesów)."""
        if not os.path.exists(self.cache_path):
            self.logger.debug("Brak pliku cache tokenu: %s", self.cache_path)
            return False
        
        try:
            with open(self.cache_path, "r", encoding="utf-8") as f:
                cache = json.load(f)
            
            self.access_token = cache.get("access_token")
            self.token_expires_at = cache.get("expires_at", 0)
            pid = cache.get("pid", "unknown")
            
            if self.access_token and time.time() < self.token_expires_at:
                self.logger.info("✓ Wczytano token z cache (ważny do: %s, utworzony przez PID: %s)", 
                               time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.token_expires_at)),
                               pid)
                self.logger.debug("  ⚡ OSZCZĘDNOŚĆ: Pominięto odświeżanie tokenu (współdzielony cache)")
                return True
            else:
                self.logger.debug("Token w cache wygasł lub jest nieprawidłowy")
                return False
        
        except Exception as exc:
            self.logger.warning("Błąd wczytywania cache tokenu: %s", exc)
            return False
    
    def _save_token_cache(self) -> None:
        """Zapisuje token do pliku cache (z lock file dla bezpieczeństwa)."""
        try:
            cache = {
                "access_token": self.access_token,
                "expires_at": self.token_expires_at,
                "updated_at": time.time(),
                "pid": os.getpid()  # Process ID dla debugowania
            }
            
            with open(self.cache_path, "w", encoding="utf-8") as f:
                json.dump(cache, f, indent=2)
            
            self.logger.info("✓ Zapisano token do cache: %s", self.cache_path)
        
        except Exception as exc:
            self.logger.error("Błąd zapisywania cache tokenu: %s", exc)
    
    def _refresh_access_token(self) -> str:
        """Odświeża access token używając refresh token."""
        if not all([self.client_id, self.client_secret, self.refresh_token]):
            raise RuntimeError(
                "Brak credentials w config.json. "
                "Uzupełnij pola: zoho.client_id, zoho.client_secret, zoho.refresh_token"
            )
        
        self.logger.info("🔄 Odświeżam access token...")
        
        payload = urllib.parse.urlencode({
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
        }).encode("utf-8")
        
        request = urllib.request.Request(
            ACCOUNTS_TOKEN_URL,
            data=payload,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                data = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
            self.logger.error("Błąd HTTP podczas odświeżania tokenu: %s - %s", exc.code, body)
            raise RuntimeError(f"Błąd HTTP {exc.code}: {body}") from exc
        
        payload_json = json.loads(data)
        if "access_token" not in payload_json:
            raise RuntimeError(f"Brak 'access_token' w odpowiedzi: {data}")
        
        self.access_token = payload_json["access_token"]
        # Token ważny przez 1 godzinę (3600s), zapisujemy z marginesem 5 min
        self.token_expires_at = time.time() + 3300
        
        self.logger.info("✓ Token odświeżony pomyślnie (ważny do: %s)", 
                        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.token_expires_at)))
        
        # Zapisz do cache
        self._save_token_cache()
        
        return self.access_token
    
    def get_access_token(self, force_refresh: bool = False) -> str:
        """Zwraca access token (z cache lub odświeża jeśli potrzeba).
        
        Args:
            force_refresh: Wymusza odświeżenie tokenu (ignoruje cache)
        
        Returns:
            Access token gotowy do użycia
        """
        # Jeśli wymuszamy refresh, od razu odśwież
        if force_refresh:
            self.logger.info("Wymuszono odświeżenie tokenu")
            return self._refresh_access_token()
        
        # Spróbuj wczytać z cache jeśli nie mamy tokenu
        if not self.access_token or time.time() >= self.token_expires_at:
            if self._load_token_cache():
                # UWAGA: Sprawdź czy wczytany token nie jest zbyt stary
                # (dodatkowe bezpieczeństwo dla równoległych procesów)
                time_left = self.token_expires_at - time.time()
                if time_left > 300:  # Przynajmniej 5 minut ważności
                    return self.access_token
                else:
                    self.logger.warning("Token z cache wygasa za %d sekund - odświeżam dla bezpieczeństwa", int(time_left))
                    return self._refresh_access_token()
        
        # Token w pamięci jest ważny
        if self.access_token and time.time() < self.token_expires_at:
            return self.access_token
        
        # Token wygasł lub nie istnieje - odśwież
        return self._refresh_access_token()
    
    def invalidate_token(self) -> None:
        """Unieważnia token (wymusza odświeżenie przy następnym użyciu)."""
        self.logger.info("Unieważniam token w cache")
        self.access_token = None
        self.token_expires_at = 0


def get_token(config_path: str = "Referencyjne/config.json", force_refresh: bool = False) -> str:
    """Funkcja pomocnicza - pobiera token (główny interface modułu).
    
    Args:
        config_path: Ścieżka do pliku config.json
        force_refresh: Wymusza odświeżenie tokenu
    
    Returns:
        Access token gotowy do użycia
    """
    manager = TokenManager(config_path)
    return manager.get_access_token(force_refresh)

