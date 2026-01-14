"""Sprawdzanie duplikatów kontaktów w Zoho CRM."""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from tqdm import tqdm

from modules.data_sanitizer import has_value, DataSanitizer
from modules.duplicate_finder import DuplicateFinder
from modules.contact_scorer import ContactScorer
from modules.zoho_api_client import ZohoAPIClient
from modules.salutation_detector import SalutationDetector
from modules.company_matcher import CompanyMatcher
from refresh_zoho_access_token import refresh_access_token


def setup_logging(log_path: Path, verbose: bool = False) -> None:
    log_level = logging.DEBUG if verbose else logging.INFO
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logging.basicConfig(level=log_level, handlers=[file_handler])
    # Konsola tylko dla błędów
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.ERROR)
    console.setFormatter(logging.Formatter("❌ %(message)s"))
    logging.getLogger().addHandler(console)


def clean_path(path: str) -> str:
    """Czyści ścieżkę z znaków PowerShell (&) i cudzysłowów."""
    cleaned = path.strip()
    # Usuń & z początku (z opcjonalnymi białymi znakami)
    if cleaned.startswith("&"):
        cleaned = cleaned[1:].strip()
    # Usuń otaczające cudzysłowy - sprawdź czy są sparowane
    if len(cleaned) >= 2:
        if (cleaned[0] == '"' and cleaned[-1] == '"') or (cleaned[0] == "'" and cleaned[-1] == "'"):
            cleaned = cleaned[1:-1].strip()
    # Jeśli nadal są pojedyncze cudzysłowy na początku/końcu, usuń je
    if cleaned and (cleaned[0] in ('"', "'")):
        cleaned = cleaned[1:].strip()
    if cleaned and (cleaned[-1] in ('"', "'")):
        cleaned = cleaned[:-1].strip()
    return cleaned.strip()


def load_config(config_file: str = "config.json") -> Dict[str, Any]:
    logger = logging.getLogger(__name__)
    config_path = Path(config_file)
    if not config_path.is_absolute():
        config_path = Path(__file__).parent / config_path
    if not config_path.exists():
        logger.error("Plik konfiguracji nie istnieje: %s", config_path)
        raise FileNotFoundError(f"Brak pliku: {config_path}")
    with config_path.open("r", encoding="utf-8") as fh:
        config = json.load(fh)
    logger.info("✓ Wczytano konfigurację z: %s", config_path)
    
    # Uzupełnij brakujące wartości z zmiennych środowiskowych
    zoho_conf = config.setdefault("zoho", {})
    env_map = {
        "client_id": "ZOHO_MEDIDESK_CLIENT_ID",
        "client_secret": "ZOHO_MEDIDESK_CLIENT_SECRET",
        "refresh_token": "ZOHO_MEDIDESK_REFRESH_TOKEN",
    }
    for key, env_var in env_map.items():
        value = zoho_conf.get(key, "")
        # Jeśli wartość jest placeholderem lub pusta, spróbuj ze zmiennej środowiskowej
        if not value or value.startswith(("TWOJ_", "WPISZ_")):
            env_value = os.environ.get(env_var)
            if env_value:
                zoho_conf[key] = env_value
                logger.info("✓ Użyto %s ze zmiennej środowiskowej", key)
    
    return config


def get_access_token(config: Dict[str, Any]) -> str:
    logger = logging.getLogger(__name__)
    logger.info("Odświeżanie tokena dostępu do Zoho API...")
    print("🔑 Odświeżanie tokena dostępu do Zoho API...")
    zoho_conf = config.get("zoho") or {}
    
    # Walidacja danych
    client_id = zoho_conf.get("client_id", "").strip()
    client_secret = zoho_conf.get("client_secret", "").strip()
    refresh_token = zoho_conf.get("refresh_token", "").strip()
    
    # Sprawdź czy są placeholdery
    placeholders = ["TWOJ_CLIENT_ID", "TWOJ_CLIENT_SECRET", "TWOJ_REFRESH_TOKEN", 
                    "WPISZ_CLIENT_ID", "WPISZ_CLIENT_SECRET", "WPISZ_REFRESH_TOKEN"]
    
    missing = []
    if not client_id or client_id in placeholders:
        missing.append("client_id")
    if not client_secret or client_secret in placeholders:
        missing.append("client_secret")
    if not refresh_token or refresh_token in placeholders:
        missing.append("refresh_token")
    
    if missing:
        raise RuntimeError(
            f"Brak konfiguracji Zoho: {', '.join(missing)}. "
            f"Uzupełnij config.json lub ustaw zmienne środowiskowe "
            f"(ZOHO_MEDIDESK_CLIENT_ID, ZOHO_MEDIDESK_CLIENT_SECRET, ZOHO_MEDIDESK_REFRESH_TOKEN)"
        )
    
    # Logowanie (bez wyświetlania wrażliwych danych)
    logger.info("Używam client_id: %s...", client_id[:8] + "..." if len(client_id) > 8 else "***")
    logger.debug("Refresh token: %s...", refresh_token[:20] + "..." if len(refresh_token) > 20 else "***")
    
    try:
        response = refresh_access_token(
            client_id,
            client_secret,
            refresh_token,
        )
        logger.debug("Odpowiedź z API Zoho: %s", {k: v for k, v in response.items() if k != "access_token"})
    except KeyError as exc:
        raise RuntimeError(f"Brak konfiguracji: {exc.args[0]}") from exc
    except Exception as exc:
        logger.error("Błąd podczas odświeżania tokena: %s", exc, exc_info=True)
        raise RuntimeError(f"Nie udało się odświeżyć tokena: {exc}") from exc
    
    token = response.get("access_token")
    if not token:
        error_msg = response.get("error", "Nieznany błąd")
        error_description = response.get("error_description", "")
        logger.error("Brak access_token w odpowiedzi. Odpowiedź: %s", response)
        raise RuntimeError(
            f"Brak 'access_token' w odpowiedzi. Błąd: {error_msg}"
            + (f" ({error_description})" if error_description else "")
        )
    
    token_preview = token[:20] + "..." if len(token) > 20 else token
    logger.info("✓ Token został odświeżony pomyślnie (token: %s)", token_preview)
    logger.debug("Pełna odpowiedź: token otrzymany, expires_in: %s", response.get("expires_in", "nieznane"))
    print("✓ Token został odświeżony pomyślnie")
    return token


def detect_csv_separator(csv_path: Path) -> str:
    import csv

    with csv_path.open("r", encoding="utf-8") as fh:
        sample = fh.read(4096)
    try:
        delimiter = csv.Sniffer().sniff(sample).delimiter
        return delimiter
    except Exception:
        for sep in [";", ",", "\t", "|"]:
            if sep in sample:
                return sep
    return ";"


def load_input_file(path_str: str) -> Tuple[pd.DataFrame, str]:
    logger = logging.getLogger(__name__)
    path = Path(path_str)
    if not path.exists():
        raise FileNotFoundError(f"Plik nie istnieje: {path}")

    suffix = path.suffix.lower()
    print(f"📂 Wczytywanie pliku: {path.name}")

    if suffix == ".csv":
        separator = detect_csv_separator(path)
        print(f"   Wykryto separator: '{separator}'")
        df = pd.read_csv(path, sep=separator, encoding="utf-8")
    elif suffix in {".xls", ".xlsx"}:
        df = pd.read_excel(path)
    else:
        raise ValueError(f"Nieobsługiwany format: {suffix}")

    logger.info("✓ Wczytano %s wierszy, %s kolumn", len(df), len(df.columns))
    print(f"✓ Wczytano: {len(df)} kontaktów, {len(df.columns)} kolumn")
    return df, suffix


def ask_for_input_file() -> str:
    print("\n📂 WYBÓR PLIKU DO ANALIZY")
    print("Obsługiwane: XLSX/XLS/CSV (separator wykrywany automatycznie)")
    while True:
        user = input("\nŚcieżka do pliku (lub q aby anulować): ").strip()
        if user.lower() in {"q", "quit", "exit"}:
            sys.exit(0)
        path = clean_path(user)
        try:
            load_input_file(path)
            return path
        except Exception as exc:
            print(f"❌ {exc}")


def configure_source_fields(api_client: ZohoAPIClient, df: pd.DataFrame) -> Dict[str, Any]:
    """
    Konfiguracja pól źródła kontaktu (TAG, Facebook_source, Polecenie_firma, itd.).
    Pobiera metadane z API i pozwala użytkownikowi wybrać wartości.
    """
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("🏷️  ROZPOCZYNAM KONFIGURACJĘ ŹRÓDŁA KONTAKTU")
    logger.info("=" * 80)
    
    print("\n" + "=" * 80)
    print("🏷️  KONFIGURACJA ŹRÓDŁA KONTAKTU I TAGÓW")
    print("=" * 80)
    
    # Lista pól związanych ze źródłem (z obrazka)
    source_fields = [
        "Tag",
        "Facebook_source",
        "Polecenie_firma",
        "Konferencja",
        "Konferencja_medidesk",
        "Konferencja_zewnetrzna",
        "Outbound_Inbound",
        "Polecenie_partner",
        "Polecenie",
        "Polecenie_pracownik",
        "Webinar",
        "Webinar_medidesk",
        "Webinar_zewnetrzne",
        "Zrodlo_inbound",
        "Zrodlo_outbound",
    ]
    
    source_config: Dict[str, Any] = {}
    
    print("\n1️⃣  Które pola chcesz wypełnić?")
    print("Dostępne pola źródła:")
    for idx, field in enumerate(source_fields, start=1):
        print(f"  [{idx:2d}] {field}")
    
    print("\nPodaj numery pól do wypełnienia (oddzielone spacją, Enter=pomiń):")
    choice = input("Wybór: ").strip()
    
    logger.info("Użytkownik wybrał: '%s'", choice or "(puste)")
    
    if not choice:
        print("✓ Pominięto konfigurację źródła")
        logger.info("Pominięto konfigurację źródła - użytkownik wybrał puste")
        return source_config
    
    try:
        # Obsługa przecinków i spacji
        choice_cleaned = choice.replace(",", " ").replace(";", " ")
        selected_indices = [int(x) - 1 for x in choice_cleaned.split() if x.strip()]
        selected_fields = [source_fields[i] for i in selected_indices if 0 <= i < len(source_fields)]
        logger.info("Wybrane pola (indeksy: %s): %s", selected_indices, selected_fields)
    except Exception as exc:
        print("❌ Nieprawidłowy format - pomijam konfigurację")
        logger.error("Błąd parsowania wyboru pól: %s", exc)
        return source_config
    
    if not selected_fields:
        logger.info("Brak wybranych pól - kończę konfigurację")
        return source_config
    
    print(f"\n2️⃣  Konfiguracja wybranych pól ({len(selected_fields)} pól):\n")
    logger.info("Rozpoczynam konfigurację %d pól: %s", len(selected_fields), selected_fields)
    
    # Sprawdź czy w DataFrame są kolumny pasujące do tych pól
    df_columns_lower = {col.lower(): col for col in df.columns}
    
    for field in selected_fields:
        logger.info("-" * 60)
        logger.info("Konfiguruję pole: %s", field)
        print(f"\n📝 Pole: {field}")
        
        # Sprawdź czy w danych wejściowych jest kolumna dla tego pola
        field_lower = field.lower()
        matching_col = df_columns_lower.get(field_lower)
        
        if matching_col:
            logger.info("  Znaleziono pasującą kolumnę w DataFrame: '%s'", matching_col)
            print(f"   ✓ Znaleziono kolumnę w pliku: '{matching_col}'")
            use_column = input(f"   Użyć danych z tej kolumny? (Enter=tak, n=ręcznie): ").strip().lower()
            logger.info("  Odpowiedź użytkownika: '%s'", use_column)
            
            if use_column != "n":
                source_config[field] = {"source": "column", "column_name": matching_col}
                print(f"   → Wartości będą brane z kolumny '{matching_col}'")
                logger.info("  ✓ Skonfigurowano: wartości z kolumny '%s'", matching_col)
                continue
        
        # Specjalna obsługa dla pola Tag
        if field == "Tag":
            logger.info("  Specjalna obsługa TAGów - pobieranie z API")
            print(f"   📡 Pobieram istniejące TAGi z API Zoho...")
            try:
                existing_tags = api_client.get_tags("Contacts")
                logger.info("  Pobrano %d istniejących TAGów: %s", len(existing_tags), existing_tags[:10])
                
                if existing_tags:
                    print(f"   Istniejące TAGi ({len(existing_tags)}):")
                    for idx, tag in enumerate(existing_tags[:20], start=1):
                        print(f"     [{idx:2d}] {tag}")
                    
                    if len(existing_tags) > 20:
                        print(f"     ... i {len(existing_tags) - 20} więcej")
                    
                    print(f"\n   [0] Utwórz nowy TAG")
                    choice = input(f"   Wybierz numer lub 0 dla nowego (Enter=pomiń): ").strip()
                    logger.info("  Wybór użytkownika dla TAG: '%s'", choice)
                    
                    if choice == "0":
                        new_tag = input("   Nazwa nowego TAGa: ").strip()
                        logger.info("  Użytkownik chce utworzyć nowy TAG: '%s'", new_tag)
                        if new_tag:
                            source_config[field] = {"source": "fixed", "value": new_tag}
                            print(f"   ✓ Utworzę nowy TAG: {new_tag}")
                            logger.info("  ✓ Skonfigurowano nowy TAG: %s", new_tag)
                    elif choice:
                        try:
                            idx = int(choice) - 1
                            if 0 <= idx < len(existing_tags):
                                selected_tag = existing_tags[idx]
                                source_config[field] = {"source": "fixed", "value": selected_tag}
                                print(f"   ✓ Wybrano TAG: {selected_tag}")
                                logger.info("  ✓ Wybrano istniejący TAG: %s (indeks: %d)", selected_tag, idx)
                        except Exception as exc:
                            print("   ❌ Nieprawidłowy wybór")
                            logger.error("  Błąd wyboru TAG: %s", exc)
                else:
                    logger.info("  Brak istniejących TAGów - proszę o nowy")
                    new_tag = input("   Brak TAGów - podaj nazwę nowego TAGa (Enter=pomiń): ").strip()
                    logger.info("  Podany nowy TAG: '%s'", new_tag)
                    if new_tag:
                        source_config[field] = {"source": "fixed", "value": new_tag}
                        print(f"   ✓ Utworzę nowy TAG: {new_tag}")
                        logger.info("  ✓ Skonfigurowano nowy TAG: %s", new_tag)
            except Exception as exc:
                logger.error("Błąd pobierania TAGów: %s", exc, exc_info=True)
                tag_value = input("   Podaj TAG ręcznie: ").strip()
                logger.info("  TAG ręczny: '%s'", tag_value)
                if tag_value:
                    source_config[field] = {"source": "fixed", "value": tag_value}
                    logger.info("  ✓ Ustawiono TAG ręcznie: %s", tag_value)
            continue
        
        # Pobierz metadane pola z API
        print(f"   📡 Pobieram metadane pola z API Zoho...")
        try:
            metadata = api_client.get_field_metadata("Contacts")
            fields_list = metadata.get("fields", [])
            
            field_info = next((f for f in fields_list if f.get("api_name") == field), None)
            
            if not field_info:
                print(f"   ⚠ Nie znaleziono pola {field} w API - podaj wartość ręcznie:")
                value = input(f"   Wartość dla {field}: ").strip()
                if value:
                    source_config[field] = {"source": "fixed", "value": value}
                continue
            
            field_type = field_info.get("data_type")
            logger.info("  Typ pola %s: %s", field, field_type)
            
            # Pole typu picklist - pobierz dozwolone wartości
            if field_type in ["picklist", "multiselectpicklist"]:
                logger.info("  Pole picklist - pobieranie dozwolonych wartości...")
                pick_values = field_info.get("pick_list_values", [])
                logger.info("  Znaleziono %d dozwolonych wartości dla %s", len(pick_values), field)
                
                if pick_values:
                    print(f"   Dozwolone wartości ({len(pick_values)}):")
                    for idx, item in enumerate(pick_values[:20], start=1):  # max 20
                        display = item.get("display_value") or item.get("actual_value")
                        print(f"     [{idx:2d}] {display}")
                    
                    if len(pick_values) > 20:
                        print(f"     ... i {len(pick_values) - 20} więcej")
                    
                    choice = input(f"   Wybierz numer (Enter=pomiń): ").strip()
                    logger.info("  Wybór użytkownika: '%s'", choice)
                    
                    if choice:
                        try:
                            idx = int(choice) - 1
                            if 0 <= idx < len(pick_values):
                                selected = pick_values[idx]
                                value = selected.get("actual_value") or selected.get("display_value")
                                source_config[field] = {"source": "fixed", "value": value}
                                print(f"   ✓ Wybrano: {value}")
                                logger.info("  ✓ Wybrano wartość picklist: %s (indeks: %d)", value, idx)
                        except Exception as exc:
                            print("   ❌ Nieprawidłowy wybór")
                            logger.error("  Błąd wyboru z picklist: %s", exc)
                else:
                    value = input(f"   Wartość dla {field}: ").strip()
                    if value:
                        source_config[field] = {"source": "fixed", "value": value}
            
            # Pole typu lookup - pobierz rekordy
            elif field_type == "lookup":
                lookup_module = field_info.get("lookup", {}).get("module")
                logger.info("  Pole lookup wskazuje na moduł: %s", lookup_module)
                
                if lookup_module:
                    print(f"   Pole odnosi się do modułu: {lookup_module}")
                    print(f"   📡 Pobieram rekordy z modułu {lookup_module}...")
                    
                    try:
                        records = api_client.get_lookup_records(lookup_module, limit=50)
                        logger.info("  Pobrano %d rekordów z modułu %s", len(records), lookup_module)
                        if records:
                            print(f"   Dostępne rekordy ({len(records)}):")
                            for idx, rec in enumerate(records[:20], start=1):
                                name = rec.get("Name") or rec.get("name") or rec.get("id")
                                print(f"     [{idx:2d}] {name} (ID: {rec.get('id')})")
                            
                            if len(records) > 20:
                                print(f"     ... i {len(records) - 20} więcej")
                            
                            choice = input(f"   Wybierz numer lub podaj ID (Enter=pomiń): ").strip()
                            logger.info("  Wybór użytkownika dla lookup: '%s'", choice)
                            
                            if choice:
                                try:
                                    # Spróbuj jako numer z listy
                                    idx = int(choice) - 1
                                    if 0 <= idx < len(records):
                                        selected_id = records[idx].get("id")
                                        selected_name = records[idx].get("Name") or records[idx].get("name")
                                        source_config[field] = {"source": "fixed", "value": selected_id}
                                        print(f"   ✓ Wybrano: {selected_name} (ID: {selected_id})")
                                        logger.info("  ✓ Wybrano rekord lookup: %s (ID: %s)", selected_name, selected_id)
                                except ValueError:
                                    # Bezpośrednie ID
                                    source_config[field] = {"source": "fixed", "value": choice}
                                    print(f"   ✓ Użyto ID: {choice}")
                                    logger.info("  ✓ Użyto bezpośredniego ID: %s", choice)
                        else:
                            print(f"   ⚠ Brak rekordów w module {lookup_module}")
                            value = input(f"   Podaj ID ręcznie (Enter=pomiń): ").strip()
                            if value:
                                source_config[field] = {"source": "fixed", "value": value}
                    except Exception as exc:
                        logger.error("Błąd pobierania rekordów lookup: %s", exc)
                        value = input(f"   Podaj ID ręcznie (Enter=pomiń): ").strip()
                        if value:
                            source_config[field] = {"source": "fixed", "value": value}
                else:
                    value = input(f"   Wartość dla {field} (ID): ").strip()
                    if value:
                        source_config[field] = {"source": "fixed", "value": value}
            
            # Inne typy pól
            else:
                value = input(f"   Wartość dla {field} ({field_type}): ").strip()
                if value:
                    source_config[field] = {"source": "fixed", "value": value}
                    
        except Exception as exc:
            logger.error("Błąd konfiguracji pola %s: %s", field, exc)
            value = input(f"   Wartość dla {field} (ręcznie): ").strip()
            if value:
                source_config[field] = {"source": "fixed", "value": value}
    
    print("\n✓ Konfiguracja źródła zakończona")
    logger.info("=" * 80)
    logger.info("✓ ZAKOŃCZONO KONFIGURACJĘ ŹRÓDŁA")
    logger.info("Skonfigurowane pola (%d): %s", len(source_config), list(source_config.keys()))
    for field, config in source_config.items():
        logger.info("  - %s: %s", field, config)
    logger.info("=" * 80)
    return source_config


def apply_source_config(contact_data: Dict[str, Any], source_config: Dict[str, Any], row: pd.Series) -> Dict[str, Any]:
    """
    Aplikuje konfigurację źródła do danych kontaktu.
    
    Args:
        contact_data: Dane kontaktu do wysłania
        source_config: Konfiguracja pól źródła
        row: Wiersz z DataFrame (dla wartości z kolumn)
    
    Returns:
        Zaktualizowane dane kontaktu
    """
    logger = logging.getLogger(__name__)
    logger.debug("Aplikuję konfigurację źródła: %s pól", len(source_config))
    
    phone_fields = {
        "Home_Phone",
        "Mobile",
        "Telefon_komorkowy_3",
        "Phone",
        "Other_Phone",
        "Telefon_stacjonarny_3",
    }
    
    sanitizer = DataSanitizer()

    for field, config in source_config.items():
        if config["source"] == "fixed":
            # Stała wartość dla wszystkich kontaktów
            value = config["value"]
            if field in phone_fields and value is not None:
                # Użyj sanitizera, żeby usunąć .0 z float
                contact_data[field] = sanitizer.sanitize_phone(value) or str(value)
            else:
                contact_data[field] = value
            logger.debug("  %s = %s (stała)", field, config["value"])
        elif config["source"] == "column":
            # Wartość z kolumny DataFrame
            column_name = config["column_name"]
            if column_name in row and pd.notna(row[column_name]):
                value = row[column_name]
                if field in phone_fields and value is not None:
                    # Użyj sanitizera, żeby usunąć .0 z float
                    contact_data[field] = sanitizer.sanitize_phone(value) or str(value)
                else:
                    contact_data[field] = value
                logger.debug("  %s = %s (z kolumny '%s')", field, row[column_name], column_name)
            else:
                logger.debug("  %s: brak wartości w kolumnie '%s'", field, column_name)
    
    return contact_data


def interactive_column_mapping(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    print("\n📋 MAPOWANIE KOLUMN")
    print("Dostępne kolumny:")
    for idx, column in enumerate(df.columns, start=1):
        print(f"  [{idx:2d}] {column}")

    mapping: Dict[str, Optional[str]] = {
        "First_Name": None,
        "Last_Name": None,
        "Email": None,
        "Phone": None,
        "NIP": None,
        "Account_Name": None,
    }

    suggestions = {
        "First_Name": ["imie", "imię", "first", "fname"],
        "Last_Name": ["nazwisko", "last", "lname"],
        "Email": ["email", "mail"],
        "Phone": ["telefon", "phone", "mobile", "tel"],
        "NIP": ["nip", "tax"],
        "Account_Name": ["firma", "company", "account"],
    }

    lower_map = {col.lower(): col for col in df.columns}

    for field, keywords in suggestions.items():
        auto_col = next((lower_map[k] for k in lower_map if k in keywords), None)
        if auto_col:
            prompt = f"{field} → {auto_col} (Enter=tak, numer=zmień, n=pomiń): "
        else:
            prompt = f"{field} → wybierz numer kolumny (Enter=pomiń): "

        answer = input(prompt).strip()
        if not answer:
            mapping[field] = auto_col
            continue
        if answer.lower() == "n":
            mapping[field] = None
            continue
        try:
            idx = int(answer) - 1
            mapping[field] = df.columns[idx]
        except Exception:
            mapping[field] = auto_col

    print("\nWybrane kolumny:")
    for field, column in mapping.items():
        print(f"  • {field:15s}: {column or '-'}")
    return mapping


def prepare_output_dataframe(df: pd.DataFrame, mapping: Dict[str, Optional[str]]) -> pd.DataFrame:
    verification_cols = [col for col in mapping.values() if col]
    result_cols = [
        "Status",
        "Zoho_Contact_ID",
        "Liczba_Duplikatow",
        "Duplikat_Scoring",
        "Wszystkie_Duplikaty_ID",
    ]
    zoho_cols = [
        "Zoho_Salutation",
        "Zoho_First_Name",
        "Zoho_Last_Name",
        "Zoho_Account_Name",
        "Zoho_Account_ID",
        "Zoho_Account_NIP",
        "Zoho_Email_1",
        "Zoho_Email_2",
        "Zoho_Email_3",
        "Zoho_Mobile_1",
        "Zoho_Mobile_2",
        "Zoho_Mobile_3",
        "Zoho_Phone_1",
        "Zoho_Phone_2",
        "Zoho_Phone_3",
        "Zoho_Title",
        "Zoho_Stanowisko",
        "Zoho_Zwrot_grzecznosciowy"
    ]
    rest = [col for col in df.columns if col not in verification_cols]
    order = verification_cols + result_cols + zoho_cols + rest
    unique_order = [col for col in order if col in df.columns]
    for col in zoho_cols:
        if col not in df.columns:
            df[col] = ""
            unique_order.append(col)
    return df[unique_order]


def create_output_directory(input_file_name: str = "") -> Path:
    base = Path(__file__).parent / "wyniki"
    base.mkdir(exist_ok=True)
    
    # Utwórz nazwę folderu: nazwa_pliku_timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    
    if input_file_name:
        # Usuń rozszerzenie z nazwy pliku
        file_stem = Path(input_file_name).stem
        folder_name = f"{file_stem}_{timestamp}"
    else:
        folder_name = timestamp
    
    target = base / folder_name
    target.mkdir()
    return target


def find_new_contact_data(
    input_data: Dict[str, Any], 
    zoho_contact: Dict[str, Any],
    salutation_detector: Optional[SalutationDetector] = None
) -> Dict[str, Any]:
    """
    Znajduje nowe dane (imię, nazwisko, email, telefon), które są w danych wejściowych,
    ale nie ma ich w Zoho i przypisuje je do wolnych slotów.
    
    Specjalna logika dla imienia/nazwiska:
    - Jeśli w Zoho jest [b.imienia] lub [b.nazwiska], nadpisz prawdziwymi danymi
    - Jeśli w danych wejściowych jest [b.imienia] lub [b.nazwiska], nie nadpisuj istniejących prawidłowych danych
    """
    logger = logging.getLogger(__name__)
    update_data = {}
    
    # Sprawdź imię
    input_first_name = input_data.get("First_Name", "").strip()
    zoho_first_name = (zoho_contact.get("First_Name") or "").strip()
    
    # Aktualizuj imię tylko jeśli:
    # 1. W Zoho jest placeholder [b.imienia] a wejściowe dane są prawidłowe
    # 2. Lub w Zoho jest puste a wejściowe dane są prawidłowe
    if input_first_name and input_first_name not in ["[b.imienia]", "[b. imienia]", "[b imienia]"]:
        if not zoho_first_name or zoho_first_name in ["[b.imienia]", "[b. imienia]", "[b imienia]"]:
            update_data["First_Name"] = input_first_name
            logger.info("    → Nowe imię '%s' → First_Name (zastępuję: '%s')", input_first_name, zoho_first_name or "(puste)")
    
    # Sprawdź nazwisko
    input_last_name = input_data.get("Last_Name", "").strip()
    zoho_last_name = (zoho_contact.get("Last_Name") or "").strip()
    
    # Aktualizuj nazwisko tylko jeśli:
    # 1. W Zoho jest placeholder [b.nazwiska] a wejściowe dane są prawidłowe
    # 2. Lub w Zoho jest puste a wejściowe dane są prawidłowe
    if input_last_name and input_last_name not in ["[b.nazwiska]", "[b. nazwiska]", "[b nazwiska]"]:
        if not zoho_last_name or zoho_last_name in ["[b.nazwiska]", "[b. nazwiska]", "[b nazwiska]"]:
            update_data["Last_Name"] = input_last_name
            logger.info("    → Nowe nazwisko '%s' → Last_Name (zastępuję: '%s')", input_last_name, zoho_last_name or "(puste)")
    
    # Sprawdź Salutation (zwrot grzecznościowy) jeśli brak lub jest placeholder
    zoho_salutation = (zoho_contact.get("Salutation") or "").strip()
    input_first_name = input_data.get("First_Name", "").strip()
    
    if input_first_name and salutation_detector:
        # Aktualizuj Salutation jeśli:
        # 1. W Zoho jest puste
        # 2. Lub dane wejściowe mają nowe imię (które będzie aktualizowane)
        should_update_salutation = (
            not zoho_salutation or 
            update_data.get("First_Name")  # Imię zostanie zaktualizowane
        )
        
        if should_update_salutation:
            detected_salutation = salutation_detector.detect_salutation(input_first_name)
            if detected_salutation:
                update_data["Salutation"] = detected_salutation
                logger.info("    → AI wykryło Salutation: '%s' (dla imienia: %s)", 
                           detected_salutation, input_first_name)
    
    # Sprawdź email
    input_email = str(input_data.get("Email") or "").strip()
    if input_email:
        existing_emails = [
            str(zoho_contact.get("Email") or "").strip(),
            str(zoho_contact.get("Secondary_Email") or "").strip(),
            str(zoho_contact.get("Email_3") or "").strip(),
        ]
        existing_emails = [e for e in existing_emails if e]  # usuń puste
        
        # Porównanie case-insensitive (adam@example.com == Adam@Example.COM)
        input_email_lower = input_email.lower()
        existing_emails_lower = [e.lower() for e in existing_emails]
        
        if input_email_lower not in existing_emails_lower:
            logger.info("    → Email '%s' nie istnieje w Zoho (istniejące: %s)", input_email, existing_emails)
            # Znajdź wolny slot
            if not zoho_contact.get("Email"):
                update_data["Email"] = input_email
                logger.info("    → Nowy email '%s' → Email (slot 1)", input_email)
            elif not zoho_contact.get("Secondary_Email"):
                update_data["Secondary_Email"] = input_email
                logger.info("    → Nowy email '%s' → Secondary_Email (slot 2)", input_email)
            elif not zoho_contact.get("Email_3"):
                update_data["Email_3"] = input_email
                logger.info("    → Nowy email '%s' → Email_3 (slot 3)", input_email)
            else:
                logger.warning("    ⚠ Brak wolnego slotu dla nowego emaila: %s", input_email)
        else:
            logger.info("    ⊘ Email '%s' już istnieje w Zoho - pomijam", input_email)
    
    # Sprawdź telefon — użyj sanitizera, żeby usunąć .0 z float
    sanitizer = DataSanitizer()
    input_phone = sanitizer.sanitize_phone(input_data.get("Phone"))
    if input_phone:
        # Pobierz istniejące telefony i sanityzuj (tylko cyfry, bez spacji/myślników)
        existing_phones_raw = [
            zoho_contact.get("Home_Phone"),
            zoho_contact.get("Mobile"),
            zoho_contact.get("Telefon_komorkowy_3"),
            zoho_contact.get("Phone"),
            zoho_contact.get("Other_Phone"),
            zoho_contact.get("Telefon_stacjonarny_3"),
        ]
        # Sanityzuj wszystkie istniejące telefony (tylko cyfry)
        existing_phones = []
        for phone in existing_phones_raw:
            sanitized = sanitizer.sanitize_phone(phone)
            if sanitized:
                existing_phones.append(sanitized)
        
        # Porównaj tylko cyfry: "601 578 786" == "601578786"
        if input_phone not in existing_phones:
            logger.info("    → Telefon '%s' nie istnieje w Zoho (istniejące: %s)", input_phone, existing_phones)
            # Znajdź wolny slot (priorytet: Mobile -> Phone -> inne)
            if not zoho_contact.get("Mobile"):
                update_data["Mobile"] = input_phone
                logger.info("    → Nowy telefon '%s' → Mobile (slot główny)", input_phone)
            elif not zoho_contact.get("Phone"):
                update_data["Phone"] = input_phone
                logger.info("    → Nowy telefon '%s' → Phone (slot 2)", input_phone)
            elif not zoho_contact.get("Home_Phone"):
                update_data["Home_Phone"] = input_phone
                logger.info("    → Nowy telefon '%s' → Home_Phone (slot 3)", input_phone)
            elif not zoho_contact.get("Other_Phone"):
                update_data["Other_Phone"] = input_phone
                logger.info("    → Nowy telefon '%s' → Other_Phone (slot 4)", input_phone)
            elif not zoho_contact.get("Telefon_komorkowy_3"):
                update_data["Telefon_komorkowy_3"] = input_phone
                logger.info("    → Nowy telefon '%s' → Telefon_komorkowy_3 (slot 5)", input_phone)
            elif not zoho_contact.get("Telefon_stacjonarny_3"):
                update_data["Telefon_stacjonarny_3"] = input_phone
                logger.info("    → Nowy telefon '%s' → Telefon_stacjonarny_3 (slot 6)", input_phone)
            else:
                logger.warning("    ⚠ Brak wolnego slotu dla nowego telefonu: %s", input_phone)
        else:
            logger.info("    ⊘ Telefon '%s' już istnieje w Zoho - pomijam", input_phone)
    
    return update_data


def enrich_with_zoho_data(row: Dict[str, Any], contact: Dict[str, Any], scoring: Dict[str, Any]) -> None:
    row["Zoho_Contact_ID"] = contact.get("id", "")
    row["Duplikat_Scoring"] = scoring.get("overall_score", 0)
    row["Zoho_Salutation"] = contact.get("Salutation", "")
    row["Zoho_First_Name"] = contact.get("First_Name", "")
    row["Zoho_Last_Name"] = contact.get("Last_Name", "")

    account = contact.get("Account_Name")
    if isinstance(account, dict):
        row["Zoho_Account_Name"] = account.get("name", "")
        row["Zoho_Account_ID"] = account.get("id", "")
    else:
        row["Zoho_Account_Name"] = account or ""
        row["Zoho_Account_ID"] = ""
    row["Zoho_Account_NIP"] = ""  # można rozbudować o dodatkowe API

    row["Zoho_Email_1"] = contact.get("Email", "")
    row["Zoho_Email_2"] = contact.get("Secondary_Email", "")
    row["Zoho_Email_3"] = contact.get("Email_3", "")

    row["Zoho_Mobile_1"] = contact.get("Home_Phone", "")
    row["Zoho_Mobile_2"] = contact.get("Mobile", "")
    row["Zoho_Mobile_3"] = contact.get("Telefon_komorkowy_3", "")

    row["Zoho_Phone_1"] = contact.get("Phone", "")
    row["Zoho_Phone_2"] = contact.get("Other_Phone", "")
    row["Zoho_Phone_3"] = contact.get("Telefon_stacjonarny_3", "")

    row["Zoho_Title"] = contact.get("Title", "")
    row["Zoho_Stanowisko"] = contact.get("Stanowisko", "")


def process_contacts(
    df: pd.DataFrame,
    mapping: Dict[str, Optional[str]],
    api_client: ZohoAPIClient,
    duplicate_finder: DuplicateFinder,
    scorer: ContactScorer,
    update_duplicates: bool = False,
    source_config: Optional[Dict[str, Any]] = None,
    salutation_detector: Optional[SalutationDetector] = None,
) -> Tuple[pd.DataFrame, List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns:
        Tuple: (result_df, duplicates_to_update, new_contacts_to_create)
    """
    results: List[Dict[str, Any]] = []
    duplicates_to_update: List[Dict[str, Any]] = []  # Zbierz duplikaty z nowymi danami
    new_contacts_to_create: List[Dict[str, Any]] = []  # Zbierz nowe kontakty do utworzenia

    logger = logging.getLogger(__name__)
    print("\n🔄 Przetwarzanie kontaktów...")
    for idx, (_, row) in enumerate(tqdm(df.iterrows(), total=len(df), desc="Postęp", unit="kontakt"), start=1):
        try:
            logger.info("\n" + "▼" * 80)
            logger.info("📌 PRZETWARZAM KONTAKT #%d/%d", idx, len(df))
            
            contact_input: Dict[str, Any] = {}
            for field, column in mapping.items():
                if column and column in row and pd.notna(row[column]):
                    contact_input[field] = row[column]
            
            logger.info("  Dane wejściowe: %s", contact_input)
            
            duplicates = duplicate_finder.find_duplicates(contact_input)

            result_row = row.to_dict()
            result_row.setdefault("Status", "NOWY_KONTAKT")
            result_row.setdefault("Zoho_Contact_ID", "")
            result_row.setdefault("Duplikat_Scoring", 0)
            result_row.setdefault("Wszystkie_Duplikaty_ID", "")
            result_row.setdefault("Liczba_Duplikatow", 0)

            if duplicates:
                logger.info("  🔴 DUPLIKAT! Znaleziono %d pasujących kontaktów", len(duplicates))
                result_row["Status"] = "DUPLIKAT_ZNALEZIONY"
                result_row["Liczba_Duplikatow"] = len(duplicates)
                result_row["Wszystkie_Duplikaty_ID"] = ", ".join(duplicates)

                logger.info("  📊 Analizuję quality score dla top 5 duplikatów...")
                best_contact = None
                best_scoring = None
                best_score_value = -1

                for dup_id in duplicates[:5]:
                    try:
                        contact = api_client.get_contact_by_id(dup_id)
                        if not contact:
                            continue
                        scoring = scorer.calculate_score(contact)
                        score_value = scoring.get("overall_score", 0)
                        logger.info("    - ID %s: score=%d", dup_id, score_value)
                        if score_value > best_score_value:
                            best_contact = contact
                            best_scoring = scoring
                            best_score_value = score_value
                    except Exception as exc:
                        logger.warning("    ✗ Błąd przy pobieraniu kontaktu %s: %s", dup_id, exc)
                        continue

                if best_contact and best_scoring:
                    logger.info("  ✅ Wybrany najlepszy kontakt (score=%d): %s %s", 
                               best_score_value,
                               best_contact.get("First_Name", ""),
                               best_contact.get("Last_Name", ""))
                    
                    # Sprawdź czy są nowe dane (niezależnie od trybu - zbieramy informacje)
                    logger.info("  🔄 Sprawdzam czy są nowe dane do dodania...")
                    new_data = find_new_contact_data(contact_input, best_contact, salutation_detector)
                    
                    # Dla duplikatów dodaj tylko TAG (jeśli skonfigurowany)
                    if source_config and "Tag" in source_config:
                        tag_config = source_config["Tag"]
                        if tag_config["source"] == "fixed":
                            new_data["Tag"] = tag_config["value"]
                            logger.info("  🏷️ Dodaję TAG do duplikatu: %s", tag_config["value"])
                        elif tag_config["source"] == "column":
                            column_name = tag_config["column_name"]
                            if column_name in row and pd.notna(row[column_name]):
                                new_data["Tag"] = row[column_name]
                                logger.info("  🏷️ Dodaję TAG z kolumny '%s': %s", column_name, row[column_name])
                    
                    if new_data:
                        logger.info("  📝 Znaleziono nowe dane do dodania")
                        contact_id = best_contact.get("id")
                        # Dodaj do listy do późniejszej aktualizacji
                        duplicates_to_update.append({
                            "contact_id": contact_id,
                            "name": f"{best_contact.get('First_Name', '')} {best_contact.get('Last_Name', '')}".strip(),
                            "old_first_name": best_contact.get("First_Name", ""),
                            "old_last_name": best_contact.get("Last_Name", ""),
                            "new_data": new_data,
                            "row_index": idx,
                        })
                        result_row["Status"] = "DUPLIKAT_DO_AKTUALIZACJI"
                    else:
                        logger.info("  ℹ Brak nowych danych - kontakt ma już wszystkie informacje")
                    
                    enrich_with_zoho_data(result_row, best_contact, best_scoring)
            else:
                logger.info("  ✅ NOWY KONTAKT - brak duplikatów")
                
                # Zbierz nowe kontakty do utworzenia (jeśli source_config podany)
                if source_config:
                    # Przygotuj dane dla nowego kontaktu
                    new_contact_data = {}
                    phone_fields = {
                        "Home_Phone",
                        "Mobile",
                        "Telefon_komorkowy_3",
                        "Phone",
                        "Other_Phone",
                        "Telefon_stacjonarny_3",
                    }
                    sanitizer = DataSanitizer()
                    for field, column in mapping.items():
                        if column and column in row and pd.notna(row[column]):
                            value = row[column]
                            if field in phone_fields and value is not None:
                                # Użyj sanitizera, żeby usunąć .0 z float
                                new_contact_data[field] = sanitizer.sanitize_phone(value) or str(value)
                            else:
                                new_contact_data[field] = value
                    
                    # Dodaj Salutation jeśli AI jest dostępne
                    if salutation_detector and contact_input.get("First_Name"):
                        salutation = salutation_detector.detect_salutation(contact_input["First_Name"])
                        if salutation:
                            new_contact_data["Salutation"] = salutation
                            logger.info("  🤖 AI dodało Salutation: %s", salutation)
                    
                    # Dodaj pola źródła
                    new_contact_data = apply_source_config(new_contact_data, source_config, row)
                    
                    new_contacts_to_create.append({
                        "data": new_contact_data,
                        "name": f"{contact_input.get('First_Name', '')} {contact_input.get('Last_Name', '')}".strip(),
                        "row_index": idx,
                    })

            results.append(result_row)
            logger.info("▲" * 80)
        except Exception as exc:
            logger.error("Błąd przy przetwarzaniu kontaktu %d (wiersz %d): %s", idx, idx+1, exc, exc_info=True)
            # Zapisz wynik z informacją o błędzie
            result_row = row.to_dict()
            result_row.setdefault("Status", "BŁĄD_PRZETWARZANIA")
            result_row.setdefault("Zoho_Contact_ID", "")
            result_row.setdefault("Duplikat_Scoring", 0)
            result_row.setdefault("Wszystkie_Duplikaty_ID", "")
            result_row.setdefault("Liczba_Duplikatow", 0)
            # Dodaj informację o błędzie do wyników
            if "Status" not in result_row or result_row["Status"] == "BŁĄD_PRZETWARZANIA":
                result_row["Status"] = f"BŁĄD: {str(exc)[:100]}"
            results.append(result_row)

    result_df = pd.DataFrame(results)
    
    return result_df, duplicates_to_update, new_contacts_to_create


def ask_for_create_confirmation(new_contacts_summary: List[Dict[str, Any]]) -> bool:
    """Pokazuje podsumowanie nowych kontaktów i pyta czy utworzyć."""
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("PODSUMOWANIE NOWYCH KONTAKTÓW: %d kontaktów", len(new_contacts_summary))
    
    if not new_contacts_summary:
        print("\n✓ Brak nowych kontaktów do utworzenia")
        logger.info("Brak nowych kontaktów do utworzenia")
        return False
    
    print("\n" + "=" * 80)
    print("➕ PODSUMOWANIE NOWYCH KONTAKTÓW DO UTWORZENIA")
    print("=" * 80)
    print(f"\nZnaleziono {len(new_contacts_summary)} nowych kontaktów:\n")
    
    for idx, contact in enumerate(new_contacts_summary[:10], start=1):
        print(f"{idx}. {contact['name']}")
        data = contact['data']
        logger.info("  %d. %s - dane: %s", idx, contact['name'], data)
        
        if data.get('Email'):
            print(f"   Email: {data['Email']}")
        if data.get('Phone'):
            print(f"   Telefon: {data['Phone']}")
        if data.get('Salutation'):
            print(f"   Zwrot: {data['Salutation']}")
    
    if len(new_contacts_summary) > 10:
        print(f"\n   ... i {len(new_contacts_summary) - 10} więcej kontaktów")
        logger.info("  (i %d więcej kontaktów)", len(new_contacts_summary) - 10)
    
    print("\n" + "=" * 80)
    logger.info("=" * 80)
    answer = input(f"\n❓ Czy utworzyć {len(new_contacts_summary)} nowych kontaktów w Zoho? (tak/nie, Enter=nie): ").strip().lower()
    logger.info("Odpowiedź użytkownika na tworzenie: '%s'", answer)
    
    if answer in ["tak", "t", "yes", "y"]:
        print("✓ Tworzę nowe kontakty w Zoho...")
        logger.info("✓ Użytkownik potwierdził tworzenie kontaktów")
        return True
    else:
        print("✗ Pominięto tworzenie kontaktów")
        logger.info("✗ Użytkownik anulował tworzenie kontaktów")
        return False


def match_contacts_to_companies(
    result_df: pd.DataFrame,
    api_client: ZohoAPIClient,
    company_matcher: CompanyMatcher
) -> pd.DataFrame:
    """
    Analizuje kontakty bez przypisanej firmy i próbuje dopasować je po domenie emaila.
    Pyta użytkownika o potwierdzenie dla każdego dopasowania.
    """
    logger = logging.getLogger(__name__)
    logger.info("\n" + "🏢" * 40)
    logger.info("ANALIZA DOPASOWANIA KONTAKTÓW DO FIRM")
    logger.info("🏢" * 40)
    
    print("\n" + "=" * 80)
    print("🏢 ANALIZA DOPASOWANIA KONTAKTÓW DO FIRM (na podstawie domeny emaila)")
    print("=" * 80)
    
    # Znajdź kontakty, które mają Zoho_Contact_ID (zostały utworzone/zaktualizowane)
    contacts_with_id = result_df[
        (result_df["Zoho_Contact_ID"].notna()) & 
        (result_df["Zoho_Contact_ID"] != "")
    ]
    
    if contacts_with_id.empty:
        print("\n✓ Brak kontaktów do sprawdzenia")
        logger.info("Brak kontaktów do dopasowania")
        return result_df
    
    matched_count = 0
    skipped_count = 0
    contacts_without_company_count = 0
    
    # Znajdź kolumnę z domeną maila w DataFrame (case-insensitive) - opcjonalna
    domain_column = None
    
    # Automatycznie wykryj kolumnę zawierającą "domena" i "mail"
    potential_columns = []
    for col in result_df.columns:
        if "domena" in col.lower() and ("mail" in col.lower() or "email" in col.lower()):
            potential_columns.append(col)
    
    if potential_columns:
        if len(potential_columns) == 1:
            # Jedna kolumna - zapytaj o potwierdzenie
            print(f"\n📧 Znaleziono kolumnę z domeną maila: '{potential_columns[0]}'")
            confirm = input(f"   Użyć tej kolumny do dopasowania firm (dla kontaktów bez emaila)? (tak/nie, Enter=tak): ").strip().lower()
            if confirm in ["", "tak", "t", "yes", "y"]:
                domain_column = potential_columns[0]
                logger.info("✓ Użytkownik potwierdził kolumnę domeny: '%s'", domain_column)
                print(f"   ✓ Użyję kolumny '{domain_column}'")
            else:
                logger.info("Użytkownik odrzucił kolumnę domeny")
                print("   ✗ Pominięto - dopasowanie tylko po emailach")
        else:
            # Wiele kolumn - pozwól wybrać
            print(f"\n📧 Znaleziono {len(potential_columns)} kolumn z domeną maila:")
            for i, col in enumerate(potential_columns, start=1):
                print(f"   [{i}] {col}")
            print(f"   [0] Pomiń - nie używaj domeny z pliku")
            
            choice = input(f"\n   Wybierz numer kolumny (Enter=1): ").strip()
            if choice == "0":
                logger.info("Użytkownik pominął kolumnę domeny")
                print("   ✗ Pominięto - dopasowanie tylko po emailach")
            else:
                try:
                    idx = int(choice) - 1 if choice else 0
                    if 0 <= idx < len(potential_columns):
                        domain_column = potential_columns[idx]
                        logger.info("✓ Użytkownik wybrał kolumnę domeny: '%s'", domain_column)
                        print(f"   ✓ Użyję kolumny '{domain_column}'")
                except (ValueError, IndexError):
                    logger.warning("Nieprawidłowy wybór - pomijam kolumnę domeny")
                    print("   ✗ Nieprawidłowy wybór - pomijam")
    else:
        logger.info("ℹ Brak kolumny z domeną maila - dopasowanie tylko po emailach z Zoho")
        print("ℹ Brak kolumny 'Domena maila' w pliku - dopasowanie tylko po emailach")
    
    for idx, row in contacts_with_id.iterrows():
        contact_id = row["Zoho_Contact_ID"]
        first_name = row.get("First_Name", "")
        last_name = row.get("Last_Name", "")
        
        # Pobierz pełne dane kontaktu z Zoho, żeby sprawdzić czy ma firmę i mieć wszystkie 3 emaile
        logger.debug("Pobieram pełne dane kontaktu ID: %s", contact_id)
        try:
            contact_full = api_client.get_contact_by_id(contact_id)
            if not contact_full:
                logger.warning("Nie udało się pobrać danych kontaktu %s", contact_id)
                skipped_count += 1
                continue
        except Exception as exc:
            logger.error("Błąd pobierania kontaktu %s: %s", contact_id, exc)
            skipped_count += 1
            continue
        
        # Sprawdź czy kontakt ma już przypisaną firmę w Zoho
        if contact_full.get("Account_Name"):
            logger.debug("Pomijam %s %s - ma już firmę: %s", 
                        first_name, last_name, 
                        contact_full.get("Account_Name", {}).get("name") if isinstance(contact_full.get("Account_Name"), dict) else contact_full.get("Account_Name"))
            skipped_count += 1
            continue
        
        contacts_without_company_count += 1
        
        # Zbierz wszystkie emaile kontaktu (z Zoho, nie z DataFrame)
        emails = [
            contact_full.get("Email"),
            contact_full.get("Secondary_Email"),
            contact_full.get("Email_3"),
        ]
        emails = [e for e in emails if e and str(e).strip()]
        
        logger.info("  📧 Kontakt %s %s (ID: %s) - emaile: %s", 
                    first_name, last_name, contact_id, emails)
        
        # Jeśli brak emaili, sprawdź kolumnę "Domena maila" z DataFrame
        domain_from_column = None
        if not emails and domain_column and domain_column in row:
            domain_from_column = str(row[domain_column]).strip() if pd.notna(row[domain_column]) else None
            if domain_from_column:
                logger.info("    ⚠ Brak emaili w Zoho, ale mam domenę z pliku: %s", domain_from_column)
        
        if not emails and not domain_from_column:
            logger.info("    ⊘ Pomijam - brak emaila i domeny")
            skipped_count += 1
            continue
        
        # Sprawdź każdy email w kolejności i znajdź pierwszą niepubliczną domenę
        matched_email = None
        matched_domain = None
        company = None
        
        # Jeśli mamy domenę z kolumny (a nie emaile), użyj jej bezpośrednio
        if domain_from_column and not emails:
            # Sprawdź czy nie jest publiczną domeną
            if company_matcher.is_public_domain(domain_from_column):
                logger.info("    ⊘ Pomijam - domena z pliku jest publiczna: %s", domain_from_column)
                skipped_count += 1
                continue
            
            logger.info("    🔍 Sprawdzam domenę z pliku: %s", domain_from_column)
            company = company_matcher.find_company_by_domain(domain_from_column)
            if company:
                matched_email = f"(domena: {domain_from_column})"  # Dla wyświetlania
                matched_domain = domain_from_column
                logger.info("    ✅ Znaleziono firmę dla domeny z pliku!")
            else:
                logger.info("    ⊘ Brak firmy dla domeny z pliku: %s", domain_from_column)
                skipped_count += 1
                continue
        
        # Normalny przepływ - sprawdzenie emaili
        for email in emails:
            domain = company_matcher.extract_email_domain(str(email))
            if not domain:
                logger.debug("    Nie można wyciągnąć domeny z: %s", email)
                continue
            
            # Pomiń publiczne domeny
            if company_matcher.is_public_domain(domain):
                logger.info("    ⊘ Pomijam publiczną domenę: %s (z emaila: %s)", domain, email)
                continue
            
            # Wyszukaj firmę dla pierwszej niepublicznej domeny
            logger.info("    🔍 Sprawdzam domenę firmową: %s (z emaila: %s)", domain, email)
            company = company_matcher.find_company_by_domain(domain)
            if company:
                matched_email = email
                matched_domain = domain
                logger.info("    ✅ Znaleziono firmę!")
                break
            else:
                logger.info("    ⊘ Brak firmy dla domeny: %s", domain)
        
        if not company:
            logger.info("  ⊘ Brak dopasowania dla %s %s (wszystkie emaile publiczne lub nie znaleziono firmy)", 
                        first_name, last_name)
            skipped_count += 1
            continue
        
        # Pokaż dopasowanie i zapytaj o potwierdzenie
        company_name = company.get("Account_Name") or company.get("Name") or "Nieznana"
        company_id = company.get("id")
        
        print(f"\n📋 Znaleziono dopasowanie:")
        print(f"   Kontakt: {first_name} {last_name}")
        print(f"   Email firmowy: {matched_email}")
        print(f"   Domena: {matched_domain}")
        print(f"   ↓")
        print(f"   Firma: {company_name} (ID: {company_id})")
        print(f"\n   Opcje:")
        print(f"   [t]ak      - przypisz do tej firmy")
        print(f"   [n]ie      - pomiń to dopasowanie")
        print(f"   /ignoruj   - dodaj domenę '{matched_domain}' do listy pomijanych (na stałe)")
        
        answer = input("   Wybór (t/n//ignoruj, Enter=nie): ").strip().lower()
        logger.info("Dopasowanie %s → %s: odpowiedź '%s'", matched_email, company_name, answer)
        
        # Obsługa /ignoruj
        if answer == "/ignoruj":
            print(f"   🚫 Dodaję domenę '{matched_domain}' do listy ignorowanych...")
            if company_matcher.add_ignored_domain(matched_domain):
                print(f"   ✅ Domena '{matched_domain}' będzie pomijana w przyszłych przebiegach")
                logger.info("Dodano '%s' do ignorowanych domen", matched_domain)
            else:
                print(f"   ✗ Błąd zapisu domeny")
            skipped_count += 1
            continue
        
        if answer in ["tak", "t", "yes", "y"]:
            # Aktualizuj kontakt w Zoho - Account_Name to pole relacji, wartość to ID firmy
            update_data = {"Account_Name": str(company_id)}
            logger.info("Wysyłam do API: %s", update_data)
            
            if api_client.update_contact(contact_id, update_data):
                # Zaktualizuj DataFrame
                result_df.at[idx, "Zoho_Account_Name"] = company_name
                result_df.at[idx, "Zoho_Account_ID"] = company_id
                matched_count += 1
                print("   ✅ Przypisano do firmy")
                logger.info("✅ Przypisano %s %s do firmy %s (ID: %s)", first_name, last_name, company_name, company_id)
            else:
                print("   ✗ Błąd aktualizacji")
                logger.error("✗ Błąd przypisania %s %s do firmy %s", first_name, last_name, company_name)
                skipped_count += 1
        else:
            print("   ⊘ Pominięto")
            logger.info("Użytkownik odrzucił dopasowanie")
            skipped_count += 1
    
    print(f"\n📊 Podsumowanie dopasowania:")
    print(f"   - Kontaktów bez firmy w Zoho: {contacts_without_company_count}")
    print(f"   - Przypisano do firm: {matched_count}")
    print(f"   - Pominięto: {skipped_count}")
    
    logger.info("Zakończono dopasowanie: %d kontaktów bez firmy, %d przypisanych, %d pominiętych", 
                contacts_without_company_count, matched_count, skipped_count)
    logger.info("🏢" * 40)
    
    return result_df


def finalize_operations(
    result_df: pd.DataFrame,
    duplicates_to_update: List[Dict[str, Any]],
    new_contacts_to_create: List[Dict[str, Any]],
    api_client: ZohoAPIClient,
    update_mode: bool
) -> pd.DataFrame:
    """
    Finalizuje operacje: aktualizuje duplikaty i tworzy nowe kontakty po potwierdzeniu.
    """
    logger = logging.getLogger(__name__)
    logger.info("\n" + "🔷" * 40)
    logger.info("FINALIZACJA OPERACJI")
    logger.info("  Duplikatów do aktualizacji: %d", len(duplicates_to_update))
    logger.info("  Nowych kontaktów do utworzenia: %d", len(new_contacts_to_create))
    logger.info("🔷" * 40)
    
    # Aktualizacja duplikatów - grupowanie podobnych zmian
    if update_mode and duplicates_to_update:
        logger.info("\n🔍 Znaleziono %d duplikatów z nowymi danymi", len(duplicates_to_update))
        print(f"\n🔄 Znaleziono {len(duplicates_to_update)} duplikatów z nowymi danymi")
        
        # Grupuj według typu zmian
        from collections import defaultdict
        groups = defaultdict(list)
        
        for dup in duplicates_to_update:
            # Utwórz klucz grupowania: posortowane nazwy pól
            fields = tuple(sorted(dup["new_data"].keys()))
            groups[fields].append(dup)
        
        logger.info("Pogrupowano na %d typów zmian", len(groups))
        print(f"\n📊 Pogrupowano zmiany na {len(groups)} typów:\n")
        
        # Pokaż grupy
        for i, (fields, items) in enumerate(groups.items(), start=1):
            print(f"   [{i}] {len(items)} kontaktów - zmiany: {', '.join(fields)}")
            logger.info("  Grupa %d: %d kontaktów - pola: %s", i, len(items), fields)
        
        updated_count = 0
        skipped_count = 0
        
        # Przetwarzaj grupy
        for group_num, (fields, group_items) in enumerate(groups.items(), start=1):
            print(f"\n" + "=" * 80)
            print(f"📦 GRUPA {group_num}/{len(groups)}: {len(group_items)} kontaktów")
            print(f"   Typ zmian: {', '.join(fields)}")
            print("=" * 80)
            
            # Pokaż przykład pierwszego rekordu z grupy
            example = group_items[0]
            print(f"\n📋 Przykład (pierwszy rekord z grupy):")
            print(f"   Kontakt: {example['name']}")
            
            # Pokaż jakie zmiany
            example_new_data = example["new_data"]
            tag_in_example = example_new_data.get("Tag")
            other_fields_example = {k: v for k, v in example_new_data.items() if k != "Tag"}
            
            if other_fields_example:
                print(f"\n   Pola do aktualizacji:")
                for field, value in other_fields_example.items():
                    print(f"      • {field} → {value}")
            
            if tag_in_example:
                print(f"\n   🏷️  TAG do dodania: {tag_in_example}")
            
            # Zapytaj o całą grupę
            print(f"\n❓ Co zrobić z tą grupą ({len(group_items)} kontaktów)?")
            print(f"   [w]szystkie  - zaktualizuj wszystkie kontakty w grupie")
            print(f"   [p]omiń      - pomiń całą grupę")
            print(f"   [j]ednostki  - przejrzyj każdy kontakt osobno")
            print(f"   [a]nuluj     - przerwij całą aktualizację")
            
            group_choice = input("\n   Wybór: ").strip().lower()
            logger.info("Grupa %d - wybór użytkownika: '%s'", group_num, group_choice)
            
            if group_choice in ["a", "anuluj"]:
                print("\n✗ Anulowano aktualizację")
                logger.info("Użytkownik anulował aktualizację")
                break
            elif group_choice in ["p", "pomiń", "pomij"]:
                print(f"   ⊘ Pominięto grupę ({len(group_items)} kontaktów)")
                logger.info("Pominięto grupę %d (%d kontaktów)", group_num, len(group_items))
                skipped_count += len(group_items)
                continue
            elif group_choice in ["w", "wszystkie"]:
                # Zaktualizuj wszystkie w grupie bez pytania
                print(f"\n   ⏳ Aktualizuję {len(group_items)} kontaktów...")
                for dup in group_items:
                    try:
                        contact_id = dup["contact_id"]
                        new_data = dup["new_data"].copy()
                        tag_value = new_data.pop("Tag", None)
                        
                        success = True
                        if new_data:
                            success = api_client.update_contact(contact_id, new_data)
                        
                        if success and tag_value:
                            api_client.add_tags_to_contact(contact_id, [tag_value])
                        
                        if success:
                            row_idx = dup["row_index"] - 1
                            if row_idx < len(result_df):
                                result_df.at[row_idx, "Status"] = "DUPLIKAT_ZAKTUALIZOWANY"
                            updated_count += 1
                            logger.info("✅ Zaktualizowano %s (ID: %s)", dup["name"], contact_id)
                        else:
                            logger.error("✗ Błąd aktualizacji %s", dup["name"])
                            skipped_count += 1
                    except Exception as exc:
                        logger.error("Błąd aktualizacji %s: %s", dup["name"], exc, exc_info=True)
                        skipped_count += 1
                
                print(f"   ✅ Zaktualizowano {updated_count} kontaktów z grupy")
                continue
            elif group_choice in ["j", "jednostki", "pojedynczo"]:
                # Przejrzyj każdy rekord osobno - użyj starego kodu
                print(f"\n   📋 Przeglądam każdy kontakt z grupy osobno...\n")
                
                auto_update_all = False
                for rec_num, dup in enumerate(group_items, start=1):
                    try:
                        contact_id = dup["contact_id"]
                        new_data_orig = dup["new_data"].copy()
                        
                        # Pobierz aktualne dane z Zoho dla porównania
                        try:
                            current_contact = api_client.get_contact_by_id(contact_id)
                        except Exception as exc:
                            logger.warning("Nie udało się pobrać danych kontaktu %s: %s", contact_id, exc)
                            current_contact = {}
                        
                        print("\n" + "=" * 80)
                        print(f"📋 REKORD {rec_num}/{len(group_items)}")
                        print(f"   Kontakt: {dup['name']} (ID: {contact_id})")
                        print("=" * 80)
                        logger.info("Rekord %d/%d: %s (ID: %s)", rec_num, len(group_items), dup["name"], contact_id)
                        
                        # Rozdziel TAG od innych pól (TAG jest dodawany, nie zamieniany)
                        tag_value_display = new_data_orig.get("Tag")
                        other_fields = {k: v for k, v in new_data_orig.items() if k != "Tag"}
                        
                        # Pokaż szczegółowo co się zmieni
                        if other_fields:
                            print("\n📊 Pola do aktualizacji (zamiana wartości):")
                            for field, new_value in other_fields.items():
                                old_value = current_contact.get(field, "(puste)")
                                if isinstance(old_value, dict):
                                    old_value = old_value.get("name") or old_value.get("id") or str(old_value)
                                print(f"   • {field}:")
                                print(f"      Było:   {old_value}")
                                print(f"      Będzie: {new_value}")
                                logger.info("     %s: '%s' → '%s'", field, old_value, new_value)
                        
                        # TAG pokazuj osobno jako "dodanie"
                        if tag_value_display:
                            print("\n🏷️  TAG do dodania (nie zastępuje istniejących):")
                            # Pokaż istniejące tagi
                            existing_tags = current_contact.get("Tag", [])
                            if existing_tags:
                                if isinstance(existing_tags, list):
                                    tag_names = [t.get("name") if isinstance(t, dict) else str(t) for t in existing_tags]
                                    print(f"   • Obecne tagi: {', '.join(tag_names)}")
                                    logger.info("     Obecne tagi: %s", tag_names)
                                else:
                                    print(f"   • Obecne tagi: {existing_tags}")
                                    logger.info("     Obecne tagi: %s", existing_tags)
                            else:
                                print(f"   • Obecne tagi: (brak)")
                            print(f"   • Nowy tag: {tag_value_display}")
                            logger.info("     Nowy tag do dodania: %s", tag_value_display)
                        
                        if not other_fields and not tag_value_display:
                            print("\n   (brak zmian)")
                            logger.info("     Brak zmian dla tego rekordu")
                        
                        # Zapytaj o aktualizację (jeśli nie auto)
                        if not auto_update_all:
                            print("\n❓ Co zrobić z tym rekordem?")
                            print("   [t]ak       - zaktualizuj ten rekord")
                            print("   [n]ie       - pomiń ten rekord")
                            print("   [w]szystkie - zaktualizuj ten i wszystkie pozostałe w grupie")
                            print("   [a]nuluj    - przerwij całą aktualizację")
                            answer = input("   Wybór (t/n/w/a, Enter=nie): ").strip().lower()
                            logger.info("  Odpowiedź: '%s'", answer)
                            
                            if answer in ["a", "anuluj", "q", "quit"]:
                                print("\n✗ Anulowano aktualizację")
                                logger.info("Użytkownik anulował całą aktualizację")
                                break
                            elif answer in ["w", "wszystkie", "all"]:
                                auto_update_all = True
                                print("✓ Aktualizuję ten i wszystkie pozostałe rekordy w grupie...")
                                logger.info("Użytkownik wybrał aktualizację wszystkich pozostałych w grupie")
                            elif answer not in ["t", "tak", "yes", "y"]:
                                print("⊘ Pominięto")
                                logger.info("Pominięto rekord")
                                skipped_count += 1
                                continue
                        
                        # Aktualizuj
                        new_data = new_data_orig.copy()
                        tag_value = new_data.pop("Tag", None)
                        
                        logger.info("Aktualizuję kontakt %s (ID: %s) z danymi: %s", 
                                   dup["name"], contact_id, new_data)
                        
                        success = True
                        if new_data:
                            success = api_client.update_contact(contact_id, new_data)
                        
                        if success and tag_value:
                            logger.info("  Dodaję TAG '%s' osobnym wywołaniem...", tag_value)
                            if not api_client.add_tags_to_contact(contact_id, [tag_value]):
                                logger.warning("  ⚠ Nie udało się dodać TAGa, ale kontakt zaktualizowany")
                        
                        if success:
                            row_idx = dup["row_index"] - 1
                            if row_idx < len(result_df):
                                result_df.at[row_idx, "Status"] = "DUPLIKAT_ZAKTUALIZOWANY"
                            updated_count += 1
                            print("   ✅ Zaktualizowano")
                            logger.info("✅ Zaktualizowano %s (ID: %s)", dup["name"], contact_id)
                        else:
                            print("   ✗ Błąd aktualizacji")
                            logger.error("✗ Nie udało się zaktualizować %s (ID: %s)", dup["name"], contact_id)
                            skipped_count += 1
                            
                    except Exception as exc:
                        logger.error("Błąd aktualizacji %s: %s", dup["name"], exc, exc_info=True)
                        skipped_count += 1
            else:
                print(f"   ✗ Nieprawidłowy wybór - pomijam grupę")
                logger.warning("Nieprawidłowy wybór dla grupy %d - pomijam", group_num)
                skipped_count += len(group_items)
        
        print(f"\n📊 Podsumowanie aktualizacji:")
        print(f"   ✅ Zaktualizowano: {updated_count}/{len(duplicates_to_update)}")
        print(f"   ⊘ Pominięto: {skipped_count}")
        logger.info("Zakończono aktualizację: %d zaktualizowanych, %d pominiętych", updated_count, skipped_count)
    
    # Tworzenie nowych kontaktów
    if new_contacts_to_create:
        logger.info("\n➕ Znaleziono %d nowych kontaktów do utworzenia", len(new_contacts_to_create))
        
        if ask_for_create_confirmation(new_contacts_to_create):
            logger.info("Rozpoczynam tworzenie %d kontaktów...", len(new_contacts_to_create))
            created_count = 0
            
            for contact in tqdm(new_contacts_to_create, desc="Tworzenie", unit="kontakt"):
                try:
                    contact_data = contact["data"].copy()
                    
                    # Wyciągnij TAG jeśli jest (wymaga osobnego endpointa)
                    tag_value = contact_data.pop("Tag", None)
                    
                    logger.info("Tworzę nowy kontakt: %s", contact["name"])
                    logger.info("  Dane: %s", contact_data)
                    
                    created_id = api_client.create_contact(contact_data)
                    if created_id:
                        # Dodaj TAG osobno jeśli był
                        if tag_value:
                            logger.info("  Dodaję TAG '%s' do nowo utworzonego kontaktu...", tag_value)
                            if not api_client.add_tags_to_contact(created_id, [tag_value]):
                                logger.warning("  ⚠ Nie udało się dodać TAGa, ale kontakt utworzony")
                        
                        # Zaktualizuj status i ID w wynikach
                        row_idx = contact["row_index"] - 1
                        logger.info("  Aktualizuję wiersz %d w DataFrame (ID: %s)", row_idx, created_id)
                        
                        if row_idx < len(result_df):
                            result_df.at[row_idx, "Status"] = "NOWY_UTWORZONY"
                            result_df.at[row_idx, "Zoho_Contact_ID"] = created_id
                            logger.info("  ✓ Zapisano w DataFrame: Status=NOWY_UTWORZONY, Zoho_Contact_ID=%s", created_id)
                        else:
                            logger.error("  ✗ Błąd: row_idx %d >= len(result_df) %d", row_idx, len(result_df))
                        
                        created_count += 1
                        logger.info("✅ Utworzono %s (ID: %s)", contact["name"], created_id)
                    else:
                        logger.error("✗ Nie udało się utworzyć %s - API zwróciło None", contact["name"])
                except Exception as exc:
                    logger.error("Błąd tworzenia %s: %s", contact["name"], exc, exc_info=True)
            
            print(f"\n✅ Utworzono {created_count}/{len(new_contacts_to_create)} kontaktów")
            logger.info("Zakończono tworzenie: %d/%d sukces", created_count, len(new_contacts_to_create))
            
            # Pokaż podsumowanie utworzonych ID
            created_ids = result_df[result_df["Status"] == "NOWY_UTWORZONY"]["Zoho_Contact_ID"].tolist()
            logger.info("Utworzone ID w Zoho: %s", created_ids)
        else:
            logger.info("Użytkownik anulował tworzenie kontaktów")
    
    logger.info("🔷" * 40)
    logger.info("ZAKOŃCZONO FINALIZACJĘ OPERACJI")
    logger.info("🔷" * 40)
    
    return result_df


def main() -> int:
    parser = argparse.ArgumentParser(description="Sprawdzanie duplikatów w Zoho CRM")
    parser.add_argument("input_file", nargs="?")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--update-duplicates",
        action="store_true",
        help="Aktualizuj duplikaty w Zoho - dodaj nowe emaile/telefony do istniejących kontaktów"
    )
    args = parser.parse_args()

    input_path = clean_path(args.input_file) if args.input_file else ask_for_input_file()
    
    # Utwórz nazwę folderu z wynikami zawierającą nazwę pliku wejściowego
    input_file_name = Path(input_path).name
    output_dir = create_output_directory(input_file_name)
    log_path = output_dir / "process.log"
    setup_logging(log_path, args.verbose)
    logger = logging.getLogger(__name__)

    try:
        config = load_config(args.config)
        access_token = get_access_token(config)

        api_client = ZohoAPIClient(access_token)
        
        # Test połączenia z API Zoho
        print("\n🔍 Testowanie połączenia z API Zoho...")
        logger.info("Testowanie połączenia z API Zoho...")
        try:
            if api_client.test_connection():
                print("✓ Połączenie z API Zoho działa poprawnie")
            else:
                print("⚠ Test połączenia nie powiódł się, ale token został odświeżony - kontynuuję...")
                logger.warning("Test połączenia zwrócił False, ale kontynuuję działanie")
        except Exception as exc:
            logger.warning("Test połączenia wywołał wyjątek: %s - kontynuuję mimo to", exc)
            print("⚠ Test połączenia nie powiódł się, ale kontynuuję (token odświeżony)...")
        
        duplicate_finder = DuplicateFinder(api_client)
        scorer = ContactScorer()
        company_matcher = CompanyMatcher(api_client)
        
        # Inicjalizuj wykrywanie Salutation (AI)
        salutation_detector = SalutationDetector()
        if salutation_detector.api_key:
            print(f"🤖 AI włączone - automatyczne wykrywanie zwrotu grzecznościowego (model: {salutation_detector.model})")
            logger.info("Salutation detector aktywny (model: %s)", salutation_detector.model)
        else:
            print("⚠ AI wyłączone - brak API_KEY_OPENAI_medidesk")
            logger.warning("Salutation detector nieaktywny - brak klucza API")

        df_input, suffix = load_input_file(input_path)
        mapping = interactive_column_mapping(df_input)
        
        # Konfiguracja źródła kontaktów (TAGi, pola odniesienia)
        print("\n🔧 Czy chcesz skonfigurować pola źródła/TAGi dla nowych kontaktów?")
        logger.info("\n" + "=" * 80)
        logger.info("PYTAM O KONFIGURACJĘ ŹRÓDŁA KONTAKTÓW")
        
        configure_source = input("   (tak/nie, Enter=nie): ").strip().lower()
        logger.info("Odpowiedź użytkownika: '%s'", configure_source)
        
        source_config = None
        if configure_source in ["tak", "t", "yes", "y"]:
            logger.info("Użytkownik wybrał konfigurację źródła - rozpoczynam...")
            source_config = configure_source_fields(api_client, df_input)
        else:
            print("✓ Pominięto konfigurację źródła - nowe kontakty nie będą tworzone")
            logger.info("Pominięto konfigurację źródła - nowe kontakty nie będą tworzone")
        
        logger.info("=" * 80)

        # Jeśli parametr --update-duplicates podany, użyj go; w przeciwnym razie zawsze tryb "sprawdź i zapytaj"
        # Tryb True = po analizie zapyta o aktualizację jeśli znajdzie nowe dane
        update_mode = args.update_duplicates or True  # Zawsze True - zapyta później
        
        if args.update_duplicates:
            print("\n📊 Tryb: Automatyczna aktualizacja (parametr --update-duplicates)")
            logger.info("Uruchomiono z parametrem --update-duplicates")
        else:
            print("\n📊 Tryb: Analiza z możliwością aktualizacji")
            logger.info("Uruchomiono w trybie interaktywnym")
        
        # Przetwarzanie kontaktów - zwraca: wyniki, duplikaty do aktualizacji, nowe kontakty
        df_output, duplicates_to_update, new_contacts_to_create = process_contacts(
            df_input, mapping, api_client, duplicate_finder, scorer, 
            update_mode, source_config, salutation_detector
        )
        
        # Finalizuj operacje (aktualizacja/tworzenie) - zapyta użytkownika
        df_output = finalize_operations(
            df_output, duplicates_to_update, new_contacts_to_create, 
            api_client, update_mode
        )
        
        # Dopasuj kontakty do firm po domenie emaila
        print("\n🔧 Czy uruchomić dopasowanie kontaktów do firm po domenie emaila?")
        print("   (Dla kontaktów bez firmy, z emailem firmowym)")
        match_companies = input("   (tak/nie, Enter=nie): ").strip().lower()
        
        if match_companies in ["tak", "t", "yes", "y"]:
            logger.info("Użytkownik wybrał dopasowanie do firm - rozpoczynam...")
            df_output = match_contacts_to_companies(df_output, api_client, company_matcher)
        else:
            print("✓ Pominięto dopasowanie do firm")
            logger.info("Pominięto dopasowanie do firm")
        
        df_output = prepare_output_dataframe(df_output, mapping)

        input_copy = output_dir / f"input{suffix}"
        shutil.copy2(input_path, input_copy)

        output_file = output_dir / ("wyniki" + suffix)
        if suffix == ".csv":
            df_output.to_csv(output_file, index=False, encoding="utf-8-sig")
        else:
            df_output.to_excel(output_file, index=False)

        print("\n✓ Zakończono. Wyniki zapisane w:", output_file)
        logger.info("Proces zakończony pomyślnie")
        return 0

    except KeyboardInterrupt:
        print("\nPrzerwano przez użytkownika")
        logger.warning("Przerwano przez użytkownika")
        return 130
    except Exception as exc:
        print(f"\n❌ BŁĄD: {exc}")
        logger.exception("Krytyczny błąd programu")
        print(f"Szczegóły w logu: {log_path}")
        return 1


if __name__ == "__main__":
    sys.exit(main())