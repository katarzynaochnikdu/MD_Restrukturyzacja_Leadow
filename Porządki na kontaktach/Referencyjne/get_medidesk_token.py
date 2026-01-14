import os
import sys
from refresh_zoho_access_token import refresh_access_token


def main() -> None:
    """
    Pobiera token dostępowy dla Medidesk i drukuje go na standardowe wyjście.
    """
    print("Rozpoczynam pobieranie nowego tokena dostępowego dla Medidesk...")

    # Użyj zmiennych środowiskowych lub wpisz dane na stałe
    client_id = os.environ.get("ZOHO_MEDIDESK_CLIENT_ID", "WPISZ_CLIENT_ID")
    client_secret = os.environ.get("ZOHO_MEDIDESK_CLIENT_SECRET", "WPISZ_CLIENT_SECRET")
    refresh_token = os.environ.get("ZOHO_MEDIDESK_REFRESH_TOKEN", "WPISZ_REFRESH_TOKEN")

    # Prosta walidacja, czy dane zostały podane
    if any(
        val.startswith("WPISZ_") for val in [client_id, client_secret, refresh_token]
    ):
        print(
            "Błąd: Dane (client_id, client_secret, refresh_token) nie zostały skonfigurowane.",
            file=sys.stderr,
        )
        print(
            "Ustaw zmienne środowiskowe (np. ZOHO_MEDSPACE_CLIENT_ID) lub wpisz je bezpośrednio w pliku.",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        token_info = refresh_access_token(
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
        )
        print("Pomyślnie pobrano nowy token.")
        print(token_info["access_token"])
    except RuntimeError as e:
        print(f"Wystąpił błąd podczas pobierania tokena: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
