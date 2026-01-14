import os
import sys

from zoho_oauth import get_initial_refresh_token

# Ten redirect URI musi być DOKŁADNIE taki sam, jak ten skonfigurowany
# w panelu Zoho API dla Twojego klienta (https://api-console.zoho.eu/)
REDIRECT_URI = "http://localhost:8000"

# Wymagane uprawnienia. `ZohoCRM.coql.READ` jest kluczowe dla naszego skryptu.
# Dodajemy uprawnienia do odczytu wszystkich potrzebnych modułów.

# Uprawnienia CRUD dla kluczowych modułów
CRUD_SCOPES = [
    "ZohoCRM.modules.leads.ALL",
    "ZohoCRM.modules.deals.ALL",
    "ZohoCRM.modules.tasks.ALL",
    "ZohoCRM.modules.events.ALL",
    "ZohoCRM.modules.calls.ALL",
    "ZohoCRM.modules.contacts.ALL",
    "ZohoCRM.modules.accounts.ALL",
]

# Uprawnienia READ dla pozostałych modułów
READ_SCOPES = [
    "ZohoCRM.modules.campaigns.READ",
    "ZohoCRM.users.READ",
    # Moduły niestandardowe (zakładając ich nazwy API)
    "ZohoCRM.modules.custom.All",
    #"ZohoCRM.modules.custom.USER_Historia.READ",
    #"ZohoCRM.modules.custom.Marketing_Leads.READ",
    #"ZohoCRM.modules.custom.Ankiety_Spotkan.READ",
    #"ZohoCRM.modules.custom.MLxKontakt.READ",
    #"ZohoCRM.modules.custom.Historyczne_Dane_Firm.READ",
    #"ZohoCRM.modules.custom.TTP.READ",
    #"ZohoCRM.modules.custom.GRUPY.READ",
]


# Łączymy wszystkie uprawnienia w jeden string
ALL_SCOPES = ",".join(CRUD_SCOPES + READ_SCOPES + ["ZohoCRM.coql.READ"])


def main():
    """
    Interaktywny skrypt do generowania nowego refresh tokena Zoho.
    """
    print("--- Generator nowego Refresh Tokena Zoho ---")
    print(
        f"Upewnij się, że w konfiguracji klienta API w Zoho masz ustawiony Redirect URI na: {REDIRECT_URI}"
    )
    print("-" * 50)

    try:
        client_id = os.environ.get("ZOHO_MEDIDESK_CLIENT_ID") or input(
            "Podaj swój Client ID: "
        ).strip()
        client_secret = os.environ.get("ZOHO_MEDIDESK_CLIENT_SECRET") or input(
            "Podaj swój Client Secret: "
        ).strip()

        if not client_id or not client_secret:
            print("Client ID i Client Secret nie mogą być puste.", file=sys.stderr)
            sys.exit(1)

        print("\nOtwieram przeglądarkę w celu autoryzacji...")
        print(
            "Zaloguj się na swoje konto Zoho, a następnie zaakceptuj uprawnienia dla aplikacji."
        )

        refresh_token = get_initial_refresh_token(
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=REDIRECT_URI,
            scopes=ALL_SCOPES,
        )

        print("\n" + "=" * 50)
        print("Pomyślnie wygenerowano nowy Refresh Token!")
        print("Oto Twój nowy token:")
        print(f"\n{refresh_token}\n")
        print("=" * 50)
        print(
            "Skopiuj powyższy token i zaktualizuj swoją zmienną środowiskową ZOHO_MEDIDESK_REFRESH_TOKEN."
        )

    except Exception as e:
        print(f"\nWystąpił błąd: {e}", file=sys.stderr)
        print(
            "Upewnij się, że podałeś poprawne dane i że serwer na porcie 8000 nie jest blokowany.",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()

