"""
Skrypt do wyświetlania aktywnych użytkowników Zoho CRM.
"""

import logging
import sys
from typing import List

from zoho_users import get_active_users, get_access_token

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def format_user(user: dict) -> str:
    return f"{user.get('full_name', user.get('name'))} ({user.get('id')}) – {user.get('role_name')} / {user.get('profile_name')}"


def main() -> None:
    print("\n==============================")
    print("LISTA AKTYWNYCH UŻYTKOWNIKÓW ZOHO CRM")
    print("==============================\n")

    token = get_access_token()
    users = get_active_users(token)
    if not users:
        print("Nie znaleziono aktywnych użytkowników.")
        sys.exit(1)

    print(f"Znaleziono {len(users)} aktywnych użytkowników:\n")
    for idx, user in enumerate(users, 1):
        print(f"{idx:2d}. {format_user(user)}")
    print("\nZapisz interesujące ID (np. użytkownika Mateusza Podlewskiego) i użyj ich w dalszych skryptach.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logging.error(f"Błąd: {exc}")
        sys.exit(1)
