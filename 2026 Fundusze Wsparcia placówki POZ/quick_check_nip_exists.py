import argparse
import json
import os
import sys
import urllib.request
import urllib.error
import pandas as pd
try:
    from tqdm import tqdm
except Exception:
    tqdm = None
from datetime import datetime

# Minimalny, odrębny skrypt: wejście CSV/XLSX z NIP‑ami, wyjście XLSX z dwiema kolumnami: NIP, ISTNIEJE (TAK/NIE)

ZOHO_CRM_COQL_URL = "https://www.zohoapis.eu/crm/v3/coql"

try:
    from refresh_zoho_access_token import refresh_access_token
except Exception:
    print("Brak modułu refresh_zoho_access_token.py – uzupełnij konfigurację.", file=sys.stderr)
    sys.exit(1)


def get_access_token() -> str:
    client_id = os.environ.get("ZOHO_MEDIDESK_CLIENT_ID", "WPISZ_CLIENT_ID")
    client_secret = os.environ.get("ZOHO_MEDIDESK_CLIENT_SECRET", "WPISZ_CLIENT_SECRET")
    refresh_token = os.environ.get("ZOHO_MEDIDESK_REFRESH_TOKEN", "WPISZ_REFRESH_TOKEN")
    if any(val.startswith("WPISZ_") for val in [client_id, client_secret, refresh_token]):
        print("Błąd: brak danych OAuth w zmiennych środowiskowych.", file=sys.stderr)
        sys.exit(1)
    info = refresh_access_token(client_id=client_id, client_secret=client_secret, refresh_token=refresh_token)
    return info["access_token"]


def verify_nip_exists(access_token: str, nip: str) -> bool:
    nip = (str(nip) or "").strip().replace("-", "")
    if not (len(nip) == 10 and nip.isdigit()):
        # Wymaganie: tylko TAK/NIE; dla niepoprawnych traktujemy jako NIE (nie odpytywać API)
        return False
    payload = json.dumps({"select_query": f"select id from Accounts where Firma_NIP = '{nip}'"}).encode("utf-8")
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/json",
    }
    req = urllib.request.Request(ZOHO_CRM_COQL_URL, data=payload, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            text = resp.read().decode("utf-8")
            if not text:
                return False
            data = json.loads(text)
            return bool(data.get("data"))
    except urllib.error.HTTPError as e:
        # Jeśli token wygasł, spróbuj odświeżyć i ponowić raz
        if e.code in (401, 403):
            try:
                new_tok = get_access_token()
                headers["Authorization"] = f"Zoho-oauthtoken {new_tok}"
                req2 = urllib.request.Request(ZOHO_CRM_COQL_URL, data=payload, headers=headers, method="POST")
                with urllib.request.urlopen(req2, timeout=30) as resp2:
                    text2 = resp2.read().decode("utf-8")
                    if not text2:
                        return False
                    data2 = json.loads(text2)
                    return bool(data2.get("data"))
            except Exception:
                return False
        return False
    except Exception:
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Szybka weryfikacja istnienia NIP w Zoho (tylko NIP -> TAK/NIE w XLSX)")
    parser.add_argument("input_file", nargs="?", default=None, help="Ścieżka do CSV/XLSX z NIP‑ami (pierwsza kolumna)")
    args = parser.parse_args()

    path = args.input_file
    if not path:
        raw_path = input("Przeciągnij plik do konsoli lub wklej ścieżkę i Enter: ")
        clean_path = raw_path.strip()
        if clean_path.startswith("& "):
            clean_path = clean_path[2:].strip()
        path = clean_path.strip("'\"")
    if not os.path.exists(path):
        print(f"Błąd: plik '{path}' nie istnieje.", file=sys.stderr)
        sys.exit(1)

    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(path, dtype=str, sep=';')
    elif ext in (".xlsx", ".xls"):
        df = pd.read_excel(path, dtype=str)
    else:
        print("Nieobsługiwany format (użyj .csv albo .xlsx)", file=sys.stderr)
        sys.exit(1)

    if df.empty:
        print("Plik wejściowy jest pusty.", file=sys.stderr)
        sys.exit(0)

    nip_col = df.columns[0]
    token = get_access_token()

    results = []
    iterator = df.iterrows()
    if tqdm is not None:
        iterator = tqdm(df.iterrows(), total=len(df), desc="Weryfikacja NIP-ów", unit=" wiersz")
    for _, row in iterator:
        nip_val = row.get(nip_col)
        exists = verify_nip_exists(token, nip_val)
        results.append({"NIP": str(nip_val) if nip_val is not None else "", "ISTNIEJE": "TAK" if exists else "NIE"})

    out_dir = os.path.join("wyniki_czy_istnieje_prosty", datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "wynik.xlsx")
    pd.DataFrame(results).to_excel(out_path, index=False, engine='openpyxl')
    print(f"Zapisano: {out_path}")


if __name__ == "__main__":
    main()


