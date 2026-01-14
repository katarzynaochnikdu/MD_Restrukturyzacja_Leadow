# Szybki start

## 1. Instalacja

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
copy config.json.example config.json
```

Uzupełnij `config.json` danymi OAuth Zoho.

## 2. Uruchomienie

```bash
python check_contacts.py
```

Program zapyta o plik wejściowy – możesz przeciągnąć go do terminala.

## 3. Wyniki

Po zakończeniu pracy w folderze `wyniki/<timestamp>/` znajdziesz:

- `input.*` – kopia pliku źródłowego
- `wyniki.*` – raport z duplikatami
- `process.log` – log procesu

