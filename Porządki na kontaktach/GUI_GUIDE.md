# 🌐 Web GUI - Przewodnik użytkownika

## 🚀 Szybki start

### 1. Instalacja Flask (jednorazowo)

```bash
pip install flask
```

### 2. Uruchomienie

```bash
python cleanup_zoho.py
```

**Wybierz opcję:**
```
Wybierz tryb pracy:

  [1] Tryb terminalowy (CLI) - klasyczny, z potwierdzeniami w konsoli
  [2] Web GUI w przeglądarce - interaktywny interfejs graficzny

Wybór (1/2, domyślnie: 1): 2
```

**Alternatywnie** - bezpośrednio:
```bash
python cleanup_zoho.py --gui
```

### 3. Przeglądarka otworzy się automatycznie

URL: http://localhost:5000

---

## 🎨 Interfejs użytkownika

### Ekran 1: Konfiguracja

```
┌─────────────────────────────────────────┐
│ Tryb:  [DRY-RUN (Symulacja)      ▼]    │
│                                         │
│ Limit par duplikatów: [10         ]    │
│                                         │
│         [ ROZPOCZNIJ ]                  │
└─────────────────────────────────────────┘
```

**Parametry:**
- **Tryb**: DRY-RUN (zalecane) lub PRODUKCJA
- **Limit**: Ile par duplikatów szukać (10-100 dla testów)

### Ekran 2: Scalanie (dla każdej pary)

**🎨 PROFESJONALNY INTERFEJS GRAFICZNY**

#### Nagłówek z postępem:
```
┌───────────────────────────────────────────────────────────────────┐
│ 📊 Postęp scalania                              [1 / 15]         │
│ ▓▓▓▓▓▓▓░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  7%             │
│                                                                   │
│ [0] Łącznie  [8] ⚡Proste  [7] ⚠️Złożone  [0] ✓Ukończone       │
└───────────────────────────────────────────────────────────────────┘
```

#### Panel auto-scalania (jeśli są proste przypadki):
```
┌───────────────────────────────────────────────────────────────────┐
│ ⚡ AUTO-SCALANIE DOSTĘPNE!                                        │
│                                                                   │
│ Wykryto 8 prostych przypadków (slave score < 5)                  │
│ Możesz scalić je automatycznie jednym kliknięciem                │
│                                                                   │
│                    [ ⚡ SCALIĆ WSZYSTKIE PROSTE ]                │
└───────────────────────────────────────────────────────────────────┘
```

#### Porównanie Master vs Slave:
```
┌──────────────────────────────┬──────────────────────────────┐
│ 👑 MASTER (Zachowaj)         │ 📦 SLAVE (Scal i usuń)       │
├──────────────────────────────┼──────────────────────────────┤
│ FIRMA A SP Z O.O.            │ Firma A sp z o.o.            │
│                              │                              │
│ Score: 25                    │ Score: 10                    │
│ ID: 751364000123             │ ID: 751364000456             │
│ NIP: 1234567890              │ NIP: 123-456-78-90           │
│ Wypełnionych pól: 18         │ Wypełnionych pól: 8          │
│ Powiązań: 45 rekordów        │ Powiązań: 5 rekordów         │
└──────────────────────────────┴──────────────────────────────┘
```

#### Tabela edytora pół (interaktywna):
```
┌──────────────────────────────────────────────────────────────────┐
│ 📝 EDYTOR PÓL                    ⚠️ 2 konflikty  ✏️ 3 edycje   │
├────────────────┬─────────────┬─────────────┬───────────────────┤
│ Pole           │ Master      │ Slave       │ Wybór             │
├────────────────┼─────────────┼─────────────┼───────────────────┤
│ Nazwa firmy    │ FIRMA A...  │ Firma A...  │ [✓ Master    ▼]  │
│ Nazwa zwycz.   │ (puste)     │ Firma A     │ [← Slave     ▼]  │
│ ⚠️ NIP         │ 1234567890  │ 123-456-... │ [✓ Master    ▼]  │
│ Telefon        │ (puste)     │ 22 123...   │ [← Slave     ▼]  │
│ Email          │ a@x.pl      │ (puste)     │ [✓ Master    ▼]  │
│ Strona www     │ https://... │ (puste)     │ [✓ Master    ▼]  │
└────────────────┴─────────────┴─────────────┴───────────────────┘

Opcje w dropdown dla każdego pola:
  • ✓ Master - użyj wartości z Master
  • ← Slave - użyj wartości ze Slave
  • ✏️ Własna... - wpisz własną wartość (pojawia się pole tekstowe)
```

#### Wizualizacja powiązań:
```
┌───────────────────────────────────────────────────────────────────┐
│ 🔗 POWIĄZANIA DO PRZENIESIENIA                                    │
├───────────────────────────────────────────────────────────────────┤
│                                                                   │
│ [Contacts: 5]  [Leads: 2]  [Deals: 1]  [Klienci: 3]             │
│ [Tasks: 8]     [Calls: 12] [Events: 4] [Invoices: 2]            │
│                                                                   │
│ Każdy moduł pokazany jako kolorowa karta z liczbą rekordów       │
└───────────────────────────────────────────────────────────────────┘
```

#### Podsumowanie operacji:
```
┌───────────────────────────────────────────────────────────────────┐
│ 📋 PODSUMOWANIE OPERACJI                                          │
├───────────────────────────────────────────────────────────────────┤
│ Pola do skopiowania: 3                                            │
│ Powiązania (rekordy): 37                                          │
│ Tagi do przeniesienia: 2                                          │
│ Slave zostanie: USUNIĘTY (score < 5)                             │
└───────────────────────────────────────────────────────────────────┘
```

#### Przyciski akcji (duże, kolorowe):
```
┌───────────────────────────────────────────────────────────────────┐
│                                                                   │
│  [  POMIŃ  ]         [  PRZERWIJ  ]      [ ✅ SCALIĆ I KONTYNUUJ ]│
│   (szary)             (czerwony)              (zielony, duży)     │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
```

**🎨 Design Features:**
- Gradient backgrounds (indigo → purple)
- Animacje fade-in przy ładowaniu
- Hover effects na polach
- Konflikty podświetlone na pomarańczowo
- Ikony Font Awesome
- Tailwind CSS
- Responsywny design

---

## ⚡ Funkcje specjalne

### Auto-scalanie prostych przypadków

Jeśli są pary gdzie **Slave ma score < 5**, zobaczysz panel:

```
╔══════════════════════════════════════════════════════════╗
║ ⚡ Auto-scalanie prostych przypadków                     ║
║                                                          ║
║ Znaleziono 8 par gdzie Slave ma bardzo niski scoring    ║
║ Można je bezpiecznie scalić automatycznie               ║
║                                                          ║
║                     [ ⚡ ZATWIERDŹ 8 PROSTYCH ]          ║
╚══════════════════════════════════════════════════════════╝
```

**Kliknięcie:**
- Automatycznie scali wszystkie "proste" przypadki
- Pominiesz 8 par jednym klikiem!
- Zostają tylko złożone przypadki do ręcznej weryfikacji

---

## 🎯 Porównanie: CLI vs Web GUI

| Funkcja | CLI | Web GUI |
|---------|-----|---------|
| Łatwość użycia | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Wizualizacja | Tekst | Grafika |
| Edycja pól | ❌ Tylko wybór Master/Slave | ✅ Master/Slave/Własna wartość |
| Preview | Logi | Wizualny podgląd |
| Auto-merge | ❌ | ✅ Proste przypadki jednym klikiem |
| Instalacja | Brak | `pip install flask` |

**Zalecenie:** Web GUI dla pierwszego użycia i produkcji, CLI dla automatyzacji/skryptów.

---

## 💡 Wskazówki

### Web GUI

1. **Zawsze zacznij od DRY-RUN** (domyślne)
2. **Limit 10** dla pierwszego testu
3. **Auto-merge** tylko dla sprawdzonych przypadków
4. **Edytuj pola** tylko jeśli widzisz konflikt

### Troubleshooting

**Błąd: "Brak modułu Flask"**
```bash
pip install flask
```

**GUI nie otwiera się:**
- Otwórz ręcznie: http://localhost:5000
- Sprawdź czy port 5000 jest wolny

**Błąd połączenia:**
- Sprawdź zmienne środowiskowe (credentials)
- Zobacz logi w konsoli

**⚠️ UWAGA: Równoległe uruchomienia**

Jeśli uruchamiasz **2 programy jednocześnie** (np. CLI + Web GUI):
- ✅ Współdzielą ten sam token cache (`.zoho_token_cache.json`)
- ✅ Nie powielają odświeżania tokenu (oszczędność API calls)
- ⚠️ **ALE:** Jeśli token wygaśnie podczas pracy obu programów, mogą oba próbować odświeżyć
- 💡 **Rozwiązanie:** Cache ma margines 5 min - odświeża wcześniej żeby uniknąć kolizji

**Zalecenie:** Uruchamiaj programy sekwencyjnie (jeden po drugim) dla maksymalnego bezpieczeństwa.

---

## 📞 Skróty klawiszowe (w przeglądarce)

- **Enter** - Scalić i kontynuuj
- **ESC** - Pomiń parę
- **Ctrl+Q** - Przerwij proces

---

**Miłego scalania!** 🎉

