@echo off
REM Skrypt do stworzenia środowiska wirtualnego Python 3.11 dla projektu

echo ============================================
echo Tworzenie środowiska wirtualnego Python 3.11
echo ============================================

REM Sprawdź czy Python 3.11 jest dostępny
python --version 2>nul | findstr /R /C:"3\.11" >nul
if %errorlevel% neq 0 (
    echo BŁĄD: Python 3.11 nie został znaleziony!
    echo Proszę zainstalować Python 3.11 lub zmienić ścieżkę w PATH
    echo.
    echo Możesz sprawdzić dostępne wersje: py -0
    pause
    exit /b 1
)

REM Sprawdź czy venv już istnieje
if exist "venv" (
    echo Środowisko wirtualne już istnieje!
    echo Usuń folder 'venv' jeśli chcesz stworzyć nowe środowisko.
    pause
    exit /b 1
)

REM Utwórz środowisko wirtualne
echo Tworzenie środowiska wirtualnego...
python -m venv venv

if %errorlevel% neq 0 (
    echo BŁĄD: Nie udało się utworzyć środowiska wirtualnego!
    pause
    exit /b 1
)

REM Aktywuj środowisko i zainstaluj zależności
echo.
echo Aktywowanie środowiska wirtualnego...
call venv\Scripts\activate.bat

echo.
echo Instalowanie zależności z requirements.txt...
python -m pip install --upgrade pip
pip install -r requirements.txt

if %errorlevel% neq 0 (
    echo BŁĄD: Nie udało się zainstalować zależności!
    pause
    exit /b 1
)

echo.
echo ============================================
echo Środowisko wirtualne zostało pomyślnie utworzone!
echo ============================================
echo.
echo Aby aktywować środowisko w przyszłości, uruchom:
echo   venv\Scripts\activate.bat
echo.
echo Aby deaktywować środowisko, uruchom:
echo   deactivate
echo.
pause
