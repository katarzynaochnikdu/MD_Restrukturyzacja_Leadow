@echo off
chcp 65001 >nul
REM Skrypt do masowego tworzenia leadów z pliku CSV/XLSX

echo.
echo ====================================================
echo  TWORZENIE LEADOW Z PLIKU
echo ====================================================
echo.

REM Sprawdz czy venv istnieje
if not exist venv\Scripts\python.exe (
    echo BLAD: Brak srodowiska venv!
    echo Uruchom najpierw NAPRAW_VENV.bat
    pause
    exit /b 1
)

REM Uruchom skrypt przez venv
echo.
venv\Scripts\python.exe create_leads_from_file.py %*

echo.
echo ====================================================
echo  ZAKONCZONO
echo ====================================================
pause
