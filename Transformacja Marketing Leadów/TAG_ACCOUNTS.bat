@echo off
chcp 65001 >nul
REM Skrypt do tagowania firm (Accounts) z pliku CSV/XLSX

echo.
echo ====================================================
echo  TAGOWANIE FIRM (ACCOUNTS)
echo ====================================================
echo.

REM Sprawdz czy venv istnieje
if not exist venv\Scripts\python.exe (
    echo BLAD: Brak srodowiska venv!
    echo Uruchom najpierw NAPRAW_VENV.bat
    pause
    exit /b 1
)

REM Uruchom skrypt
venv\Scripts\python.exe tag_accounts.py %*

echo.
echo ====================================================
echo  ZAKONCZONO
echo ====================================================
pause
