@echo off
chcp 65001 >nul
echo.
echo ====================================================
echo  NAPRAWA SRODOWISKA VENV
echo ====================================================
echo.

REM Usun stary venv jesli istnieje
if exist venv (
    echo Usuwanie starego venv...
    rd /s /q venv
    if exist venv (
        echo BLAD: Nie udalo sie usunac starego venv!
        echo Zamknij wszystkie programy ktore moga go uzywac i sprobuj ponownie.
        pause
        exit /b 1
    )
    echo OK - stary venv usuniety
)

echo.
echo Tworzenie nowego venv...
py -m venv venv
if errorlevel 1 (
    echo BLAD: Nie udalo sie utworzyc venv!
    pause
    exit /b 1
)
echo OK - venv utworzony

echo.
echo Instalacja bibliotek...
venv\Scripts\pip.exe install pandas openpyxl tqdm colorama
if errorlevel 1 (
    echo BLAD: Nie udalo sie zainstalowac bibliotek!
    pause
    exit /b 1
)

echo.
echo ====================================================
echo  SUKCES! Venv naprawiony.
echo  Mozesz teraz uruchomic CREATE_LEADS.bat
echo ====================================================
pause
