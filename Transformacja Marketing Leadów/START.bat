@echo off
REM Skrypt startowy dla Windows
REM Uruchamia główny program workflow

echo.
echo ====================================================
echo  TRANSFORMACJA LEADOW - PROGRAM GLOWNY
echo ====================================================
echo.
echo Uruchamianie...
echo.

REM Sprawdź czy venv istnieje
if exist venv\Scripts\activate.bat (
    echo Aktywacja srodowiska wirtualnego...
    call venv\Scripts\activate.bat
)

REM Uruchom główny program
python main_workflow.py

REM Jeśli program się zakończył błędem, poczekaj na Enter
if errorlevel 1 (
    echo.
    echo Program zakonczyl sie bledem!
    pause
)
