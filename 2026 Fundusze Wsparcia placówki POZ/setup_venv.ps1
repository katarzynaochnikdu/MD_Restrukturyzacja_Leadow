# Skrypt PowerShell do stworzenia srodowiska wirtualnego Python 3.11 dla projektu

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Tworzenie srodowiska wirtualnego Python 3.11" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Sprawdz czy Python 3.11 jest dostepny
# Najpierw sprobuj uzyc 'py' launcher, potem 'python'
$pythonCmd = $null
$pythonVersion = $null

# Sprawdz czy 'py' launcher jest dostepny
try {
    $pyTest = py -3.11 --version 2>&1
    if ($pyTest -match "3\.11") {
        $pythonCmd = "py -3.11"
        $pythonVersion = $pyTest
    }
} catch {}

# Jesli py nie dziala, sprobuj python
if (-not $pythonCmd) {
    try {
        $pyVersion = python --version 2>&1
        if ($pyVersion -match "3\.11") {
            $pythonCmd = "python"
            $pythonVersion = $pyVersion
        }
    } catch {}
}

if (-not $pythonCmd -or ($pythonVersion -notmatch "3\.11")) {
    Write-Host "BLAD: Python 3.11 nie zostal znaleziony!" -ForegroundColor Red
    Write-Host "Prosze zainstalowac Python 3.11" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Mozesz sprawdzic dostepne wersje: py -0" -ForegroundColor Yellow
    Read-Host "Nacisnij Enter aby zakonczyc"
    exit 1
}

Write-Host "Znaleziono: $pythonVersion" -ForegroundColor Green
Write-Host ""

# Sprawdz czy venv juz istnieje
if (Test-Path "venv") {
    Write-Host "Srodowisko wirtualne juz istnieje!" -ForegroundColor Yellow
    Write-Host "Usun folder 'venv' jesli chcesz stworzyc nowe srodowisko." -ForegroundColor Yellow
    Read-Host "Nacisnij Enter aby zakonczyc"
    exit 1
}

# Utworz srodowisko wirtualne
Write-Host "Tworzenie srodowiska wirtualnego..." -ForegroundColor Green
if ($pythonCmd -eq "py -3.11") {
    py -3.11 -m venv venv
} else {
    python -m venv venv
}

if ($LASTEXITCODE -ne 0) {
    Write-Host "BLAD: Nie udalo sie utworzyc srodowiska wirtualnego!" -ForegroundColor Red
    Read-Host "Nacisnij Enter aby zakonczyc"
    exit 1
}

# Aktywuj srodowisko i zainstaluj zaleznosci
Write-Host ""
Write-Host "Aktywowanie srodowiska wirtualnego..." -ForegroundColor Green
& "venv\Scripts\Activate.ps1"

Write-Host ""
Write-Host "Instalowanie zaleznosci z requirements.txt..." -ForegroundColor Green
if ($pythonCmd -eq "py -3.11") {
    py -3.11 -m pip install --upgrade pip
    venv\Scripts\python.exe -m pip install -r requirements.txt
} else {
    python -m pip install --upgrade pip
    venv\Scripts\python.exe -m pip install -r requirements.txt
}

if ($LASTEXITCODE -ne 0) {
    Write-Host "BLAD: Nie udalo sie zainstalowac zaleznosci!" -ForegroundColor Red
    Read-Host "Nacisnij Enter aby zakonczyc"
    exit 1
}

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Srodowisko wirtualne zostalo pomyslnie utworzone!" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Aby aktywowac srodowisko w przyszlosci, uruchom:" -ForegroundColor Yellow
Write-Host "  venv\Scripts\Activate.ps1" -ForegroundColor White
Write-Host ""
Write-Host "Aby deaktywowac srodowisko, uruchom:" -ForegroundColor Yellow
Write-Host "  deactivate" -ForegroundColor White
Write-Host ""
Read-Host "Nacisnij Enter aby zakonczyc"
