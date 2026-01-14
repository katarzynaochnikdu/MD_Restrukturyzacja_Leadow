# Przykładowy skrypt do ustawiania zmiennych środowiskowych
# Skopiuj do setup_env.ps1 i uzupełnij własnymi wartościami

# ⚠️ UWAGA: Ten plik jest w .gitignore - bezpiecznie uzupełnij credentials

# Ustawienie zmiennych dla bieżącej sesji PowerShell
$env:ZOHO_MEDIDESK_CLIENT_ID="WPISZ_CLIENT_ID"
$env:ZOHO_MEDIDESK_CLIENT_SECRET="WPISZ_CLIENT_SECRET"
$env:ZOHO_MEDIDESK_REFRESH_TOKEN="WPISZ_REFRESH_TOKEN"

Write-Host "✓ Zmienne środowiskowe ustawione dla tej sesji PowerShell" -ForegroundColor Green
Write-Host ""
Write-Host "Aby ustawić je PERMANENTNIE (dla systemu):" -ForegroundColor Yellow
Write-Host "1. Wciśnij Win+R" -ForegroundColor Cyan
Write-Host "2. Wpisz: sysdm.cpl" -ForegroundColor Cyan
Write-Host "3. Zakładka 'Zaawansowane' → 'Zmienne środowiskowe'" -ForegroundColor Cyan
Write-Host "4. Dodaj w sekcji 'Zmienne użytkownika':" -ForegroundColor Cyan
Write-Host "   - ZOHO_MEDIDESK_CLIENT_ID" -ForegroundColor White
Write-Host "   - ZOHO_MEDIDESK_CLIENT_SECRET" -ForegroundColor White
Write-Host "   - ZOHO_MEDIDESK_REFRESH_TOKEN" -ForegroundColor White
Write-Host ""
Write-Host "Lub uruchom ten skrypt przed każdym użyciem cleanup_zoho.py:" -ForegroundColor Yellow
Write-Host "  . .\setup_env.ps1" -ForegroundColor Cyan

