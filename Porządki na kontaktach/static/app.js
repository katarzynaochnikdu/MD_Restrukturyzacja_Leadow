// Zoho Data Cleanup - Web GUI JavaScript

let currentState = {
    totalPairs: 0,
    currentIndex: 0,
    simpleCount: 0,
    complexCount: 0,
    doneCount: 0,
    currentPair: null,
    editedFields: {},
    conflicts: 0
};

// Initialize system
async function initializeSystem() {
    const dryRun = document.getElementById('init-dry-run').value === 'true';
    const limit = parseInt(document.getElementById('init-limit').value);
    
    showGlobalLoading('Inicjalizacja systemu...', 'Pobieranie firm i szukanie duplikatów...');
    
    // Start progress polling
    const progressInterval = startProgressPolling();
    
    try {
        const response = await fetch('/api/init', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ limit, dry_run: dryRun })
        });
        
        clearInterval(progressInterval);
        const result = await response.json();
        
        if (!result.success) {
            alert('Błąd: ' + result.error);
            hideGlobalLoading();
            return;
        }
        
        // Update state
        currentState.totalPairs = result.total_pairs;
        currentState.simpleCount = result.simple_count;
        currentState.complexCount = result.complex_count;
        currentState.currentIndex = 0;
        currentState.doneCount = 0;
        
        // Show main interface
        document.getElementById('init-screen').style.display = 'none';
        document.getElementById('main-interface').style.display = 'block';
        
        updateModeBadge(dryRun);
        updateAllStats();
        
        // Show logs button
        document.getElementById('logs-button').style.display = 'inline-block';
        
        // Auto-merge panel
        if (result.simple_count > 0) {
            document.getElementById('auto-merge-panel').style.display = 'block';
            document.getElementById('auto-count-text').textContent = result.simple_count;
        }
        
        hideGlobalLoading();
        await loadCurrentPair();
        
    } catch (error) {
        alert('Błąd połączenia: ' + error.message);
        hideGlobalLoading();
    }
}

// Load current pair
async function loadCurrentPair() {
    console.log('loadCurrentPair: index=', currentState.currentIndex, 'total=', currentState.totalPairs);
    
    if (currentState.currentIndex >= currentState.totalPairs) {
        console.log('Wszystkie pary przetworzone - pokazuję ekran zakończenia');
        showFinishedScreen();
        return;
    }
    
    try {
        const response = await fetch('/api/get_current_pair');
        const result = await response.json();
        
        console.log('get_current_pair response:', result);
        
        if (result.finished) {
            console.log('API zwróciło finished=true');
            showFinishedScreen();
            return;
        }
        
        currentState.currentPair = result;
        currentState.editedFields = {};
        
        console.log('Wyświetlam parę:', result.master?.Account_Name, 'vs', result.slave?.Account_Name);
        
        displayPair(result);
        await loadFieldsEditor(result.master, result.slave);
        
    } catch (error) {
        console.error('Błąd ładowania pary:', error);
        alert('Błąd ładowania: ' + error.message);
    }
}

// Display pair
function displayPair(pair) {
    const master = pair.master;
    const slave = pair.slave;
    const masterScore = master._score || {};
    const slaveScore = slave._score || {};
    
    // Header
    document.getElementById('merge-title').textContent = pair.key;
    document.getElementById('merge-subtitle').textContent = `Duplikat wykryty: ${pair.key.split(':')[0]}`;
    document.getElementById('pair-number').textContent = `${currentState.currentIndex + 1}/${currentState.totalPairs}`;
    
    // Safe badge
    document.getElementById('safe-badge').style.display = pair.auto_merge_safe ? 'inline-block' : 'none';
    
    // Master
    document.getElementById('master-name').textContent = master.Account_Name || 'Bez nazwy';
    document.getElementById('master-id').textContent = master.id;
    document.getElementById('master-score').textContent = masterScore.total_score || '0';
    document.getElementById('master-nip').textContent = master.Firma_NIP || '(brak)';
    document.getElementById('master-fields').textContent = masterScore.AccountScoreDetale || '0';
    document.getElementById('master-relations').textContent = masterScore.AccountScorePowiazaniaRekordyModulow || '0';
    
    // Slave
    document.getElementById('slave-name').textContent = slave.Account_Name || 'Bez nazwy';
    document.getElementById('slave-id').textContent = slave.id;
    document.getElementById('slave-score').textContent = slaveScore.total_score || '0';
    document.getElementById('slave-nip').textContent = slave.Firma_NIP || '(brak)';
    document.getElementById('slave-fields').textContent = slaveScore.AccountScoreDetale || '0';
    document.getElementById('slave-relations').textContent = slaveScore.AccountScorePowiazaniaRekordyModulow || '0';
}

// Load fields editor
async function loadFieldsEditor(master, slave) {
    try {
        const response = await fetch('/api/get_merge_fields', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ master, slave })
        });
        
        const result = await response.json();
        if (!result.success) return;
        
        const tbody = document.getElementById('fields-table-body');
        tbody.innerHTML = '';
        
        currentState.conflicts = 0;
        
        result.fields.forEach(field => {
            if (field.has_conflict) currentState.conflicts++;
            
            const row = document.createElement('tr');
            row.className = `field-row ${field.has_conflict ? 'conflict-marker' : ''}`;
            row.innerHTML = `
                <td class="px-6 py-4">
                    <div class="font-bold text-gray-900">${field.label}</div>
                    <div class="text-xs text-gray-500 font-mono">${field.name}</div>
                    ${field.has_conflict ? '<div class="text-xs text-orange-600 font-bold mt-1"><i class="fas fa-exclamation-triangle mr-1"></i>KONFLIKT</div>' : ''}
                </td>
                <td class="px-6 py-4">
                    <div class="font-semibold text-green-700">${field.master_value || '<span class="text-gray-400 italic">(puste)</span>'}</div>
                </td>
                <td class="px-6 py-4">
                    <div class="font-semibold text-orange-700">${field.slave_value || '<span class="text-gray-400 italic">(puste)</span>'}</div>
                </td>
                <td class="px-6 py-4">
                    <select onchange="updateFieldChoice('${field.name}', this.value, this)" 
                            class="w-full px-4 py-2 border-2 border-gray-300 rounded-lg font-semibold focus:border-indigo-500 focus:ring-2 focus:ring-indigo-200 transition-all">
                        <option value="master" ${field.default_source === 'master' ? 'selected' : ''}>✓ Master</option>
                        <option value="slave" ${field.default_source === 'slave' ? 'selected' : ''}>← Slave</option>
                        <option value="custom">✏️ Własna...</option>
                    </select>
                    <input type="text" id="custom-${field.name}" placeholder="Wpisz wartość" 
                           class="w-full px-4 py-2 border-2 border-indigo-400 rounded-lg mt-2 font-semibold focus:border-indigo-600 focus:ring-2 focus:ring-indigo-200"
                           style="display: none;"
                           onchange="updateCustomValue('${field.name}', this.value)">
                </td>
            `;
            tbody.appendChild(row);
        });
        
        document.getElementById('conflicts-count').textContent = currentState.conflicts;
        updatePreviewSummary();
        
    } catch (error) {
        console.error('Błąd ładowania pól:', error);
    }
}

// Update field choice
function updateFieldChoice(fieldName, source, selectElement) {
    const customInput = document.getElementById(`custom-${fieldName}`);
    const row = selectElement.closest('tr');
    
    if (source === 'custom') {
        customInput.style.display = 'block';
        row.classList.add('selected-field');
        currentState.editedFields[fieldName] = { source: 'custom', custom_value: customInput.value };
    } else {
        customInput.style.display = 'none';
        row.classList.remove('selected-field');
        currentState.editedFields[fieldName] = { source };
    }
    
    document.getElementById('edited-count').textContent = Object.keys(currentState.editedFields).length;
    updatePreviewSummary();
}

// Update custom value
function updateCustomValue(fieldName, value) {
    currentState.editedFields[fieldName] = { source: 'custom', custom_value: value };
    updatePreviewSummary();
}

// Update preview summary
function updatePreviewSummary() {
    const fieldsCount = Object.keys(currentState.editedFields).filter(k => 
        currentState.editedFields[k].source !== 'master'
    ).length;
    
    document.getElementById('summary-fields').textContent = fieldsCount;
}

// Merge current pair
async function mergePair() {
    if (!currentState.currentPair) {
        alert('Brak aktualnej pary do scalenia');
        return;
    }
    
    console.log('Scalanie pary:', currentState.currentIndex, 'z', currentState.totalPairs);
    
    showGlobalLoading('Scalanie firm...', 'Przenoszenie danych i powiązań...');
    
    try {
        const response = await fetch('/api/merge_pair', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                pair_index: currentState.currentIndex,
                edited_fields: currentState.editedFields
            })
        });
        
        const result = await response.json();
        
        if (!result.success) {
            alert('Błąd scalania: ' + result.error);
            hideGlobalLoading();
            return;
        }
        
        console.log('Scalono pomyślnie, przechodzę do następnej pary');
        
        // Move to next
        currentState.currentIndex++;
        currentState.doneCount++;
        
        console.log('Nowy index:', currentState.currentIndex, '/', currentState.totalPairs);
        
        updateAllStats();
        hideGlobalLoading();
        
        // Load next pair
        await loadCurrentPair();
        
    } catch (error) {
        console.error('Błąd scalania:', error);
        alert('Błąd: ' + error.message);
        hideGlobalLoading();
    }
}

// Skip pair
async function skipPair() {
    console.log('Pomijam parę, obecny index:', currentState.currentIndex);
    
    const newIndex = currentState.currentIndex + 1;
    
    await fetch('/api/skip_pair', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ new_index: newIndex })
    });
    
    currentState.currentIndex = newIndex;
    
    console.log('Nowy index po pominięciu:', currentState.currentIndex);
    
    updateAllStats();
    await loadCurrentPair();
}

// Quit process
function quitProcess() {
    if (confirm('Czy na pewno chcesz przerwać? Postęp zostanie zapisany.')) {
        showFinishedScreen();
    }
}

// Approve all simple
async function approveAllSimple() {
    if (!confirm(`Scalić automatycznie ${currentState.simpleCount} prostych przypadków?`)) return;
    
    showGlobalLoading('Auto-scalanie...', `Scalanie ${currentState.simpleCount} prostych par...`);
    
    try {
        const response = await fetch('/api/approve_all_simple', { method: 'POST' });
        const result = await response.json();
        
        if (result.success) {
            alert(`✅ Scalono ${result.merged} par!`);
            currentState.doneCount += result.merged;
            updateAllStats();
        }
        
        hideGlobalLoading();
        window.location.reload();
        
    } catch (error) {
        alert('Błąd: ' + error.message);
        hideGlobalLoading();
    }
}

// Update all statistics
function updateAllStats() {
    const percent = currentState.totalPairs > 0 ? (currentState.currentIndex / currentState.totalPairs) * 100 : 0;
    
    document.getElementById('progress-bar').style.width = `${percent}%`;
    document.getElementById('progress-percent').textContent = `${Math.round(percent)}%`;
    document.getElementById('progress-text').textContent = `${currentState.currentIndex} / ${currentState.totalPairs}`;
    
    document.getElementById('stat-total').textContent = currentState.totalPairs;
    document.getElementById('stat-simple').textContent = currentState.simpleCount;
    document.getElementById('stat-complex').textContent = currentState.complexCount;
    document.getElementById('stat-done').textContent = currentState.doneCount;
}

// Update mode badge
function updateModeBadge(dryRun) {
    const container = document.getElementById('mode-badge-container');
    const badge = document.getElementById('mode-badge');
    
    if (dryRun) {
        container.className = 'inline-flex items-center px-5 py-3 rounded-xl shadow-lg bg-green-100 text-green-800';
        badge.textContent = 'DRY-RUN (Symulacja)';
    } else {
        container.className = 'inline-flex items-center px-5 py-3 rounded-xl shadow-lg bg-red-100 text-red-800';
        badge.textContent = 'PRODUKCJA ⚠️';
    }
}

// Show finished screen
function showFinishedScreen() {
    document.getElementById('merge-card').style.display = 'none';
    document.getElementById('auto-merge-panel').style.display = 'none';
    document.getElementById('finished-screen').style.display = 'block';
    
    const summary = document.getElementById('final-summary');
    summary.innerHTML += `
        <div class="grid grid-cols-2 gap-6">
            <div class="bg-white rounded-xl p-6 shadow">
                <div class="text-4xl font-black text-indigo-600 mb-2">${currentState.totalPairs}</div>
                <div class="text-sm text-gray-600">Łącznie par</div>
            </div>
            <div class="bg-white rounded-xl p-6 shadow">
                <div class="text-4xl font-black text-green-600 mb-2">${currentState.doneCount}</div>
                <div class="text-sm text-gray-600">Scalonych</div>
            </div>
            <div class="bg-white rounded-xl p-6 shadow">
                <div class="text-4xl font-black text-orange-600 mb-2">${currentState.simpleCount}</div>
                <div class="text-sm text-gray-600">Prostych</div>
            </div>
            <div class="bg-white rounded-xl p-6 shadow">
                <div class="text-4xl font-black text-purple-600 mb-2">${currentState.currentIndex - currentState.doneCount}</div>
                <div class="text-sm text-gray-600">Pominiętych</div>
            </div>
        </div>
    `;
}

// Progress polling during init
function startProgressPolling() {
    document.getElementById('loading-stats').style.display = 'block';
    
    return setInterval(async () => {
        try {
            const response = await fetch('/api/get_progress');
            const result = await response.json();
            
            if (result.success) {
                document.getElementById('loading-fetched').textContent = result.fetched_accounts;
                document.getElementById('loading-duplicates').textContent = result.found_duplicates;
            }
        } catch (error) {
            console.error('Progress polling error:', error);
        }
    }, 1000);  // Co 1 sekundę
}

// Open logs
async function openLogs() {
    try {
        const response = await fetch('/api/get_logs');
        const result = await response.json();
        
        if (result.success) {
            alert(`Logi znajdują się w:\n\n${result.log_path}\n\nFolder: ${result.run_dir}`);
            
            // Opcjonalnie: spróbuj otworzyć folder (tylko Windows)
            if (confirm('Otworzyć folder z logami w Eksploratorze?')) {
                // W prawdziwej implementacji można użyć electron lub native bridge
                alert('Skopiuj ścieżkę i otwórz ręcznie:\n' + result.run_dir);
            }
        } else {
            alert(result.message || 'Logi nie są jeszcze dostępne');
        }
    } catch (error) {
        alert('Błąd: ' + error.message);
    }
}

// Loading overlay
function showGlobalLoading(title, message) {
    document.getElementById('loading-message').textContent = title;
    document.getElementById('loading-sub').textContent = message;
    document.getElementById('loading-overlay').style.display = 'flex';
}

function hideGlobalLoading() {
    document.getElementById('loading-overlay').style.display = 'none';
    document.getElementById('loading-stats').style.display = 'none';
}

// Keyboard shortcuts
document.addEventListener('keydown', function(e) {
    if (e.key === 'Enter' && e.ctrlKey) {
        mergePair();
    } else if (e.key === 'Escape') {
        skipPair();
    } else if (e.key === 'q' && e.ctrlKey) {
        quitProcess();
    }
});
