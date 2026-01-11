
import { 
    listUserTasks, 
    updateUserTask, 
    categorizeTasks 
} from './tasks.js';
import { 
    showToast, 
    fireConfetti, 
    escapeHtml, 
    formatDateISO, 
    friendlyDayLabel 
} from './ui.js';

let currentDay = new Date(); // Default to today
currentDay.setHours(0,0,0,0);

// Handlers injected from main.js
let openCreateModalHandler = null;
let openScheduleModalHandler = null;
let openGeneralEditModalHandler = null;

let currentViewMode = 'day';

export function setViewMode(mode) {
    currentViewMode = mode;
    console.log('View mode set to:', mode);
}

export function initCalendarInteractions() {
    // Placeholder for global calendar interactions
    console.log('Calendar interactions initialized');
}

export function setModalHandlers(create, schedule, edit) {
    openCreateModalHandler = create;
    openScheduleModalHandler = schedule;
    openGeneralEditModalHandler = edit;
}

// Expose current day for other modules
export function getCurrentDay() { return currentDay; }
export function setCurrentDay(d) { 
    currentDay = new Date(d); 
    currentDay.setHours(0,0,0,0); 
    // update global reference if needed
    try { window.currentDay = currentDay; } catch(e){}
}
export function adjustCurrentDay(deltaDays) {
    currentDay.setDate(currentDay.getDate() + deltaDays);
    try { window.currentDay = currentDay; } catch(e){}
    return currentDay;
}

// --- Undo System ---
const undoStack = [];
const MAX_UNDO_STACK = 20;

export function pushUndoAction(action) {
    undoStack.push(action);
    if (undoStack.length > MAX_UNDO_STACK) undoStack.shift();
}

export async function performUndo() {
    const action = undoStack.pop();
    if (!action) {
        showToast('Nothing to undo', 'info');
        return;
    }
    showToast('Undoing...', 'info');
    try {
        if (action.type === 'update') {
            await updateUserTask(action.id, action.oldPayload);
        } else if (action.type === 'delete') {
            // Complex to undo delete without full payload. 
            // For now, if we support delete undo, we assume payload was saved.
        }
        await loadAndRender();
        showToast('Undone', 'success');
    } catch (e) {
        console.error('Undo failed', e);
        showToast('Undo failed', 'error');
    }
}



export async function loadAndRender() {
    const calendarEl = document.getElementById('calendar');
    if (!calendarEl) return;
    
    // expose current day for external UI
    try { window.currentDay = currentDay; } catch (e) {}
    const display = document.getElementById('currentDateDisplay');
    if (display) display.textContent = friendlyDayLabel(currentDay);
    
    // Only show full loading state if we don't have the calendar structure yet
    if (!calendarEl.querySelector('.calendar-inner')) {
        calendarEl.innerHTML = '<p>Loading tasks...</p>';
    } else {
        calendarEl.classList.add('refreshing');
    }
    
    // Categorize any tasks with null categories before loading
    try {
        await categorizeTasks();
    } catch (err) {
        console.error('Error categorizing tasks:', err);
    }
    
    let docs = [];
    try { docs = await listUserTasks(); } catch (err) { console.error('Could not list tasks', err); }
    let tasks = (docs || []).map(d => {
        const preferred = d.assigned || d.due; // prefer assigned; fallback to due
        const dateStr = preferred ? String(preferred).slice(0,10) : '';
        return {
            id: d.$id || d.$uid || d.id || '',
            title: d.name || '',
            date: dateStr,
            assigned: d.assigned || null,
            category: d.category || '',
            color: d.color || 'cadet',
            estimateMinutes: typeof d.estimated_time === 'number' ? d.estimated_time : (typeof d.estimateMinutes === 'number' ? d.estimateMinutes : null),
            completed: d.complete !== undefined ? !!d.complete : !!d.completed,
            repeat: d.repeat || null,
            priority: d.priority || 'medium'
        };
    });

    // augment with weekly repeats for the current day if needed
    const currentKey = formatDateISO(currentDay);
    const existingKeys = new Set(tasks
        .filter(t => t.date === currentKey)
        .map(t => {
            const assigned = t.assigned ? new Date(t.assigned) : null;
            const mins = assigned ? assigned.getHours()*60 + assigned.getMinutes() : -1;
            return `${t.title}|${t.category}|${mins}`;
        })
    );
    const extra = [];
    (tasks || []).forEach(t => {
        if (t.repeat === true && t.assigned) {
            const baseDate = new Date(t.assigned);
            const wd = baseDate.getDay();
            if (wd === currentDay.getDay()) {
                const h = baseDate.getHours(); const m = baseDate.getMinutes();
                const key = `${t.title}|${t.category}|${h*60+m}`;
                if (!existingKeys.has(key)) {
                    const assignedDate = new Date(currentDay);
                    assignedDate.setHours(h, m, 0, 0);
                    extra.push({ ...t, date: currentKey, assigned: assignedDate.toISOString(), id: `virt-${t.id}-${currentKey}` });
                }
            }
        }
    });
    tasks = tasks.concat(extra);
    renderCalendar(tasks);
}

function renderCalendar(tasks) {
    const calendarEl = document.getElementById('calendar');
    if (!calendarEl) return;
    const byDate = {};
    tasks.forEach(t => { if (!t.date) return; const key = t.date.slice(0,10); (byDate[key] || (byDate[key]=[])).push(t); });
    const key = formatDateISO(currentDay);
    
    // Check if we already have the calendar structure
    let wrapper = calendarEl.querySelector('.calendar-inner');
    let dayGrid = wrapper ? wrapper.querySelector('.day-grid') : null;
    let timeCol = dayGrid ? dayGrid.querySelector('.time-column') : null;
    let eventsCol = dayGrid ? dayGrid.querySelector('.events-column') : null;
    
    // Only create the structure and time column if it doesn't exist
    if (!wrapper || !dayGrid || !timeCol || !eventsCol) {
        calendarEl.innerHTML = '';
        wrapper = document.createElement('div'); wrapper.className = 'calendar-inner';
        
        dayGrid = document.createElement('div'); dayGrid.className = 'day-grid';
        timeCol = document.createElement('div'); timeCol.className = 'time-column';
        // render from 06:00 (6 AM) to 24:00 (midnight) in 15-minute increments
        const START_MINUTES = 6 * 60; // 06:00
        const END_MINUTES = 24 * 60; // 24:00 midnight
        const totalMinutes = END_MINUTES - START_MINUTES; // 18 hours = 1080 minutes
        const slots = totalMinutes / 15; // 72 slots
        for (let i = 0; i < slots; i++) {
            const slot = document.createElement('div'); slot.className = 'time-slot';
            const minutesAtSlot = START_MINUTES + i * 15;
            if (minutesAtSlot % 60 === 0) {
                slot.classList.add('hour');
                let hour = Math.floor(minutesAtSlot / 60);
                const hh = String(hour).padStart(2,'0');
                slot.textContent = `${hh}:00`;
            }
            timeCol.appendChild(slot);
        }
        eventsCol = document.createElement('div'); eventsCol.className = 'events-column';
        dayGrid.appendChild(timeCol);
        dayGrid.appendChild(eventsCol);
        wrapper.appendChild(dayGrid);
        calendarEl.appendChild(wrapper);
    }

    const firstSlot = timeCol.querySelector('.time-slot');
    const slotHeight = (firstSlot && firstSlot.clientHeight > 0) ? firstSlot.clientHeight : 48; // fallback to CSS 48px
    const dayHeight = slotHeight * 72; // 72 quarters
    // events layer
    const layer = eventsCol.querySelector('.events-layer') || document.createElement('div');
    if (!layer.parentNode) {
        layer.className = 'events-layer';
        layer.style.position = 'relative';
        layer.style.height = dayHeight + 'px';
        layer.style.marginTop = '12px';
        
        if (eventsCol.innerHTML !== '') eventsCol.innerHTML = '';
        eventsCol.appendChild(layer);
        
        // Enable Drop
        let scrollSpeed = 0;
        let scrollRaf = null;
        let lastDragTime = 0;
        
        function dndScrollLoop() {
            if (scrollSpeed !== 0) {
                if (Date.now() - lastDragTime > 100) {
                    scrollSpeed = 0; scrollRaf = null; return;
                }
                window.scrollBy(0, scrollSpeed);
                scrollRaf = requestAnimationFrame(dndScrollLoop);
            } else { scrollRaf = null; }
        }

        layer.addEventListener('dragover', (e) => {
            e.preventDefault(); e.dataTransfer.dropEffect = 'move';
            lastDragTime = Date.now();
            let newSpeed = 0;
            if (e.clientY < 60) newSpeed = -15;
            else if (window.innerHeight - e.clientY < 60) newSpeed = 15;
            if (newSpeed !== 0) { scrollSpeed = newSpeed; if (!scrollRaf) dndScrollLoop(); } 
            else { scrollSpeed = 0; }
        });
        layer.addEventListener('drop', async (e) => {
            scrollSpeed = 0; if (scrollRaf) cancelAnimationFrame(scrollRaf);
            e.preventDefault();
            const id = e.dataTransfer.getData('text/plain');
            if (!id) return;
            const offset = parseFloat(e.dataTransfer.getData('offset')) || 0;
            const layerRect = layer.getBoundingClientRect();
            const relativeY = e.clientY - layerRect.top - offset;
            const minutesFromStart = (relativeY / dayHeight) * 1080;
            const dayStartMinutes = 6 * 60; 
            let finalMinutes = dayStartMinutes + minutesFromStart;
            finalMinutes = Math.round(finalMinutes / 15) * 15;
            finalMinutes = Math.max(dayStartMinutes, Math.min(24*60 - 15, finalMinutes));

            const h = Math.floor(finalMinutes / 60);
            const m = finalMinutes % 60;
            const newDate = new Date(currentDay);
            newDate.setHours(h, m, 0, 0);

            // Optimistic move
            const card = layer.querySelector(`.event-card[data-id="${id}"]`);
            if (card) {
                const pxPerMinute = dayHeight / 1080;
                const newTop = (finalMinutes - 6*60) * pxPerMinute;
                card.style.top = newTop + 'px';
                card.classList.remove('dragging');
                card.classList.add('dropping');
            }
            
            try {
                await updateUserTask(id, { assigned: newDate.toISOString() });
                await loadAndRender();
                showToast('Task moved', 'success', { label: 'Undo', onClick: performUndo });
            } catch (err) {
                console.error(err); showToast('Move failed', 'error');
            }
        });
    } else {
        layer.innerHTML = '';
    }
    
    function updateCurrentTimeLine() {
        const existing = layer.querySelectorAll('.current-time-line');
        existing.forEach(el => el.remove());
        const now = new Date();
        const todayStr = formatDateISO(now);
        if (key === todayStr) {
            const startMins = 6 * 60;
            const curMins = now.getHours() * 60 + now.getMinutes();
            if (curMins >= startMins && curMins <= 24*60) {
                const pxPerMin = dayHeight / (18 * 60);
                const offsets = Math.round((curMins - startMins) * pxPerMin);
                const line = document.createElement('div');
                line.className = 'current-time-line';
                line.style.top = offsets + 'px';
                layer.appendChild(line);
            }
        }
    }
    updateCurrentTimeLine();
    if (window.currentTimeInterval) clearInterval(window.currentTimeInterval);
    window.currentTimeInterval = setInterval(updateCurrentTimeLine, 60000);

    // Double Click to Create
    if (!layer.dataset.dndInit) {
        layer.dataset.dndInit = 'true';
        layer.addEventListener('dblclick', (e) => {
            if (e.target.closest('.event-card')) return;
            e.preventDefault();
            const rect = layer.getBoundingClientRect();
            const relY = e.clientY - rect.top;
            const pxPerMin = dayHeight / 1080; 
            const minsFromStart = relY / pxPerMin;
            const totalMins = (6 * 60) + minsFromStart;
            const snapped = Math.round(totalMins / 15) * 15;
            const h = Math.floor(snapped / 60);
            const m = snapped % 60;
            const timeStr = `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}`;
            
            if (openCreateModalHandler) openCreateModalHandler(key, timeStr);
        });
    }
    
    if (calendarEl.classList.contains('refreshing')) calendarEl.classList.remove('refreshing');

    let items = byDate[key] || [];

    // Conflict Detection
    items.sort((a,b) => {
        const timeA = a.assigned ? new Date(a.assigned).getTime() : 0;
        const timeB = b.assigned ? new Date(b.assigned).getTime() : 0;
        return timeA - timeB;
    });
    for(let i=0; i<items.length; i++) {
        const taskA = items[i];
        if(!taskA.assigned) continue;
        const dateA = new Date(taskA.assigned);
        const startA = dateA.getHours()*60 + dateA.getMinutes();
        const durA = (typeof taskA.estimateMinutes==='number' ? taskA.estimateMinutes : 60);
        const endA = startA + durA;

        for(let j=i+1; j<items.length; j++) {
            const taskB = items[j];
            if(!taskB.assigned) continue;
            const dateB = new Date(taskB.assigned);
            const startB = dateB.getHours()*60 + dateB.getMinutes();
            if(startB >= endA) break; 
            taskA.isConflict = true;
            taskB.isConflict = true;
        }
    }
    
    if (!items.length) { eventsCol.classList.add('has-hint'); } else { eventsCol.classList.remove('has-hint'); }

    function resolveColor(value) {
        if (!value) return '#5f6c80';
        const v = String(value).trim();
        if (v.startsWith('#') || /^rgb|^hsl/i.test(v)) return v;
        const key = v.toLowerCase();
        const map = {
            cadet: '#93A8AC', coral: '#EF6F6C', mint: '#59C9A5', celadon: '#ACDD91', violet: '#715D73', rose: '#9B6371',
            'dark-slate': '#465775', sandy: '#F7B074', maize: '#FFF07C', info: '#50908D',
            blue: '#3b82f6', green: '#22c55e', amber: '#f59e0b', purple: '#a855f7', red: '#ef4444', orange: '#f97316',
            yellow: '#eab308', teal: '#14b8a6', cyan: '#06b6d4', indigo: '#6366f1', pink: '#ec4899', gray: '#6b7280'
        };
        return map[key] || v; 
    }

    function textColorFor(bg) {
        const hex = String(bg).trim();
        const m = /^#([0-9a-f]{3}|[0-9a-f]{6})$/i.exec(hex);
        if (!m) return '#ffffff';
        let r, g, b;
        let h = m[1];
        if (h.length === 3) {
            r = parseInt(h[0] + h[0], 16);
            g = parseInt(h[1] + h[1], 16);
            b = parseInt(h[2] + h[2], 16);
        } else {
            r = parseInt(h.slice(0, 2), 16);
            g = parseInt(h.slice(2, 4), 16);
            b = parseInt(h.slice(4, 6), 16);
        }
        const luminance = (0.299 * r + 0.587 * g + 0.114 * b);
        return luminance > 186 ? '#111111' : '#ffffff';
    }

    items.forEach(it => {
        const assigned = it.assigned ? new Date(it.assigned) : null;
        const startMinutes = assigned ? (assigned.getHours() * 60 + assigned.getMinutes()) : (9 * 60); 
        const duration = (typeof it.estimateMinutes === 'number' && it.estimateMinutes > 0) ? it.estimateMinutes : 60;
        const visibleTotal = 18 * 60; 
        const pxPerMinute = dayHeight / visibleTotal;
        const relStart = Math.max(0, Math.min(visibleTotal, startMinutes - (6 * 60)));
        const remaining = Math.max(0, visibleTotal - relStart);
        const clampedDuration = Math.min(duration, remaining);
        const top = Math.round(relStart * pxPerMinute);
        const height = Math.max(20, Math.round(clampedDuration * pxPerMinute));

        const card = document.createElement('div');
        card.className = 'event-card' + (it.isConflict ? ' conflict' : '');
        card.dataset.id = it.id;
        card.style.position = 'absolute';
        card.style.left = '0'; card.style.right = '0';
        card.style.top = top + 'px'; card.style.height = height + 'px';
        card.style.overflow = 'hidden'; card.style.padding = '4px 8px';
        card.dataset.priority = it.priority || 'medium';
        card.style.boxSizing = 'border-box';
        const isBlockedEvent = String(it.category||'').toLowerCase()==='blocked';
        let bg = resolveColor(isBlockedEvent ? 'black' : (it.color || 'cadet'));
        if (it.completed && !isBlockedEvent) { bg = resolveColor('gray'); card.classList.add('completed'); }
        card.style.background = bg;
        card.style.color = textColorFor(bg);
        card.style.borderRadius = '6px';
        card.style.boxShadow = '0 1px 2px rgba(0,0,0,0.1)';
        card.title = `${it.title}${it.category ? ' · ' + it.category : ''} · ${duration} min`;
        card.innerHTML = `<div class="event-header" style="display:flex;justify-content:flex-start;align-items:center;gap:6px"><div style="font-weight:600;white-space:nowrap;text-overflow:ellipsis;overflow:hidden;line-height:1.2;">${escapeHtml(it.title)}</div></div>${it.category ? `<div style=\"font-size:.75rem;opacity:.9;white-space:nowrap;text-overflow:ellipsis;overflow:hidden;line-height:1.15;\">${escapeHtml(it.category)}</div>` : ''}`;

        card.style.paddingRight = '68px';
        const btn = document.createElement('button');
        btn.className = 'complete-btn';
        btn.type = 'button';
        btn.title = it.completed ? 'Completed' : 'Mark complete';
        btn.innerHTML = '<svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true"><path fill="currentColor" d="M9 16.2 4.8 12l-1.4 1.4L9 19 21 7l-1.4-1.4z"/></svg>';
        if (it.completed) {
            btn.disabled = true;
            btn.classList.add('completed');
        }
        card.appendChild(btn);

        const dur = document.createElement('div');
        dur.className = 'event-duration';
        dur.textContent = `${duration}m`;
        dur.style.position = 'absolute'; dur.style.top = '50%'; dur.style.right = '42px'; dur.style.transform = 'translateY(-50%)';
        dur.style.fontSize = '.75rem'; dur.style.opacity = '0.9';
        card.appendChild(dur);

        if (isBlockedEvent) {
            btn.style.display = 'none'; dur.style.display = 'none';
            card.style.cursor = 'pointer';
            card.addEventListener('click', (e) => {
                e.preventDefault(); e.stopPropagation();
                if (openScheduleModalHandler) openScheduleModalHandler();
            });
        } else {
            card.style.cursor = 'pointer';
            card.addEventListener('click', (e) => {
                 if (e.target.closest('.complete-btn') || e.target.closest('.resize-handle')) return;
                 if (openGeneralEditModalHandler) openGeneralEditModalHandler(it);
            });

            if (!it.completed) {
                card.draggable = true;
                card.addEventListener('dragstart', (e) => {
                    e.dataTransfer.setData('text/plain', it.id);
                    const rect = card.getBoundingClientRect();
                    e.dataTransfer.setData('offset', e.clientY - rect.top);
                    e.dataTransfer.effectAllowed = 'move';
                    pushUndoAction({ type: 'update', id: it.id, oldPayload: { assigned: it.assigned, estimated_time: duration } });
                    
                    const dragImage = card.cloneNode(true);
                    dragImage.style.top = '-9999px'; dragImage.style.left = '-9999px';
                    document.body.appendChild(dragImage);
                    e.dataTransfer.setDragImage(dragImage, e.clientX - rect.left, e.clientY - rect.top);
                    setTimeout(() => document.body.removeChild(dragImage), 0);
                    setTimeout(() => card.classList.add('dragging'), 0);
                });
                card.addEventListener('dragend', () => card.classList.remove('dragging'));
            }

            if (!it.completed) {
                const handle = document.createElement('div');
                handle.className = 'resize-handle';
                card.appendChild(handle);
                handle.addEventListener('mousedown', (e) => {
                    e.stopPropagation(); e.preventDefault();
                    pushUndoAction({ type: 'update', id: it.id, oldPayload: { assigned: it.assigned, estimated_time: duration } });
                    const startPageY = e.pageY;
                    const startH = height;
                    let scrollSpeed = 0; let rafId = null;

                    function scrollTick() {
                        if (scrollSpeed !== 0) { window.scrollBy(0, scrollSpeed); rafId = requestAnimationFrame(scrollTick); } 
                        else { rafId = null; }
                    }

                    function onMove(evt) {
                        if (evt.clientY < 60) { scrollSpeed = -15; if (!rafId) scrollTick(); } 
                        else if (window.innerHeight - evt.clientY < 60) { scrollSpeed = 15; if (!rafId) scrollTick(); } 
                        else { scrollSpeed = 0; }
                        const diff = evt.pageY - startPageY;
                        const newH = Math.max(20, startH + diff);
                        card.style.height = newH + 'px';
                        const currentMins = (newH / dayHeight) * visibleTotal;
                        const snappedMins = Math.max(1, Math.round(currentMins));
                        if (dur) dur.textContent = `${snappedMins}m`;
                    }
                    async function onUp(evt) {
                        if (rafId) cancelAnimationFrame(rafId);
                        scrollSpeed = 0;
                        document.removeEventListener('mousemove', onMove);
                        document.removeEventListener('mouseup', onUp);
                        const finalH = parseFloat(card.style.height);
                        const exactMins = (finalH / dayHeight) * visibleTotal;
                        const snapped = Math.max(1, Math.round(exactMins));
                        if (snapped !== duration) {
                            try {
                                await updateUserTask(it.id, { estimated_time: snapped });
                                await loadAndRender();
                                showToast('Duration updated', 'success');
                            } catch (err) { showToast('Resize failed', 'error'); await loadAndRender(); }
                        } else { await loadAndRender(); }
                    }
                    document.addEventListener('mousemove', onMove);
                    document.addEventListener('mouseup', onUp);
                });
            }
        }

        btn.addEventListener('click', async (e) => {
            e && e.stopPropagation && e.stopPropagation();
            try {
                card.classList.add('pop'); btn.classList.add('completed'); btn.disabled = true;
                const gray = resolveColor('gray');
                card.style.background = gray; card.style.color = textColorFor(gray);
                fireConfetti();
                
                // Allow Undo
                pushUndoAction({ type: 'update', id: it.id, oldPayload: { complete: false, color: it.color } });
                
                await updateUserTask(it.id, { complete: true, color: 'gray' });
                await loadAndRender();
                
                showToast('Task completed!', 'success', { label: 'Undo', onClick: performUndo });
            } catch (e) {
                console.error('Failed to complete task', e); btn.disabled = false;
            }
        });
        layer.appendChild(card);
    });
}
