
import { 
    createUserTask, 
    updateUserTask, 
    deleteUserTask, 
    listUserTasks, 
    autoSchedule,
    categorizeTasks
} from './tasks.js';
import { 
    showToast, 
    formatDateISO, 
    startOfDay 
} from './ui.js';
import { getCurrentDay, loadAndRender } from './calendar.js';
import { parseSmartInput } from './parser.js';

function parseHHMM(str) {
    if (!str || typeof str !== 'string' || !/^[0-9]{2}:[0-9]{2}$/.test(str)) return null;
    const [hh, mm] = str.split(':').map(n => parseInt(n,10));
    if (hh<0||hh>23||mm<0||mm>59) return null;
    return { hh, mm, minutes: hh*60+mm };
}

function weekStartOf(date) {
    const d = new Date(date);
    d.setHours(0,0,0,0);
    const day = d.getDay(); // 0 Sun .. 6 Sat
    d.setDate(d.getDate() - day);
    return d;
}

function isModalOpen(modalEl) {
    return !!(modalEl && modalEl.classList && modalEl.classList.contains('is-open'));
}

function openModal(modalEl) {
    if (!modalEl) return;
    modalEl.classList.add('is-open');
    modalEl.setAttribute('aria-hidden', 'false');
}

function closeModal(modalEl) {
    if (!modalEl) return;
    modalEl.classList.remove('is-open');
    modalEl.setAttribute('aria-hidden', 'true');
}

function bindModalDismissal(modalEl, onClose) {
    if (!modalEl || typeof onClose !== 'function') return;
    document.addEventListener('click', (e) => {
        if (!isModalOpen(modalEl)) return;
        if (e.target === modalEl) onClose();
    });
    document.addEventListener('keydown', (e) => {
        if (e.key !== 'Escape') return;
        if (!isModalOpen(modalEl)) return;
        onClose();
    });
}

// --- Schedule Modal Logic ---
let scheduleExistingDocs = [];

export function initScheduleModal() {
    const schedulePopup = document.getElementById('createTaskBtn-popup');
    const defineScheduleModal = document.getElementById('defineScheduleModal');
    const cancelScheduleBtn = document.getElementById('cancelSchedule');
    const closeScheduleBtn = document.getElementById('closeScheduleModal');
    const scheduleForm = document.getElementById('scheduleForm');
    const repeatWeeklyEl = document.getElementById('repeatWeekly');

    function addIntervalRow(dayIdx, defaults) {
        const container = document.querySelector(`.day-intervals[data-day="${dayIdx}"] .intervals`);
        if (!container) return;
        const row = document.createElement('div');
        row.className = 'interval';
        row.innerHTML = `
                <input type=\"time\" class=\"start\" aria-label=\"Start time\" value=\"${defaults && defaults.start ? defaults.start : ''}\">
                <span style=\"opacity:.7;\">to</span>
                <input type=\"time\" class=\"end\" aria-label=\"End time\" value=\"${defaults && defaults.end ? defaults.end : ''}\">
                <input type=\"text\" class=\"label\" aria-label=\"What is this time for?\" placeholder=\"What is this time for?\" value=\"${defaults && defaults.label ? defaults.label.replace(/\\/g,'\\\\').replace(/"/g,'\\\"') : ''}\">
            <button type="button" class="remove-interval" title="Remove">×</button>
        `;
        row.querySelector('.remove-interval').addEventListener('click', () => {
            row.remove();
        });
        container.appendChild(row);
        return row;
    }

    document.querySelectorAll('.add-interval').forEach(btn => {
        btn.addEventListener('click', () => {
            const dayIdx = btn.getAttribute('data-day');
            addIntervalRow(dayIdx);
        });
    });

    function clearScheduleUI() {
        document.querySelectorAll('.day-intervals .intervals').forEach(el => { el.innerHTML = ''; });
        if (repeatWeeklyEl) repeatWeeklyEl.checked = true;
    }

    async function populateScheduleFromExisting() {
        const docs = await listUserTasks();
        const base = weekStartOf(getCurrentDay());
        const weekStart = new Date(base);
        const weekEnd = new Date(base); weekEnd.setDate(weekEnd.getDate() + 7);
        const isBlocked = d => (String(d.category||'').toLowerCase()==='blocked');
        const inWeek = iso => { const dt = new Date(iso); return dt >= weekStart && dt < weekEnd; };
        
        let picked = docs.filter(d => isBlocked(d) && d.repeat === true);
        if (!picked.length) picked = docs.filter(d => isBlocked(d) && d.assigned && inWeek(d.assigned));

        scheduleExistingDocs = picked.slice();

        clearScheduleUI();
        picked.forEach(d => {
            if (!d.assigned) return;
            const dt = new Date(d.assigned);
            const day = dt.getDay();
            const hh = String(dt.getHours()).padStart(2,'0');
            const mm = String(dt.getMinutes()).padStart(2,'0');
            const label = d.name || 'Blocked time';
            const mins = typeof d.estimated_time==='number' ? d.estimated_time : (typeof d.estimateMinutes==='number'? d.estimateMinutes : 60);
            const endDate = new Date(dt.getTime() + mins*60000);
            const eh = String(endDate.getHours()).padStart(2,'0');
            const em = String(endDate.getMinutes()).padStart(2,'0');
            const row = addIntervalRow(day, { start: `${hh}:${mm}`, end: `${eh}:${em}`, label });
            if (row && d.$id) row.dataset.docId = d.$id;
        });

        if (repeatWeeklyEl) repeatWeeklyEl.checked = picked.some(d => d.repeat === true);
    }

    function openScheduleModal() {
        clearScheduleUI();
        populateScheduleFromExisting().finally(() => {
            openModal(defineScheduleModal);
        });
    }

    if (schedulePopup && defineScheduleModal) {
        schedulePopup.addEventListener('click', (e) => {
            e.stopPropagation();
            openScheduleModal();
        });
    }
    if (cancelScheduleBtn && defineScheduleModal) {
        cancelScheduleBtn.addEventListener('click', () => closeModal(defineScheduleModal));
    }

    if (closeScheduleBtn && defineScheduleModal) {
        closeScheduleBtn.addEventListener('click', () => closeModal(defineScheduleModal));
    }

    if (defineScheduleModal) {
        bindModalDismissal(defineScheduleModal, () => closeModal(defineScheduleModal));
    }

    if (scheduleForm) {
        scheduleForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const errors = [];
            const intervalsByDay = {};
            const base = weekStartOf(getCurrentDay());
            document.querySelectorAll('.day-intervals').forEach(dayEl => {
                const dayIdx = parseInt(dayEl.getAttribute('data-day'),10);
                const rows = Array.from(dayEl.querySelectorAll('.interval'));
                const parts = [];
                rows.forEach(r => {
                    const startVal = r.querySelector('.start')?.value || '';
                    const endVal = r.querySelector('.end')?.value || '';
                    const labelVal = r.querySelector('.label')?.value || '';
                    const docId = r.dataset && r.dataset.docId ? r.dataset.docId : null;
                    const s = parseHHMM(startVal);
                    const e2 = parseHHMM(endVal);
                    if (!s || !e2) return; 
                    if (e2.minutes <= s.minutes) { errors.push(`End must be after start for day ${dayIdx}`); return; }
                    parts.push({ start: s, end: e2, label: labelVal, docId });
                });
                if (parts.length) intervalsByDay[dayIdx] = parts;
            });
            if (errors.length) { showToast(errors[0], 'error'); return; }
            
            const toCreate = [];
            const toUpdate = [];
            const seenDocIds = new Set();
            const repeatVal = !!(repeatWeeklyEl && repeatWeeklyEl.checked);

            Object.keys(intervalsByDay).forEach(k => {
                const dayIdx = parseInt(k,10);
                const date = new Date(base);
                date.setDate(base.getDate() + dayIdx);
                intervalsByDay[dayIdx].forEach(({start, end, label, docId}) => {
                    const startDate = new Date(date);
                    startDate.setHours(start.hh, start.mm, 0, 0);
                    const endDate = new Date(date);
                    endDate.setHours(end.hh, end.mm, 0, 0);
                    const durationMin = Math.max(1, Math.round((endDate - startDate) / 60000));
                    const payload = {
                        name: (label && label.trim()) ? label.trim() : 'Blocked time',
                        due: new Date(date.getFullYear(), date.getMonth(), date.getDate(), 23, 59, 59).toISOString(),
                        assigned: startDate.toISOString(),
                        category: 'Blocked',
                        color: 'black',
                        estimated_time: durationMin,
                        complete: false,
                        repeat: repeatVal,
                        priority: 'medium'
                    };
                    if (docId) {
                        toUpdate.push({ id: docId, payload });
                        seenDocIds.add(docId);
                    } else {
                        toCreate.push(payload);
                    }
                });
            });
            
            if (!toCreate.length && !toUpdate.length) { showToast('Add at least one time range', 'info'); return; }
            try {
                await Promise.all(toUpdate.map(({ id, payload }) => updateUserTask(id, payload)));
                await Promise.all(toCreate.map(payload => createUserTask(payload)));
                const existing = Array.isArray(scheduleExistingDocs) ? scheduleExistingDocs : [];
                await Promise.all(
                    existing
                        .filter(d => d && d.$id && !seenDocIds.has(d.$id))
                        .map(d => deleteUserTask(d.$id))
                );
                closeModal(defineScheduleModal);
                await loadAndRender();
                showToast('Schedule updated', 'success');
            } catch (err) {
                console.error('Failed to create blocked events', err);
                showToast(err.message || 'Failed to create blocked events', 'error');
            }
        });
    }

    return { openScheduleModal };
}

// --- Edit Modal (General) ---
export function initEditModal() {
    const editEventModal = document.getElementById('editEventModal');
    const editEventForm = document.getElementById('editEventForm');
    const cancelEditEventBtn = document.getElementById('cancelEditEvent');
    const closeEditEventBtn = document.getElementById('closeEditEvent');
    const editDocId = document.getElementById('editDocId');
    const editTitle = document.getElementById('editTitle');
    const editDate = document.getElementById('editDate');
    const editStart = document.getElementById('editStart');
    const editDuration = document.getElementById('editDuration');
    const editPriority = document.getElementById('editPriority');
    const editCategory = document.getElementById('editCategory');
    const editColor = document.getElementById('editColor');
    const editRepeatWeekly = document.getElementById('editRepeatWeekly');
    const editCompleted = document.getElementById('editCompleted');

    function pad2(n) { return String(n).padStart(2, '0'); }
    function closeEditModal() { closeModal(editEventModal); }
    if (cancelEditEventBtn) cancelEditEventBtn.addEventListener('click', closeEditModal);
    if (closeEditEventBtn) closeEditEventBtn.addEventListener('click', closeEditModal);

    if (editEventModal) bindModalDismissal(editEventModal, closeEditModal);

    function openGeneralEditModal(task) {
        if (!editEventModal) return;
        const id = String(task.id || '');
        let realId = id;
        if (id.startsWith('virt-')) {
            const idx = id.lastIndexOf('-');
            realId = id.slice(5, idx > 4 ? idx : undefined);
        }
        if (editDocId) editDocId.value = realId;
        if (editTitle) editTitle.value = task.title || '';
        const when = task.assigned ? new Date(task.assigned) : (task.date ? new Date(task.date + 'T00:00:00') : new Date());
        if (editDate) editDate.value = (task.date ? task.date : when.toISOString().slice(0,10));
        if (editStart) editStart.value = pad2(when.getHours()) + ':' + pad2(when.getMinutes());
        if (editDuration) editDuration.value = task.estimateMinutes || 60;
        if (editPriority) editPriority.value = task.priority || 'medium';
        if (editCategory) editCategory.value = task.category || '';
        if (editColor) editColor.value = (String(task.color||'').startsWith('#') ? task.color : '#3b82f6');
        if (editRepeatWeekly) editRepeatWeekly.checked = !!task.repeat;
        if (editCompleted) editCompleted.checked = !!task.completed;
        openModal(editEventModal);
    }

    if (editEventForm) {
        editEventForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            if (!editDocId || !editDocId.value) { showToast('Missing event id.', 'error'); return; }
            const title = (editTitle && editTitle.value.trim()) || '';
            const dateStr = (editDate && editDate.value) || '';
            const timeStr = (editStart && editStart.value) || '';
            const duration = Math.max(1, parseInt((editDuration && editDuration.value) || '60', 10));
            if (!title || !dateStr || !/^[0-9]{2}:[0-9]{2}$/.test(timeStr)) { showToast('Please complete required fields.', 'error'); return; }
            const [hh, mm] = timeStr.split(':').map(n => parseInt(n,10));
            const assigned = new Date(dateStr + 'T00:00:00'); assigned.setHours(hh, mm, 0, 0);
            const payload = {
                name: title,
                assigned: assigned.toISOString(),
                due: new Date(dateStr + 'T23:59:59').toISOString(),
                estimated_time: duration,
                priority: (editPriority && editPriority.value) || 'medium',
                category: (editCategory && editCategory.value.trim()) || null,
                color: (editColor && editColor.value) || '#3b82f6',
                repeat: !!(editRepeatWeekly && editRepeatWeekly.checked),
                complete: !!(editCompleted && editCompleted.checked)
            };
            try {
                await updateUserTask(editDocId.value, payload);
                closeEditModal();
                await loadAndRender();
                showToast('Event updated', 'success');
            } catch (err) {
                console.error('Update event failed', err);
                showToast(err.message || 'Failed to update event', 'error');
            }
        });
    }

    return { openGeneralEditModal };
}
// --- Create Task Modal ---
export function initTaskModal() {
    const createBtn = document.getElementById('createTaskBtn'); 
    const modal = document.getElementById('taskModal'); 
    const cancelBtn = document.getElementById('cancelTask'); 
    const closeBtn = document.getElementById('closeTaskModal');
    const taskForm = document.getElementById('taskCreateForm');
    
    function openCreateModal(dateStr, timeStr) {
        if (!modal) return;
        const dateEl = document.getElementById('taskDueDate');
        const timeEl = document.getElementById('taskAssignedTime');
        const title = document.getElementById('taskTitle');
        const smartInput = document.getElementById('smartInput');
        document.getElementById('taskCreateForm').reset();
        if (dateEl) dateEl.value = dateStr || formatDateISO(getCurrentDay());
        if (timeEl) timeEl.value = timeStr || '';
        openModal(modal);
        if (smartInput) smartInput.focus(); else if (title) title.focus();
    }

    if (createBtn && modal) {
        createBtn.addEventListener('click', () => { 
            openCreateModal(formatDateISO(getCurrentDay()));
        });
    }
    if (cancelBtn && modal) cancelBtn.addEventListener('click', () => closeModal(modal));
    if (closeBtn && modal) closeBtn.addEventListener('click', () => closeModal(modal));
    if (modal) bindModalDismissal(modal, () => closeModal(modal));
    
    const smartInput = document.getElementById('smartInput');
    if (smartInput) {
        smartInput.addEventListener('change', () => {
            const val = smartInput.value;
            if (!val) return;
            const parsed = parseSmartInput(val);
            if (parsed.title) document.getElementById('taskTitle').value = parsed.title;
            if (parsed.date) document.getElementById('taskDueDate').value = parsed.date;
            if (parsed.time) document.getElementById('taskAssignedTime').value = parsed.time;
            if (parsed.duration) document.getElementById('taskEstimateMinutes').value = parsed.duration;
        });
        smartInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                smartInput.blur();
                document.getElementById('taskTitle').focus();
            }
        });
    }

    if (taskForm) {
        taskForm.addEventListener('submit', async (e) => {
            e.preventDefault(); 
            const name = document.getElementById('taskTitle').value.trim(); 
            const dueDate = document.getElementById('taskDueDate').value; 
            const estimateStr = (document.getElementById('taskEstimateMinutes')||{}).value || '';
            const estimateMinutes = estimateStr ? Math.max(1, parseInt(estimateStr, 10)) : 60;
            const priorityVal = (document.getElementById('taskPriority')||{}).value || 'medium';
            const assignedTime = (document.getElementById('taskAssignedTime')||{}).value;
            
            if (!name || !dueDate) return showToast('Please enter task name and due date', 'error');
            
            let assignedIso = null;
            let dueIso = new Date(dueDate + 'T23:59:59.000Z').toISOString();
            if (assignedTime) {
                assignedIso = new Date(`${dueDate}T${assignedTime}:00.000Z`).toISOString();
            }
            
            const taskData = {
                name: name,
                due: dueIso,
                category: null,
                color: null,
                assigned: assignedIso,
                estimated_time: estimateMinutes,
                complete: false,
                priority: priorityVal
            };
            
            try { 
                await createUserTask(taskData); 
                closeModal(modal);
                taskForm.reset(); 
                await loadAndRender(); 
                showToast('Task created', 'success');
                await categorizeTasks();
                const count = await autoSchedule(getCurrentDay());
                if (count > 0) {
                     showToast(`Auto-scheduled ${count} tasks`, 'success');
                     await loadAndRender();
                }
            } catch (err) { 
                console.error('create task error',err); 
                showToast(err.message || 'Could not create task', 'error'); 
            }
        });
    }

    return { openCreateModal, parseSmartInput };
}
