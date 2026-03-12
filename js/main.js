
import { initAuth, checkSession, logout } from './auth.js';
import { ensureSchema } from './appwrite.js';
import { loadAndRender, adjustCurrentDay, getCurrentDay, setCurrentDay, setViewMode, setModalHandlers } from './calendar.js';
import { initTaskModal, initScheduleModal, initEditModal } from './modals.js';
import { initTimer } from './timer.js';
import { showToast, formatDateISO, fireConfetti } from './ui.js';
import { categorizeTasks, autoSchedule, autoRescheduleOverdueTasks, listUserTasks, createUserTask, updateUserTask, deleteUserTask, calculateStreak, clearCompletedTasks } from './tasks.js';
import { parseSmartInput } from './parser.js';
import { updateStreak, syncAchievementsFromLocal, loadAchievementsFromServer, loadStatsFromServer } from './achievement-tracker.js';

// Global state for theme
function getInitialTheme() {
    const stored = localStorage.getItem('theme');
    if (stored === 'dark' || stored === 'light') return stored;
    const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
    return prefersDark ? 'dark' : 'light';
}

let currentTheme = getInitialTheme();

function initTheme() {
    const root = document.documentElement;
    const isDark = currentTheme === 'dark';

    root.classList.toggle('dark', isDark);
    if (document.body) document.body.classList.toggle('dark', isDark);

    root.style.colorScheme = currentTheme;

    const toggleIds = ['toggleThemeBtn', 'dmToggle', 'darkModeToggle'];
    
    // Sync UI state
    toggleIds.forEach(id => {
        const el = document.getElementById(id);
        if (el && el.type === 'checkbox') el.checked = (currentTheme === 'dark');
    });

    // Attach listeners
    toggleIds.forEach(id => {
        const el = document.getElementById(id);
        if (!el) return;
        
           const handler = () => {
               root.classList.toggle('dark');
               if (document.body) document.body.classList.toggle('dark', root.classList.contains('dark'));
               currentTheme = root.classList.contains('dark') ? 'dark' : 'light';
               localStorage.setItem('theme', currentTheme);
               root.style.colorScheme = currentTheme;
             
             // Sync all checkboxes
             toggleIds.forEach(oid => {
                 const oel = document.getElementById(oid);
                 if (oel && oel.type === 'checkbox') oel.checked = (currentTheme === 'dark');
             });
        };

        if (el.type === 'checkbox') {
            el.addEventListener('change', handler);
        } else {
            el.addEventListener('click', handler);
        }
    });
}

function initNavigation() {
    // Calendar navigation
    const prevBtn = document.getElementById('prevDayBtn') || document.getElementById('prevDay');
    if (prevBtn) {
        prevBtn.addEventListener('click', () => {
             adjustCurrentDay(-1);
             loadAndRender();
        });
    }

    const nextBtn = document.getElementById('nextDayBtn') || document.getElementById('nextDay');
    if (nextBtn) {
        nextBtn.addEventListener('click', () => {
             adjustCurrentDay(1);
             loadAndRender();
        });
    }

    document.getElementById('todayBtn')?.addEventListener('click', () => {
        // Instant reset: Update state and re-render without page reload
        setCurrentDay(new Date());
        loadAndRender();
    });

    // View Toggle
    const viewGroup = document.getElementById('viewModeGroup'); // Radio buttons
    if (viewGroup) {
        viewGroup.addEventListener('change', (e) => {
             if (e.target.name === 'viewMode') {
                 setViewMode(e.target.value);
                 loadAndRender();
             }
        });
    }
}

function initHotkeys(taskModalOpen) {
    document.addEventListener('keydown', (e) => {
        if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
            e.preventDefault();
            taskModalOpen(formatDateISO(getCurrentDay())); 
        }
        if (e.key === 'Escape') {
             // Close any open modal
             document.querySelectorAll('.modal-overlay').forEach(m => m.style.display = 'none');
             document.querySelectorAll('.modal.is-open').forEach(m => {
                 m.classList.remove('is-open');
                 m.setAttribute('aria-hidden', 'true');
             });
        }
    });
}

function isAppPage() {
    return window.location.href.includes('/dashboard/') || 
           window.location.href.includes('/calendar/') || 
           window.location.href.includes('/tasks/') ||
           window.location.href.includes('/analytics/');
}

function isAuthPage() {
    return window.location.href.includes('/login/') || window.location.href.includes('/signup/');
}

function initQuickAdd() {
    const input = document.getElementById('quickTaskInput');
    const btn = document.getElementById('quickAddBtn');
    if (!input || !btn) return;

    const handleAdd = async () => {
        const val = input.value.trim();
        if (!val) return;
        
        const originalText = btn.innerHTML;
        btn.disabled = true;
        btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i>'; 

        try {
            const parsed = parseSmartInput(val);
            // Default to today if no specific date mentioned
            const today = new Date().toISOString().slice(0, 10);
            const dateToUse = parsed.date || today;

            const data = {
                name: parsed.title,
                due: dateToUse,
                estimateMinutes: parsed.duration || 30, // Default 30 min for quick tasks
                priority: parsed.priority || 'medium',
                complete: false,
                category: parsed.category || null
            };
            
            await createUserTask(data);
            showToast('Task added!', 'success');
            input.value = '';
            
            // Reload to update stats and lists
            setTimeout(() => window.location.reload(), 500);

        } catch (err) {
            console.error(err);
            showToast('Error adding task', 'error');
        } finally {
            btn.disabled = false;
            btn.innerHTML = originalText;
        }
    };

    btn.addEventListener('click', handleAdd);
    input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') handleAdd();
    });
}

// --- Smart Reminders ------------------------------------------------
const reminderState = { timer: null, notified: new Set() };
function startSmartReminders() {
    if (reminderState.timer) return;
    const poll = async () => {
        try {
            await autoRescheduleOverdueTasks(new Date());
        } catch (e) {
            console.warn('Auto-reschedule warning:', e);
        }

        let tasks = [];
        try { tasks = await listUserTasks(); } catch (e) { return; }
        const now = new Date();
        const leadMinutes = 15; // remind 15 minutes before start
        const graceMinutes = 5; // keep reminding a few minutes after start if missed

        tasks.forEach(t => {
            if (t.complete || !t.assigned) return;
            const start = new Date(t.assigned);
            if (isNaN(start)) return;
            const diff = (start - now) / 60000; // minutes
            if (diff <= leadMinutes && diff >= -graceMinutes) {
                const id = t.$id || t.id || t.name;
                if (reminderState.notified.has(id)) return;
                reminderState.notified.add(id);
                const timeStr = start.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
                showToast(`${t.name || 'Task'} @ ${timeStr}`, 'info', {
                    label: 'Open planner',
                    onClick: () => { window.location.href = '../calendar/index.html'; }
                }, 'Starts soon');
            }
        });
    };
    reminderState.timer = setInterval(poll, 60 * 1000);
    poll();
}

async function runApp() {
    console.log('Starting PolyTask...');
    initTheme();

    // 1. Auth Check
    const user = await checkSession();
    
    // Redirect logic
    if (!user) {
        if (isAppPage()) {
            window.location.href = '../login/index.html'; 
            return;
        }
    } else {
        // User is logged in
        if (isAuthPage()) {
            window.location.href = '../dashboard/index.html';
            return;
        }
    }

    // Initialize Auth UI (listeners for login forms if they exist)
    initAuth();

    // Check if we have the app container (Dashboard/Calendar/Tasks) or specific container
    const appContainer = document.getElementById('appContainer') || 
                         document.querySelector('.timeline-container') ||
                         document.querySelector('.calendar-large') || 
                         isAppPage(); // Allow execution if we are on a known app page

    const loginSection = document.getElementById('loginSection');

    // Handle SPA-like toggling if elements exist (e.g. if we are on a single page)
    if (user && appContainer && loginSection) {
        loginSection.style.display = 'none';
        if (typeof appContainer.style !== 'undefined') appContainer.style.display = 'block';
    }

    if (user) {
        const avatar = document.getElementById('userAvatar');
        if (avatar) avatar.innerText = user.name ? user.name.charAt(0).toUpperCase() : 'U';
        document.getElementById('logoutBtn')?.addEventListener('click', logout);

        // Only init DB and heavyweight stuff if we are arguably IN the app
        if (appContainer) {
            try {
                await ensureSchema();
            } catch (err) {
                console.warn('Schema init error', err);
            }

            initNavigation();
            // initCalendarInteractions removed as it was empty
            try {
                const { openCreateModal } = initTaskModal();
                const { openScheduleModal } = initScheduleModal();
                const { openGeneralEditModal } = initEditModal();
                setModalHandlers(openCreateModal, openScheduleModal, openGeneralEditModal);
                initHotkeys(openCreateModal);
            } catch (e) { console.warn('Modal init failed', e); }

            initTimer();
            initQuickAdd();
            startSmartReminders();
            
            // Start periodic achievement sync (every 5 minutes)
            setInterval(() => {
                syncAchievementsFromLocal().catch(e => console.warn('Periodic sync failed:', e));
            }, 5 * 60 * 1000);

            // Run calendar render only if calendar element exists, otherwise simple data init is enough
            if (document.getElementById('calendar')) {
                 await loadAndRender();
            }

            // Background tasks
            setTimeout(async () => {
                try {
                    const rescheduled = await autoRescheduleOverdueTasks(new Date());
                    if (rescheduled > 0) {
                        showToast(`${rescheduled} overdue task${rescheduled === 1 ? '' : 's'} rescheduled`, 'info');
                    }
                    await categorizeTasks();
                    await autoSchedule(getCurrentDay());
                    if (document.getElementById('calendar')) loadAndRender(); 
                } catch (err) {
                    console.warn('Background tasks warning:', err);
                }
            }, 1000);
        }
    } else {
         // Not logged in.
         // If loginSection exists (login page), ensure it's visible.
         if (loginSection) loginSection.style.display = 'flex';
         if (appContainer && typeof appContainer.style !== 'undefined') appContainer.style.display = 'none';
    }
    
    // Sync achievements to server on app startup if logged in
    if (user) {
        try {
            await syncAchievementsFromLocal();
        } catch (e) {
            console.warn('Failed to sync achievements on startup:', e);
        }
    }
}

// Start
runApp().catch(err => console.error('Fatal Error:', err));

// Expose Core API for Dashboard and Layouts
window.PolyTask = {
    getCurrentUser: checkSession,
    logout: logout,
    listUserTasks: listUserTasks,
    createUserTask: createUserTask,
    updateUserTask: updateUserTask,
    deleteUserTask: deleteUserTask,
    parseSmartInput: parseSmartInput,
    fireConfetti: fireConfetti,
    calculateStreak: calculateStreak,
    clearCompletedTasks: clearCompletedTasks,
    autoRescheduleOverdueTasks: autoRescheduleOverdueTasks,
    updateStreak: updateStreak,
    loadAchievementsFromServer: loadAchievementsFromServer,
    loadStatsFromServer: loadStatsFromServer,
    syncAchievementsFromLocal: syncAchievementsFromLocal,
    
    // Dashboard Focus Widget Logic
    focusState: {
        timer: null,
        isRunning: false,
        timeLeft: 25 * 60,
        defaultTime: 25 * 60
    },
    toggleFocus: function() {
        const btn = document.querySelector('button[onclick="PolyTask.toggleFocus()"]');
        
        if (this.focusState.isRunning) {
            // Pause
            clearInterval(this.focusState.timer);
            this.focusState.isRunning = false;
            if (btn) btn.textContent = 'Start';
        } else {
            // Start
            if (btn) btn.textContent = 'Pause';
            this.focusState.isRunning = true;
            this.focusState.timer = setInterval(() => {
                this.focusState.timeLeft--;
                if (this.focusState.timeLeft <= 0) {
                    this.focusState.timeLeft = 0;
                    this.resetFocus();
                    showToast('Focus completed!', 'success');
                }
                this.updateFocusWidget();
            }, 1000);
        }
    },
    resetFocus: function() {
        clearInterval(this.focusState.timer);
        this.focusState.isRunning = false;
        this.focusState.timeLeft = this.focusState.defaultTime;
        this.updateFocusWidget();
        const btn = document.querySelector('button[onclick="PolyTask.toggleFocus()"]');
        if (btn) btn.textContent = 'Start';
    },
    updateFocusWidget: function() {
        const display = document.getElementById('focusDisplay');
        const ring = document.getElementById('focusRing');
        if (!display) return;
        
        const m = Math.floor(this.focusState.timeLeft / 60);
        const s = this.focusState.timeLeft % 60;
        display.textContent = `${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;
        
        if (ring) {
            const total = 377; // Circumference for r=60
            const progress = this.focusState.timeLeft / this.focusState.defaultTime;
            const offset = total - (progress * total);
            ring.style.strokeDashoffset = offset;
        }
    }
};

window.toggleDarkMode = () => {
    const btn = document.getElementById('toggleThemeBtn');
    if (btn) {
        btn.click();
    } else {
         const root = document.documentElement;
         root.classList.toggle('dark');
         if (document.body) document.body.classList.toggle('dark', root.classList.contains('dark'));
         const currentTheme = root.classList.contains('dark') ? 'dark' : 'light';
         localStorage.setItem('theme', currentTheme);
         root.style.colorScheme = currentTheme;
    }
};
