// Import Transformers.js for semantic categorization
import { pipeline, env } from 'https://cdn.jsdelivr.net/npm/@xenova/transformers@2.6.0';

// Skip local model checks since we are running in a browser environment
env.allowLocalModels = false;
env.useBrowserCache = true;

let extractor = null;

// script expects Appwrite UMD to be loaded via a <script src="https://cdn.jsdelivr.net/npm/appwrite@21.4.0"></script>
document.addEventListener('DOMContentLoaded', () => {
		// --- schedule modal popup logic ---
		const schedulePopup = document.getElementById('createTaskBtn-popup');
		const defineScheduleModal = document.getElementById('defineScheduleModal');
		const cancelScheduleBtn = document.getElementById('cancelSchedule');
			const scheduleForm = document.getElementById('scheduleForm');
			const repeatWeeklyEl = document.getElementById('repeatWeekly');
			let scheduleExistingDocs = [];
			function openScheduleModal() {
				// clear and populate from existing blocked events
				clearScheduleUI();
				populateScheduleFromExisting().finally(() => {
					defineScheduleModal.style.display = 'flex';
				});
			}
			if (schedulePopup && defineScheduleModal) {
				schedulePopup.addEventListener('click', (e) => {
					e.stopPropagation();
					openScheduleModal();
				});
			}
		if (cancelScheduleBtn && defineScheduleModal) {
			cancelScheduleBtn.addEventListener('click', () => {
				defineScheduleModal.style.display = 'none';
			});
		}
			// helper to add an interval row for a specific day container
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
			// attach add-interval handlers
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
						if (!PolyTask || typeof PolyTask.listUserTasks !== 'function') return;
						const docs = await PolyTask.listUserTasks();
						const base = weekStartOf(window.currentDay || new Date());
						const weekStart = new Date(base);
						const weekEnd = new Date(base); weekEnd.setDate(weekEnd.getDate() + 7);
						const isBlocked = d => (String(d.category||'').toLowerCase()==='blocked');
						const inWeek = iso => { const dt = new Date(iso); return dt >= weekStart && dt < weekEnd; };
						// Prefer explicit weekly repeats; else use this week's blocked events
						let picked = docs.filter(d => isBlocked(d) && d.repeat === true);
						if (!picked.length) picked = docs.filter(d => isBlocked(d) && d.assigned && inWeek(d.assigned));

						// remember chosen docs (for update/delete on save)
						scheduleExistingDocs = picked.slice();

						// reset UI and populate one row per doc (attach docId for update tracking)
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

						// checkbox state reflects whether any picked docs are weekly
						if (repeatWeeklyEl) repeatWeeklyEl.checked = picked.some(d => d.repeat === true);
					}

			function weekStartOf(date) {
				const d = new Date(date);
				d.setHours(0,0,0,0);
				const day = d.getDay(); // 0 Sun .. 6 Sat
				d.setDate(d.getDate() - day);
				return d;
			}
			function parseHHMM(str) {
				if (!str || typeof str !== 'string' || !/^[0-9]{2}:[0-9]{2}$/.test(str)) return null;
				const [hh, mm] = str.split(':').map(n => parseInt(n,10));
				if (hh<0||hh>23||mm<0||mm>59) return null;
				return { hh, mm, minutes: hh*60+mm };
			}
			// on save, create events for the current week
			if (scheduleForm) {
				scheduleForm.addEventListener('submit', async (e) => {
					e.preventDefault();
					const errors = [];
					const intervalsByDay = {};
						const base = weekStartOf(window.currentDay || new Date());
						const weekStart = new Date(base);
						const weekEnd = new Date(base); weekEnd.setDate(weekEnd.getDate() + 7);
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
							if (!s || !e2) return; // skip incomplete
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
					// build payloads
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
									repeat: repeatVal
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
									// Update existing rows
									// Parallelize updates for performance
									await Promise.all(toUpdate.map(({ id, payload }) => PolyTask.updateUserTask(id, payload)));
									
									// Create new rows
									await Promise.all(toCreate.map(payload => PolyTask.createUserTask(payload)));
									
									// Delete removed rows (docs we loaded but didn't keep)
									const existing = Array.isArray(scheduleExistingDocs) ? scheduleExistingDocs : [];
									await Promise.all(
										existing
											.filter(d => d && d.$id && !seenDocIds.has(d.$id))
											.map(d => PolyTask.deleteUserTask(d.$id))
									);
								defineScheduleModal.style.display = 'none';
								// refresh to show new blocked events
								await PolyTask.loadAndRender();
								showToast('Schedule updated', 'success');
					} catch (err) {
						console.error('Failed to create blocked events', err);
						showToast(err.message || 'Failed to create blocked events', 'error');
					}
				});
			}

				// --- general edit modal (for non-blocked events) ------------------
				const editEventModal = document.getElementById('editEventModal');
				const editEventForm = document.getElementById('editEventForm');
				const cancelEditEventBtn = document.getElementById('cancelEditEvent');
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

				function closeEditModal() { if (editEventModal) editEventModal.style.display = 'none'; }
				if (cancelEditEventBtn) cancelEditEventBtn.addEventListener('click', closeEditModal);

				// Expose a global opener so calendar cards can call it
				try {
					window.openGeneralEditModal = function (task) {
						if (!editEventModal) return;
						const id = String(task.id || '');
						// Unwrap virtual weekly id to original id if needed
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
						editEventModal.style.display = 'flex';
					};
				} catch (_) {}

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
							await PolyTask.updateUserTask(editDocId.value, payload);
							closeEditModal();
							await PolyTask.loadAndRender();
							showToast('Event updated', 'success');
						} catch (err) {
							console.error('Update event failed', err);
							showToast(err.message || 'Failed to update event', 'error');
						}
					});
				}

		const PolyTask = (function () {
		// --- config & state -------------------------------------------------
		const APPWRITE_ENDPOINT = window.APPWRITE_ENDPOINT || 'https://nyc.cloud.appwrite.io/v1';
		// Hard-coded Appwrite project ID — replace with your real project id
		const APPWRITE_PROJECT = 'polytask';
		const APPWRITE_DATABASE = window.APPWRITE_DATABASE || window.APPWRITE_DATABASE_ID || 'events';

		let cachedUserId = null;
		let AppwriteModule = null;
		let currentDay = startOfDay(new Date());

		async function invokeWithCompat(target, methodName, legacyArgs = [], modernPayload) {
			const fn = target && typeof target[methodName] === 'function' ? target[methodName] : null;
			if (!fn) return { called: false, value: undefined };
			const arity = Number.isInteger(fn.length) ? fn.length : legacyArgs.length;
			const value = arity <= 1
				? await fn.call(target, modernPayload !== undefined ? modernPayload : legacyArgs[0])
				: await fn.apply(target, legacyArgs);
			return { called: true, value };
		}

		// --- small helpers --------------------------------------------------
		function fireConfetti() {
			const colors = ['#EF6F6C', '#465775', '#F7B074', '#FFF07C', '#ACDD91', '#59C9A5'];
			for (let i = 0; i < 40; i++) {
				const el = document.createElement('div');
				el.className = 'confetti';
				// Random start position near top (rain effect)
				el.style.left = Math.random() * 100 + 'vw';
				el.style.top = '-10px';
				el.style.backgroundColor = colors[Math.floor(Math.random() * colors.length)];
				// Random fall speed and delay
				const duration = Math.random() * 1.5 + 1.5; 
				el.style.animation = `fall ${duration}s linear forwards`;
				el.style.animationDelay = (Math.random() * 0.5) + 's';
				
				document.body.appendChild(el);
				setTimeout(() => el.remove(), (duration + 1) * 1000);
			}
		}

		function showToast(message, type = 'info', action = null) {
			let container = document.getElementById('toast-container');
			if (!container) {
				container = document.createElement('div');
				container.id = 'toast-container';
				document.body.appendChild(container);
			}
			const toast = document.createElement('div');
			toast.className = `toast ${type}`;
			
			const text = document.createElement('span');
			text.textContent = message;
			toast.appendChild(text);

			if (action && action.label && typeof action.onClick === 'function') {
				const btn = document.createElement('button');
				btn.textContent = action.label;
				btn.style.marginLeft = '12px';
				btn.style.padding = '4px 8px';
				btn.style.border = '1px solid currentColor';
				btn.style.borderRadius = '4px';
				btn.style.background = 'transparent';
				btn.style.color = 'inherit';
				btn.style.cursor = 'pointer';
				btn.style.fontSize = '0.8rem';
				btn.addEventListener('click', (e) => {
					e.stopPropagation();
					action.onClick();
					toast.remove();
				});
				toast.appendChild(btn);
			}

			container.appendChild(toast);
			// Auto remove (longer if there is an action)
			const duration = action ? 6000 : 3000;
			setTimeout(() => {
				if (toast.parentElement) {
					toast.classList.add('fade-out');
					toast.addEventListener('animationend', () => toast.remove());
				}
			}, duration);
		}

		function showFormError(form, msg) {
			if (!form) return;
			const el = form.querySelector('#formError') || form.querySelector('#pwMismatch');
			if (el) {
				el.style.display = msg ? 'block' : 'none';
				el.textContent = msg || '';
			} else if (msg) showToast(msg, 'error');
		}

		async function ensureAppwriteClient() {
			// Expect the Appwrite UMD global (window.Appwrite) to be present because
			// the SDK is included directly in the HTML via CDN.
			if (typeof window !== 'undefined' && window.Appwrite) {
				AppwriteModule = window.Appwrite;
				return new AppwriteModule.Client().setEndpoint(APPWRITE_ENDPOINT).setProject(APPWRITE_PROJECT);
			}
			console.error('Appwrite SDK not found on window. Please include the CDN script before script.js: <script src="https://cdn.jsdelivr.net/npm/appwrite@21.4.0"></script>');
			throw new Error('Appwrite SDK not loaded');
		}

		function escapeHtml(s) { return String(s || '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":"&#39;"}[c])); }

		// --- collection management ------------------------------------------
		async function ensureUserCollection(userId) {
			if (!userId) throw new Error('User ID required to ensure collection');
			const client = await ensureAppwriteClient();
			const App = AppwriteModule;
			const db = new App.Databases(client);
			let collectionExists = false;
			// Check if collection exists
			try {
				const existing = await invokeWithCompat(db, 'getCollection', [APPWRITE_DATABASE, userId], {
					databaseId: APPWRITE_DATABASE,
					collectionId: userId
				});
				if (existing.called && existing.value) {
					console.log(`Collection ${userId} already exists`);
					collectionExists = true;
					// Try to ensure new attributes like 'completed' exist
					try { await invokeWithCompat(db, 'createBooleanAttribute', [APPWRITE_DATABASE, userId, 'completed', false, false], { databaseId: APPWRITE_DATABASE, collectionId: userId, key: 'completed', required: false, default: false }); } catch (e) {}
					try { await invokeWithCompat(db, 'createStringAttribute', [APPWRITE_DATABASE, userId, 'repeat', 20, false], { databaseId: APPWRITE_DATABASE, collectionId: userId, key: 'repeat', size: 20, required: false }); } catch (e) {}
					try { await invokeWithCompat(db, 'createStringAttribute', [APPWRITE_DATABASE, userId, 'priority', 10, false, 'medium'], { databaseId: APPWRITE_DATABASE, collectionId: userId, key: 'priority', size: 10, required: false, default: 'medium' }); } catch (e) {}
					return existing.value;
				}
				// Try legacy signature
				const col = await db.getCollection(APPWRITE_DATABASE, userId);
				if (col) {
					console.log(`Collection ${userId} already exists`);
					collectionExists = true;
					try { await invokeWithCompat(db, 'createBooleanAttribute', [APPWRITE_DATABASE, userId, 'completed', false, false], { databaseId: APPWRITE_DATABASE, collectionId: userId, key: 'completed', required: false, default: false }); } catch (e) {}
					try { await invokeWithCompat(db, 'createStringAttribute', [APPWRITE_DATABASE, userId, 'repeat', 20, false], { databaseId: APPWRITE_DATABASE, collectionId: userId, key: 'repeat', size: 20, required: false }); } catch (e) {}
					try { await invokeWithCompat(db, 'createStringAttribute', [APPWRITE_DATABASE, userId, 'priority', 10, false, 'medium'], { databaseId: APPWRITE_DATABASE, collectionId: userId, key: 'priority', size: 10, required: false, default: 'medium' }); } catch (e) {}
					return col;
				}
			} catch (err) {
				// Collection doesn't exist, create it
				console.log(`Collection ${userId} not found, creating...`);
			}

			// Create collection with user ID as collection ID and set permissions
			// Permissions: allow the owner (user) to read, create, update, and delete their own documents
			const permissions = [
				`read("user:${userId}")`,
				`create("user:${userId}")`,
				`update("user:${userId}")`,
				`delete("user:${userId}")`
			];
			
			const created = await invokeWithCompat(db, 'createCollection', [APPWRITE_DATABASE, userId, userId, permissions, false], {
				databaseId: APPWRITE_DATABASE,
				collectionId: userId,
				name: userId,
				permissions: permissions,
				documentSecurity: false
			});
			const collection = created.called ? created.value : await db.createCollection(APPWRITE_DATABASE, userId, userId, permissions, false);
			console.log(`Created collection ${userId} with permissions:`, permissions);

			// Create attributes matching existing schema: due, assigned, category, color, name
			// due (datetime, required)
			await invokeWithCompat(db, 'createDatetimeAttribute', [APPWRITE_DATABASE, userId, 'due', true], {
				databaseId: APPWRITE_DATABASE,
				collectionId: userId,
				key: 'due',
				required: true
			});

			// assigned (datetime, optional)
			await invokeWithCompat(db, 'createDatetimeAttribute', [APPWRITE_DATABASE, userId, 'assigned', false], {
				databaseId: APPWRITE_DATABASE,
				collectionId: userId,
				key: 'assigned',
				required: false
			});

			// category (string, optional, size 20)
			await invokeWithCompat(db, 'createStringAttribute', [APPWRITE_DATABASE, userId, 'category', 20, false], {
				databaseId: APPWRITE_DATABASE,
				collectionId: userId,
				key: 'category',
				size: 20,
				required: false
			});

			// color (string, optional, size 20, default 'cadet')
			await invokeWithCompat(db, 'createStringAttribute', [APPWRITE_DATABASE, userId, 'color', 20, false, 'cadet'], {
				databaseId: APPWRITE_DATABASE,
				collectionId: userId,
				key: 'color',
				size: 20,
				required: false,
				default: 'cadet'
			});

			// name (string, required, size 50)
			await invokeWithCompat(db, 'createStringAttribute', [APPWRITE_DATABASE, userId, 'name', 50, true], {
				databaseId: APPWRITE_DATABASE,
				collectionId: userId,
				key: 'name',
				size: 50,
				required: true
			});

			// estimateMinutes (integer, optional, min 1)
			try {
				await invokeWithCompat(db, 'createIntegerAttribute', [APPWRITE_DATABASE, userId, 'estimateMinutes', false, 1], {
					databaseId: APPWRITE_DATABASE,
					collectionId: userId,
					key: 'estimateMinutes',
					required: false,
					min: 1
				});
			} catch (e) {
				// ignore if already exists or if backend rejects optional bounds signature
			}

			console.log(`Created attributes for collection ${userId}`);
			// completed (boolean, optional, default false)
			try {
				await invokeWithCompat(db, 'createBooleanAttribute', [APPWRITE_DATABASE, userId, 'completed', false, false], {
					databaseId: APPWRITE_DATABASE,
					collectionId: userId,
					key: 'completed',
					required: false,
					default: false
				});
			} catch (e) { /* ignore if exists */ }
			// repeat (string, optional, size 20)
			try {
				await invokeWithCompat(db, 'createStringAttribute', [APPWRITE_DATABASE, userId, 'repeat', 20, false], {
					databaseId: APPWRITE_DATABASE,
					collectionId: userId,
					key: 'repeat',
					size: 20,
					required: false
				});
			} catch (e) { /* ignore if exists */ }
			// priority (string, optional, size 10)
			try {
				await invokeWithCompat(db, 'createStringAttribute', [APPWRITE_DATABASE, userId, 'priority', 10, false, 'medium'], {
					databaseId: APPWRITE_DATABASE,
					collectionId: userId,
					key: 'priority',
					size: 10,
					required: false,
					default: 'medium'
				});
			} catch (e) { /* ignore if exists */ }
			return collection;
		}

		// --- auth helpers --------------------------------------------------
		async function getCurrentUserId() {
			if (cachedUserId) return cachedUserId;
			try {
				const client = await ensureAppwriteClient();
				const App = AppwriteModule || (typeof window !== 'undefined' && window.Appwrite) || null;
				if (!App) return null;
				const account = new App.Account(client);
				const u = await account.get();
				cachedUserId = u && (u.$id || u.$uid || u.id);
				return cachedUserId || null;
			} catch (err) {
				// common reasons: not authenticated or network/CORS issues
				console.warn('Could not get current Appwrite user:', err && err.message ? err.message : err);
				return null;
			}
		}

		// --- db helpers ----------------------------------------------------
		async function listUserTasks() {
			const userId = await getCurrentUserId();
			if (!userId) return [];
			const client = await ensureAppwriteClient();
			const App = AppwriteModule;
			const db = new App.Databases(client);
			if (!App.Query || typeof db.listDocuments !== 'function') throw new Error('Appwrite Databases.listDocuments or Query not available');
			// Use userId as collection ID
			const modern = await invokeWithCompat(db, 'listDocuments', [APPWRITE_DATABASE, userId], {
				databaseId: APPWRITE_DATABASE,
				collectionId: userId
			});
			const res = modern.called ? modern.value : await db.listDocuments(APPWRITE_DATABASE, userId);
			return (res && res.documents) || [];
		}

		async function createUserTask(data) {
			const userId = await getCurrentUserId();
			if (!userId) throw new Error('User not authenticated');
			const client = await ensureAppwriteClient();
			const App = AppwriteModule;
			const db = new App.Databases(client);
			// No need for ownerId - each user has their own collection
			if (typeof db.createDocument !== 'function') throw new Error('Appwrite Databases.createDocument not available');
			const uniqueId = App.ID && typeof App.ID.unique === 'function' ? App.ID.unique() : 'unique()';
			// Normalize payload to match collection schema
			const payload = {
				name: data.name,
				due: data.due,
				assigned: data.assigned || null,
				category: data.category || null,
				color: data.color || 'cadet',
				estimated_time: typeof data.estimated_time === 'number' ? data.estimated_time : (typeof data.estimateMinutes === 'number' ? data.estimateMinutes : 60),
				complete: data.complete === true ? true : false,
				repeat: data.repeat || null,
				priority: data.priority || 'medium'
			};
			// Use userId as collection ID
			const created = await invokeWithCompat(db, 'createDocument', [APPWRITE_DATABASE, userId, uniqueId, payload], {
				databaseId: APPWRITE_DATABASE,
				collectionId: userId,
				documentId: uniqueId,
				data: payload
			});
			if (created.called) return created.value;
			return await db.createDocument(APPWRITE_DATABASE, userId, uniqueId, payload);
		}

		// Categorize tasks with null categories using semantic ML
		async function categorizeTasks() {
			try {
				const userId = await getCurrentUserId();
				if (!userId) return;
				
				const client = await ensureAppwriteClient();
				const App = AppwriteModule;
				const db = new App.Databases(client);
				
				// Get all tasks
				const listResult = await invokeWithCompat(db, 'listDocuments', [APPWRITE_DATABASE, userId], {
					databaseId: APPWRITE_DATABASE,
					collectionId: userId
				});
				const allDocs = listResult.called ? listResult.value.documents : (await db.listDocuments(APPWRITE_DATABASE, userId)).documents;
				
				// Filter tasks that need categorization (null category OR 'General')
				// Skip "Blocked" tasks - they should never be auto-categorized
				const uncategorized = allDocs.filter(d => {
					const isBlocked = String(d.name || '').toLowerCase().includes('blocked') || 
					                  String(d.category || '').toLowerCase() === 'blocked';
					const needsCategorization = !d.category || d.category === null || d.category === 'General';
					return needsCategorization && !isBlocked;
				});
				if (uncategorized.length === 0) return;
				
				// Build knowledge base from categorized tasks (exclude Blocked and General)
				const categorized = allDocs.filter(d => d.category && d.category !== 'Blocked' && d.category !== 'General');
				
				// Initialize ML model
				if (!extractor) {
					extractor = await pipeline('feature-extraction', 'Xenova/all-MiniLM-L6-v2');
				}
				
				// Helper functions
				const normalize = (s) => String(s || '').toLowerCase().replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim();
				// CSS-defined color palette (matching styles.css :root variables)
				const palette = [
					'#EF6F6C',  // coral
					'#465775',  // dark-slate
					'#F7B074',  // sandy
					'#FFF07C',  // maize
					'#ACDD91',  // celadon
					'#59C9A5',  // mint
					'#50908D',  // dark-cyan
					'#715D73',  // violet
					'#9B6371',  // rose
					'#93A8AC'   // cadet
				];
				
				function mean(vecs) {
					const dim = vecs[0].length;
					const out = new Float32Array(dim);
					for (const v of vecs) {
						for (let i = 0; i < dim; i++) out[i] += v[i];
					}
					for (let i = 0; i < dim; i++) out[i] /= vecs.length;
					return out;
				}
				
				function cosine(a, b) {
					let dot = 0, na = 0, nb = 0;
					for (let i = 0; i < a.length; i++) {
						const x = a[i], y = b[i];
						dot += x * y;
						na += x * x;
						nb += y * y;
					}
					return dot / (Math.sqrt(na) * Math.sqrt(nb) + 1e-8);
				}
				
				async function embed(text) {
					const out = await extractor(text, { pooling: 'mean', normalize: true });
					if (out.data) return out.data;
					if (Array.isArray(out) && out.length > 0) {
						if (out[0].data) return out[0].data;
						if (Array.isArray(out[0])) return out[0]; // Should not happen with pooling
						return out;
					}
					return out;
				}
				
				// Build parent category registry and color mapping
				const parentCats = new Set();
				const parentToColor = new Map();
				const exemplars = [];
				
				categorized.forEach(d => {
					const cat = d.category || '';
					if (cat) {
						parentCats.add(cat);
						// Preserve user-selected colors (if they manually changed color in edit modal)
						// Only use palette colors if the color is already from our palette
						const userColor = d.color;
						if (userColor && palette.includes(userColor.toUpperCase())) {
							// It's a palette color, use it for consistency
							parentToColor.set(cat, userColor);
						} else if (userColor && !palette.includes(userColor.toUpperCase())) {
							// User picked a custom color - preserve it for this category
							parentToColor.set(cat, userColor);
						}
						exemplars.push({ name: d.name || '', cat });
					}
				});
				
				// Assign palette colors to parent categories that don't have one yet
				const sortedParents = [...parentCats].sort();
				sortedParents.forEach((cat, i) => {
					if (!parentToColor.has(cat)) {
						parentToColor.set(cat, palette[i % palette.length]);
					}
				});
				
				// Build name-to-category exact match map
				const nameToCat = new Map();
				exemplars.forEach(ex => {
					nameToCat.set(normalize(ex.name), ex.cat);
				});
				
				// Compute per-category centroids (limit examples per category to prevent dominance)
				const perCat = new Map();
				const MAX_EXAMPLES_PER_CAT = 10; // Prevent any single category from dominating
				for (const ex of exemplars) {
					if (!perCat.has(ex.cat)) perCat.set(ex.cat, []);
					if (perCat.get(ex.cat).length < MAX_EXAMPLES_PER_CAT) {
						perCat.get(ex.cat).push({ name: ex.name, cat: ex.cat });
					}
				}
				
				// Compute embeddings for balanced exemplars
				const categoryEmbeddings = new Map();
				for (const [cat, examples] of perCat) {
					const embeddings = [];
					for (const ex of examples) {
						embeddings.push(await embed(ex.name));
					}
					categoryEmbeddings.set(cat, embeddings);
				}
				
				const centroids = new Map();
				for (const [cat, vecs] of categoryEmbeddings) {
					centroids.set(cat, mean(vecs));
				}
				
				// Subject-specific keyword detection
				function detectSubjectByKeywords(title) {
					const t = normalize(title);
					const keywords = {
						'Math': ['math', 'calculus', 'algebra', 'geometry', 'trigonometry', 'statistics', 'equation'],
						'Science': ['biology', 'chemistry', 'physics', 'lab', 'experiment', 'bio', 'chem'],
						'English': ['essay', 'literature', 'writing', 'poem', 'novel', 'reading', 'grammar'],
						'History': ['history', 'historical', 'war', 'civilization', 'ancient', 'revolution'],
						'Computer Science': ['programming', 'coding', 'algorithm', 'code', 'software', 'debug', 'cs'],
						'Language': ['spanish', 'french', 'german', 'chinese', 'japanese', 'language', 'vocabulary'],
						'Art': ['art', 'drawing', 'painting', 'sketch', 'design', 'creative'],
						'Music': ['music', 'piano', 'guitar', 'song', 'practice', 'instrument'],
						'PE': ['gym', 'exercise', 'workout', 'physical', 'sports', 'fitness'],
						'Social Studies': ['geography', 'economics', 'government', 'politics', 'society']
					};
					
					for (const [subject, words] of Object.entries(keywords)) {
						for (const word of words) {
							if (t.includes(word)) {
								return subject;
							}
						}
					}
					return null;
				}
				
				// Categorize each uncategorized task (in parallel)
				const updatePromises = uncategorized.map(async (task) => {
					let finalCategory = 'General';
					let finalColor = '#3b82f6';
					
					const title = task.name || '';
					
					// Check exact match first
					const exact = nameToCat.get(normalize(title));
					if (exact) {
						finalCategory = exact;
						finalColor = parentToColor.get(exact) || palette[0];
					} else {
						// Try keyword detection
						const keywordMatch = detectSubjectByKeywords(title);
						if (keywordMatch) {
							// Check if this category already exists in our system
							if (parentCats.has(keywordMatch)) {
								finalCategory = keywordMatch;
								finalColor = parentToColor.get(keywordMatch) || palette[0];
							} else {
								// New category detected via keywords
								finalCategory = keywordMatch;
								parentCats.add(keywordMatch);
								const newIndex = [...parentCats].sort().indexOf(keywordMatch);
								finalColor = palette[newIndex % palette.length];
								parentToColor.set(keywordMatch, finalColor);
							}
						} else if (centroids.size > 0) {
							// Semantic similarity with HIGHER threshold for better accuracy
							const emb = await embed(title);
							let bestCat = null, best = 0;
							const scores = [];
							
							for (const [cat, cen] of centroids) {
								const sim = cosine(emb, cen);
								scores.push({ cat, sim });
								if (sim > best) {
									best = sim;
									bestCat = cat;
								}
							}
							
							// Use top match only if it's clearly better (higher threshold + margin)
							scores.sort((a, b) => b.sim - a.sim);
							const secondBest = scores[1]?.sim || 0;
							const margin = best - secondBest;
							
							// Require high confidence (0.65+) OR clear winner (0.55+ with 0.1+ margin)
							if (bestCat && (best >= 0.65 || (best >= 0.55 && margin >= 0.1))) {
								finalCategory = bestCat;
								finalColor = parentToColor.get(bestCat) || palette[0];
							} else {
								// Not confident enough - keep as General
								if (!parentToColor.has(finalCategory)) {
									parentToColor.set(finalCategory, palette[sortedParents.length % palette.length]);
								}
								finalColor = parentToColor.get(finalCategory);
							}
						}
					}
					
					// Update the task
					// Skip update if nothing changed (e.g. General -> General)
					if (task.category === finalCategory && task.color === finalColor) return;

					const updatePayload = {
						...task,
						category: finalCategory,
						color: finalColor
					};
					delete updatePayload.$id;
					delete updatePayload.$collectionId;
					delete updatePayload.$databaseId;
					delete updatePayload.$createdAt;
					delete updatePayload.$updatedAt;
					delete updatePayload.$permissions;
					
					return invokeWithCompat(db, 'updateDocument', [APPWRITE_DATABASE, userId, task.$id, updatePayload], {
						databaseId: APPWRITE_DATABASE,
						collectionId: userId,
						documentId: task.$id,
						data: updatePayload
					});
				});

				await Promise.all(updatePromises);
				
				console.log(`Categorized ${uncategorized.length} tasks`);
			} catch (err) {
				console.error('Error categorizing tasks:', err);
			}
		}

		// --- auto scheduler ------------------------------------------------
		async function autoSchedule() {
			const btn = document.getElementById('autoScheduleBtn');
			if (btn) btn.classList.add('btn-loading');
			try {
				const userId = await getCurrentUserId();
				if (!userId) return;

				// 1. Get all tasks
				const allTasks = await listUserTasks();
				
				const currentDayStr = formatDateISO(currentDay);
				const startOfCurrentDay = new Date(currentDay); startOfCurrentDay.setHours(0,0,0,0);
				const endOfCurrentDay = new Date(currentDay); endOfCurrentDay.setHours(23,59,59,999);

				// Fixed tasks: assigned within current day
				const fixedTasks = allTasks.filter(t => {
					if (!t.assigned) return false;
					const d = new Date(t.assigned);
					return d >= startOfCurrentDay && d <= endOfCurrentDay;
				});

				// Floating candidates: unassigned, due today OR in the future, not completed
				const candidates = allTasks.filter(t => {
					if (t.assigned || t.complete) return false;
					if (!t.due) return false;
					const dueStr = new Date(t.due).toISOString().slice(0,10);
					return dueStr >= currentDayStr;
				});

				if (candidates.length === 0) {
					console.log('No unassigned tasks to schedule.');
					return;
				}

				// --- Improved Sorting Logic ---
				// Weights: High=3, Medium=2, Low=1
				const priorityWeight = p => (p === 'high' ? 3 : p === 'low' ? 1 : 2);

				candidates.sort((a, b) => {
					// 1. Due Today always first!
					const dateA = new Date(a.due).toISOString().slice(0,10);
					const dateB = new Date(b.due).toISOString().slice(0,10);
					const isTodayA = (dateA === currentDayStr);
					const isTodayB = (dateB === currentDayStr);
					
					if (isTodayA && !isTodayB) return -1;
					if (!isTodayA && isTodayB) return 1;

					// 2. Priority Descending (High first)
					const pA = priorityWeight(a.priority);
					const pB = priorityWeight(b.priority);
					if (pA !== pB) return pB - pA;

					// 3. Due Date Ascending (Earliest first)
					const dueA = new Date(a.due).getTime();
					const dueB = new Date(b.due).getTime();
					if (dueA !== dueB) return dueA - dueB;
					
					// 4. Duration Descending (Big blocks first)
					const estA = typeof a.estimated_time === 'number' ? a.estimated_time : (typeof a.estimateMinutes === 'number' ? a.estimateMinutes : 60);
					const estB = typeof b.estimated_time === 'number' ? b.estimated_time : (typeof b.estimateMinutes === 'number' ? b.estimateMinutes : 60);
					return estB - estA;
				});

				// 3. Build a map of occupied minutes (06:00 to 24:00)
				const occupied = new Uint8Array(24 * 60); // 0 to 1439 minutes
				// Mark hours 0-6 as occupied
				for (let i = 0; i < 6 * 60; i++) occupied[i] = 1;

				fixedTasks.forEach(t => {
					const d = new Date(t.assigned);
					const startMin = d.getHours() * 60 + d.getMinutes();
					const duration = typeof t.estimated_time === 'number' ? t.estimated_time : (typeof t.estimateMinutes === 'number' ? t.estimateMinutes : 60);
					for (let i = startMin; i < startMin + duration && i < 1440; i++) {
						occupied[i] = 1;
					}
				});

				const BUFFER_MINUTES = 15; // Breathing room between tasks

				let scheduledCount = 0;
				for (const task of candidates) {
					const duration = typeof task.estimated_time === 'number' ? task.estimated_time : (typeof task.estimateMinutes === 'number' ? task.estimateMinutes : 60);
					let bestStart = -1;
					
					// Search for gap: Needs (Duration) free
					for (let i = 6 * 60; i < 24 * 60 - duration; i++) {
						let fits = true;
						for (let k = 0; k < duration; k++) {
							if (occupied[i + k] === 1) { fits = false; break; }
						}
						if (fits) {
							bestStart = i;
							break;
						}
					}

					if (bestStart !== -1) {
						// Mark occupied: Task + Buffer
						// We mark the buffer as occupied so nothing overlaps it.
						const endMark = Math.min(1440, bestStart + duration + BUFFER_MINUTES);
						for (let i = bestStart; i < endMark; i++) occupied[i] = 1;
						
						// Update Task
						const newDate = new Date(currentDay);
						newDate.setHours(Math.floor(bestStart/60), bestStart%60, 0, 0);
						
						await updateUserTask(task.$id || task.id, { assigned: newDate.toISOString() });
						scheduledCount++;
					}
				}

				if (scheduledCount > 0) {
					await loadAndRender();
					showToast(`Auto-scheduled ${scheduledCount} tasks`, 'success');
				} else {
					showToast('Could not find free slots for any tasks', 'info');
				}

			} catch (err) {
				console.error('Auto-schedule error', err);
				showToast('Auto-schedule failed: ' + err.message, 'error');
			} finally {
				if (btn) btn.classList.remove('btn-loading');
			}
		}

		// --- calendar rendering --------------------------------------------
		function startOfDay(date) { const d = new Date(date); d.setHours(0,0,0,0); return d; }
		function formatDateISO(d) { return d.toISOString().slice(0,10); }
		function friendlyDayLabel(d) { return d.toLocaleDateString(undefined, { weekday:'long', month:'short', day:'numeric' }); }

		async function loadAndRender() {
			const calendarEl = document.getElementById('calendar');
			if (!calendarEl) return;
			// expose current day for external UI (schedule modal)
			try { window.currentDay = currentDay; } catch (e) {}
			const display = document.getElementById('currentDateDisplay');
			if (display) display.textContent = friendlyDayLabel(currentDay);
			
			// Only show full loading state if we don't have the calendar structure yet
			if (!calendarEl.querySelector('.calendar-inner')) {
				calendarEl.innerHTML = '<p>Loading tasks...</p>';
			} else {
				// Optional: add a subtle loading class to the existing calendar if desired
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

		// --- Undo System ---
		const undoStack = [];
		const MAX_UNDO_STACK = 20;

		function pushUndoAction(action) {
			undoStack.push(action);
			if (undoStack.length > MAX_UNDO_STACK) undoStack.shift();
		}

		async function performUndo() {
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
					// We need to re-create the document with the same ID logic ideally, 
					// but Appwrite IDs are immutable. We can just create a new one with same data.
					// However, the action likely stores the whole object.
					// For drag/resize, we only use 'update'.
				}
				await loadAndRender();
				showToast('Undone', 'success');
			} catch (e) {
				console.error('Undo failed', e);
				showToast('Undo failed', 'error');
			}
		}

		async function updateUserTask(documentId, patch) {
			const userId = await getCurrentUserId();
			if (!userId) throw new Error('User not authenticated');
			const client = await ensureAppwriteClient();
			const App = AppwriteModule;
			const db = new App.Databases(client);
			const upd = await invokeWithCompat(db, 'updateDocument', [APPWRITE_DATABASE, userId, documentId, patch], {
				databaseId: APPWRITE_DATABASE,
				collectionId: userId,
				documentId: documentId,
				data: patch
			});
			if (upd.called) return upd.value;
			return await db.updateDocument(APPWRITE_DATABASE, userId, documentId, patch);
		}

		async function deleteUserTask(documentId) {
			const userId = await getCurrentUserId();
			if (!userId) throw new Error('User not authenticated');
			const client = await ensureAppwriteClient();
			const App = AppwriteModule;
			const db = new App.Databases(client);
			const del = await invokeWithCompat(db, 'deleteDocument', [APPWRITE_DATABASE, userId, documentId], {
				databaseId: APPWRITE_DATABASE,
				collectionId: userId,
				documentId
			});
			if (del.called) return del.value;
			return await db.deleteDocument(APPWRITE_DATABASE, userId, documentId);
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
				// wrapper for the day
				wrapper = document.createElement('div'); wrapper.className = 'calendar-inner';
				// Remove internal header to avoid duplication with nav bar
				// const header = document.createElement('div'); ... 
				
				// day grid: left times, right events
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
				// Append grid to DOM
				dayGrid.appendChild(timeCol);
				dayGrid.appendChild(eventsCol);
				wrapper.appendChild(dayGrid);
				calendarEl.appendChild(wrapper);
			} else {
				// Update header with new date (Removed to prevent duplication)
				// const header = wrapper.querySelector('div[style*="display"]');
				// if (header) header.innerHTML = `...`;
			}

			const firstSlot = timeCol.querySelector('.time-slot');
			const slotHeight = (firstSlot && firstSlot.clientHeight > 0) ? firstSlot.clientHeight : 48; // fallback to CSS height 48px
			const dayHeight = slotHeight * 72; // 72 quarters in our visible range (06:00-24:00)
			// events layer
			const layer = eventsCol.querySelector('.events-layer') || document.createElement('div');
			if (!layer.parentNode) {
				layer.className = 'events-layer';
				layer.style.position = 'relative';
				layer.style.height = dayHeight + 'px';
				layer.style.marginTop = '12px';
				
				// Clear any existing content (important if we're recovering from a bad state)
				if (eventsCol.innerHTML !== '') eventsCol.innerHTML = '';
				
				eventsCol.appendChild(layer);
				
				// Enable Drop on the layer (only attach once)
				let scrollSpeed = 0;
				let scrollRaf = null;
				let lastDragTime = 0;
				
				function dndScrollLoop() {
					if (scrollSpeed !== 0) {
						// Safety: stop if we haven't seen a dragover in 100ms
						if (Date.now() - lastDragTime > 100) {
							scrollSpeed = 0;
							scrollRaf = null;
							return;
						}
						window.scrollBy(0, scrollSpeed);
						scrollRaf = requestAnimationFrame(dndScrollLoop);
					} else {
						scrollRaf = null;
					}
				}

				layer.addEventListener('dragover', (e) => {
					e.preventDefault();
					e.dataTransfer.dropEffect = 'move';
					
					lastDragTime = Date.now();
					let newSpeed = 0;
					
					if (e.clientY < 60) newSpeed = -15;
					else if (window.innerHeight - e.clientY < 60) newSpeed = 15;
					
					if (newSpeed !== 0) {
						scrollSpeed = newSpeed;
						if (!scrollRaf) dndScrollLoop();
					} else {
						scrollSpeed = 0;
					}
				});
				layer.addEventListener('drop', async (e) => {
					scrollSpeed = 0;
					if (scrollRaf) cancelAnimationFrame(scrollRaf);
					e.preventDefault();
					const id = e.dataTransfer.getData('text/plain');
					// If we dropped something that isn't an ID (files etc), ignore
					if (!id) return;
					
					const offset = parseFloat(e.dataTransfer.getData('offset')) || 0;
					
					const layerRect = layer.getBoundingClientRect();
					// Calculate Y position relative to the layer, compensating for where user grabbed card
					const relativeY = e.clientY - layerRect.top - offset;
					
					// visibleTotal = 1080 mins (18 hours)
					// dayHeight = total height of column
					const minutesFromStart = (relativeY / dayHeight) * 1080;
					
					// 06:00 start time
					const dayStartMinutes = 6 * 60; 
					let finalMinutes = dayStartMinutes + minutesFromStart;
					
					// Snap to 15 mins
					finalMinutes = Math.round(finalMinutes / 15) * 15;
					
					// Clamp to 06:00 - 24:00
					finalMinutes = Math.max(dayStartMinutes, Math.min(24*60 - 15, finalMinutes));
	
					const h = Math.floor(finalMinutes / 60);
					const m = finalMinutes % 60;
					
					const newDate = new Date(currentDay);
					newDate.setHours(h, m, 0, 0);

					// Optimistic UI Update: Move the dragged element immediately to the new spot
					// We need to find the card in the layer that matches the dragged ID
					const card = layer.querySelector(`.event-card[data-id="${id}"]`);
					if (card) {
						// Immediately position the card
						const pxPerMinute = dayHeight / 1080;
						const newTop = (finalMinutes - 6*60) * pxPerMinute;
						card.style.top = newTop + 'px';
						
						// Add animation class so it fades in at the new spot
						card.classList.remove('dragging');
						card.classList.add('dropping');
					}
					
					// For now, let's just do the DB update and let the re-render handle it.  
					// Since re-render is now non-destructive to the grid, it should be stable.
					
					try {
						await updateUserTask(id, { assigned: newDate.toISOString() });
						await loadAndRender();
						showToast('Task moved', 'success', {
							label: 'Undo',
							onClick: () => {
								if (typeof performUndo === 'function') performUndo();
							}
						});
					} catch (err) {
						console.error(err);
						showToast('Move failed', 'error');
						// Revert if needed (next loadAndRender will handle it)
					}
				});
			} else {
				// Clear only the event cards, keep the layer structure
				layer.innerHTML = '';
			}
			

			function updateCurrentTimeLine() {
				// Remove existing lines first
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

			// Initial draw
			updateCurrentTimeLine();
			// Clear any previous interval to avoid dupes
			if (window.currentTimeInterval) clearInterval(window.currentTimeInterval);
			window.currentTimeInterval = setInterval(updateCurrentTimeLine, 60000); // Update every minute

			// --- Double Click to Create ---
			if (!layer.dataset.dndInit) {
				layer.dataset.dndInit = 'true';
				layer.addEventListener('dblclick', (e) => {
					if (e.target.closest('.event-card')) return;
					e.preventDefault();
					const rect = layer.getBoundingClientRect();
					const relY = e.clientY - rect.top;
					const pxPerMin = dayHeight / 1080; // 18*60
					const minsFromStart = relY / pxPerMin;
					const totalMins = (6 * 60) + minsFromStart;
					
					// Snap to 15
					const snapped = Math.round(totalMins / 15) * 15;
					const h = Math.floor(snapped / 60);
					const m = snapped % 60;
					const timeStr = `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}`;
					
					// Open modal
					if (typeof openCreateModal === 'function') {
						openCreateModal(key, timeStr); // key is '2025-01-06'
					}
				});
			}
			
			if (calendarEl.classList.contains('refreshing')) calendarEl.classList.remove('refreshing');

			let items = byDate[key] || [];

			// --- Conflict Detection ---
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
					if(startB >= endA) break; // Sorted, so no more overlaps
					
					// Overlap found!
					taskA.isConflict = true;
					taskB.isConflict = true;
				}
			}
			
			// Handle empty state class
			if (!items.length) {
				eventsCol.classList.add('has-hint');
			} else {
				eventsCol.classList.remove('has-hint');
			}

			// Don't return early! otherwise we lose the time line and double-click handlers.
			// if (!items.length) { ... } 
			
			function resolveColor(value) {
				if (!value) return '#5f6c80';
				const v = String(value).trim();
				// direct CSS color formats
				if (v.startsWith('#') || /^rgb|^hsl/i.test(v)) return v;
				const key = v.toLowerCase();
				const map = {
					// site palette
					cadet: '#93A8AC', coral: '#EF6F6C', mint: '#59C9A5', celadon: '#ACDD91', violet: '#715D73', rose: '#9B6371',
					'dark-slate': '#465775', sandy: '#F7B074', maize: '#FFF07C', info: '#50908D',
					// common bright colors
					blue: '#3b82f6', green: '#22c55e', amber: '#f59e0b', purple: '#a855f7', red: '#ef4444', orange: '#f97316',
					yellow: '#eab308', teal: '#14b8a6', cyan: '#06b6d4', indigo: '#6366f1', pink: '#ec4899', gray: '#6b7280'
				};
				return map[key] || v; // fall back to any valid CSS color name
			}

			function textColorFor(bg) {
				// only handle hex here; otherwise default to white text
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
				// relative luminance (simple)
				const luminance = (0.299 * r + 0.587 * g + 0.114 * b);
				return luminance > 186 ? '#111111' : '#ffffff';
			}

			items.forEach(it => {
				const assigned = it.assigned ? new Date(it.assigned) : null;
				const startMinutes = assigned ? (assigned.getHours() * 60 + assigned.getMinutes()) : (9 * 60); // fallback 9:00
				const duration = (typeof it.estimateMinutes === 'number' && it.estimateMinutes > 0) ? it.estimateMinutes : 60;
				const visibleTotal = 18 * 60; // 1080 minutes
				const pxPerMinute = dayHeight / visibleTotal;
				// clamp into visible window [06:00, 24:00]
				const relStart = Math.max(0, Math.min(visibleTotal, startMinutes - (6 * 60)));
				const remaining = Math.max(0, visibleTotal - relStart);
				const clampedDuration = Math.min(duration, remaining);
				const top = Math.round(relStart * pxPerMinute);
				const height = Math.max(20, Math.round(clampedDuration * pxPerMinute));

				const card = document.createElement('div');
				card.className = 'event-card' + (it.isConflict ? ' conflict' : '');
				card.dataset.id = it.id; // Store ID for drag/drop lookup
				card.style.position = 'absolute';
				card.style.left = '0';
				card.style.right = '0';
				card.style.top = top + 'px';
				card.style.height = height + 'px';
				card.style.overflow = 'hidden';
				card.style.padding = '4px 8px';
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

				// add complete button (centered vertically in the event) and reserve right side
				card.style.paddingRight = '68px';
				const btn = document.createElement('button');
				btn.className = 'complete-btn';
				btn.type = 'button';
				btn.title = it.completed ? 'Completed' : 'Mark complete';
				btn.setAttribute('aria-label', btn.title);
				btn.innerHTML = '<svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true"><path fill="currentColor" d="M9 16.2 4.8 12l-1.4 1.4L9 19 21 7l-1.4-1.4z"/></svg>';
				if (it.completed) btn.disabled = true;

				// Reset styles to avoid generic button overrides and ensure compact size
				btn.style.background = 'rgba(255,255,255,0.3)';
				btn.style.border = '1px solid rgba(255,255,255,0.4)';
				btn.style.borderRadius = '6px';
				btn.style.padding = '0';
				btn.style.margin = '0';
				btn.style.minWidth = 'auto';
				btn.style.width = '24px';
				btn.style.height = '24px';
				btn.style.display = 'flex';
				btn.style.alignItems = 'center';
				btn.style.justifyContent = 'center';
				btn.style.cursor = 'pointer';
				btn.style.transition = 'background 0.2s ease';

				btn.addEventListener('mouseenter', () => { btn.style.background = 'rgba(255,255,255,0.5)'; });
				btn.addEventListener('mouseleave', () => { btn.style.background = 'rgba(255,255,255,0.3)'; });

				btn.style.position = 'absolute';
				btn.style.top = '50%';
				btn.style.right = '8px';
				btn.style.transform = 'translateY(-50%)';
				card.appendChild(btn);

				// duration label to the left of the button
				const dur = document.createElement('div');
				dur.className = 'event-duration';
				dur.textContent = `${duration}m`;
				dur.style.position = 'absolute';
				dur.style.top = '50%';
				dur.style.right = '42px';
				dur.style.transform = 'translateY(-50%)';
				dur.style.fontSize = '.75rem';
				dur.style.opacity = '0.9';
				card.appendChild(dur);

				// Blocked time special behavior: no complete, open schedule modal on click
				if (isBlockedEvent) {
					btn.style.display = 'none';
					dur.style.display = 'none';
					card.style.cursor = 'pointer';
					card.addEventListener('click', (e) => {
						e.preventDefault();
						e.stopPropagation();
						if (typeof openScheduleModal === 'function' && defineScheduleModal) {
							openScheduleModal();
						}
					});
				} else {
					// --- DRAG SUPPORT ---
					if (!it.completed) {
						card.draggable = true;
						card.addEventListener('dragstart', (e) => {
							e.dataTransfer.setData('text/plain', it.id);
							const rect = card.getBoundingClientRect();
							const offsetY = e.clientY - rect.top;
							e.dataTransfer.setData('offset', offsetY);
							e.dataTransfer.effectAllowed = 'move';
							
							// Capture state for undo
							if (typeof pushUndoAction === 'function') {
								pushUndoAction({ 
									type: 'update', 
									id: it.id, 
									oldPayload: { 
										assigned: it.assigned, 
										estimated_time: duration 
									} 
								});
							}
							
							// Fix for Windows drag gradient: create a manual drag image
							const dragImage = card.cloneNode(true);
							dragImage.style.position = 'absolute';
							dragImage.style.top = '-9999px';
							dragImage.style.left = '-9999px';
							dragImage.style.width = rect.width + 'px';
							dragImage.style.height = rect.height + 'px';
							dragImage.style.margin = '0';
							dragImage.style.opacity = '0.8';
							dragImage.style.transform = 'none';
							document.body.appendChild(dragImage);
							e.dataTransfer.setDragImage(dragImage, e.clientX - rect.left, offsetY);
							setTimeout(() => document.body.removeChild(dragImage), 0);
							
							// Delay adding class for the source element
							setTimeout(() => card.classList.add('dragging'), 0);
						});
						card.addEventListener('dragend', () => card.classList.remove('dragging'));
					}

					// --- RESIZE SUPPORT ---
					if (!it.completed) {
						const handle = document.createElement('div');
						handle.className = 'resize-handle';
						card.appendChild(handle);
						handle.addEventListener('mousedown', (e) => {
							e.stopPropagation(); e.preventDefault();
							
							// Capture state for undo
							if (typeof pushUndoAction === 'function') {
								pushUndoAction({ 
									type: 'update', 
									id: it.id, 
									oldPayload: { 
										assigned: it.assigned, 
										estimated_time: duration 
									} 
								});
							}

							const startPageY = e.pageY;
							const startH = height;
							
							let scrollSpeed = 0;
							let rafId = null;

							function scrollTick() {
								if (scrollSpeed !== 0) {
									window.scrollBy(0, scrollSpeed);
									rafId = requestAnimationFrame(scrollTick);
								} else {
									rafId = null;
								}
							}

							function onMove(evt) {
								// Smooth scroll triggering
								if (evt.clientY < 60) {
									scrollSpeed = -15;
									if (!rafId) scrollTick();
								} else if (window.innerHeight - evt.clientY < 60) {
									scrollSpeed = 15;
									if (!rafId) scrollTick();
								} else {
									scrollSpeed = 0;
								}

								const diff = evt.pageY - startPageY;
								const newH = Math.max(20, startH + diff);
								card.style.height = newH + 'px';
								
								// Live update duration text
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

					// Non-blocked: clicking the card opens the general edit modal
					// (check if we were dragging/resizing - but click usually fires after mouseup)
					card.style.cursor = 'pointer';
					card.addEventListener('click', (e) => {
						// Don't open if we just clicked the complete btn
						if (e.target.closest('.complete-btn') || e.target.closest('.resize-handle')) return;
						e.preventDefault();
						e.stopPropagation();
						if (window && typeof window.openGeneralEditModal === 'function') {
							window.openGeneralEditModal(it);
						}
					});
				}

				btn.addEventListener('click', async (e) => {
					e && e.stopPropagation && e.stopPropagation();
					try {
						card.classList.add('pop');
						btn.classList.add('spin');
						btn.disabled = true;
						// optimistic UI
						const gray = resolveColor('gray');
						card.style.background = gray;
						card.style.color = textColorFor(gray);
						fireConfetti(); // Celebrate!
						await updateUserTask(it.id, { complete: true, color: 'gray' });
						await loadAndRender();
					} catch (e) {
						console.error('Failed to complete task', e);
						btn.disabled = false;
					}
				});
				layer.appendChild(card);
			});
		}

		// --- UI wiring -----------------------------------------------------
		function initPasswordToggles() {
			document.querySelectorAll('.toggle-btn[data-toggle], #togglePassword').forEach(btn => {
				btn.addEventListener('click', () => {
					const targetId = btn.dataset.toggle || 'password';
					const pwd = document.getElementById(targetId); if (!pwd) return;
					const isPassword = pwd.type === 'password'; pwd.type = isPassword ? 'text' : 'password';
					btn.setAttribute('aria-pressed', String(isPassword)); btn.textContent = isPassword ? 'Hide' : 'Show';
				});
			});
		}

		function initAuthHandlers() {
			const loginForm = document.getElementById('loginForm');
			// If we're on the login page and already authenticated, skip to /dashboard/
			if (loginForm) {
				(async () => {
					try {
						const client = await ensureAppwriteClient(); const App = AppwriteModule; const account = new App.Account(client);
						const u = await account.get();
						if (u && (u.$id || u.id)) { window.location.href = '/dashboard/'; return; }
					} catch (_) {}
				})();
				// Prefill from localStorage remember
				try {
					const savedRemember = localStorage.getItem('pt_remember') === '1';
					const savedEmail = localStorage.getItem('pt_email') || '';
					const rememberEl = document.getElementById('remember');
					if (rememberEl) rememberEl.checked = savedRemember;
					if (savedEmail) { const emailEl = document.getElementById('email'); if (emailEl) emailEl.value = savedEmail; }
				} catch (_) {}
			}
			if (loginForm) loginForm.addEventListener('submit', async (e) => {
				e.preventDefault(); showFormError(loginForm,'');
				const email = (document.getElementById('email')||{}).value || ''; const password = (document.getElementById('password')||{}).value || '';
				if (!email || !password) return showFormError(loginForm,'Please enter email and password.');
				try {
					const client = await ensureAppwriteClient(); const App = AppwriteModule; const account = new App.Account(client);
					let handled = false;
					try {
						const modern = await invokeWithCompat(account, 'createEmailPasswordSession', [email, password], { email, password });
						if (modern.called) handled = true;
						if (!handled) {
							const legacy = await invokeWithCompat(account, 'createEmailSession', [email, password], { email, password });
							handled = legacy.called;
						}
						if (!handled && typeof account.createSession === 'function' && account.createSession.length > 1) {
							await account.createSession(email, password);
							handled = true;
						}
						if (!handled) throw new Error('Appwrite session method missing');
					} catch (sessionErr) {
						// Only swallow "session active" errors
						if (sessionErr.message && (sessionErr.message.includes('session is active') || sessionErr.message.includes('should be guest'))) {
							// Verify we actually have a session
							const u = await account.get();
							if (u && (u.$id || u.id)) { 
								console.log('Recovered existing session');
								handled = true; 
							} else {
								throw sessionErr;
							}
						} else {
							throw sessionErr;
						}
					}
					
					// Get user ID and ensure their collection exists
					cachedUserId = null; // Clear cache to force fresh fetch
					const userId = await getCurrentUserId();
					if (userId) {
						try {
							await ensureUserCollection(userId);
						} catch (collErr) {
							console.warn('Could not ensure user collection:', collErr);
						}
					}
					
					// Remember preference and email for convenience on this device
					try {
						const rememberEl = document.getElementById('remember');
						const remember = !!(rememberEl && rememberEl.checked);
						if (remember) {
							localStorage.setItem('pt_remember','1');
							localStorage.setItem('pt_email', email);
						} else {
							localStorage.removeItem('pt_remember');
							localStorage.removeItem('pt_email');
						}
					} catch (_) {}
					window.location.href = '/dashboard/';
				} catch (err) { console.error('Login error',err); showFormError(loginForm, err.message || 'Sign in failed'); }
			});

			const signupForm = document.getElementById('signupForm');
			if (!signupForm) return;
			const pwd = document.getElementById('password'); const confirm = document.getElementById('confirmPassword'); const submitBtn = document.getElementById('submitBtn'); const pwMismatch = document.getElementById('pwMismatch');
			function validate(){ const match = pwd && confirm && pwd.value && confirm.value && pwd.value === confirm.value; const terms = !!(document.getElementById('terms')||{}).checked; if (pwMismatch) pwMismatch.style.display = match ? 'none' : 'block'; if (submitBtn) submitBtn.disabled = !(match && terms); }
			[pwd,confirm].forEach(el => el && el.addEventListener('input', validate)); const terms = document.getElementById('terms'); if (terms) terms.addEventListener('change', validate);
			signupForm.addEventListener('submit', async (e) => {
				e.preventDefault(); showFormError(signupForm,'');
				const fullName = (document.getElementById('fullName')||{}).value||''; const email = (document.getElementById('email')||{}).value||''; const passwordVal = (document.getElementById('password')||{}).value||''; const confirmVal = (document.getElementById('confirmPassword')||{}).value||''; const termsChecked = !!(document.getElementById('terms')||{}).checked;
				if (!email || !passwordVal || !confirmVal) return showFormError(signupForm,'Please complete all fields.'); if (passwordVal !== confirmVal) return showFormError(signupForm,'Passwords do not match.'); if (!termsChecked) return showFormError(signupForm,'You must accept the terms.');
				try {
					const client = await ensureAppwriteClient(); const App = AppwriteModule; const account = new App.Account(client);
					const desiredId = App.ID && typeof App.ID.unique === 'function' ? App.ID.unique() : 'unique()';
					let created = await invokeWithCompat(account, 'create', [desiredId, email, passwordVal, fullName], {
						userId: desiredId,
						email,
						password: passwordVal,
						name: fullName
					});
					if (!created.called) {
						created = await invokeWithCompat(account, 'createAccount', [email, passwordVal, fullName], {
							email,
							password: passwordVal,
							name: fullName
						});
					}
					if (!created.called) throw new Error('Account create not available');
					const session = await invokeWithCompat(account, 'createEmailPasswordSession', [email, passwordVal], { email, password: passwordVal });
					if (!session.called) {
						const legacy = await invokeWithCompat(account, 'createEmailSession', [email, passwordVal], { email, password: passwordVal });
						if (!legacy.called && typeof account.createSession === 'function' && account.createSession.length > 1) {
							await account.createSession(email, passwordVal);
						}
					}
					
					// Get the newly created user ID and ensure their collection exists
					cachedUserId = null; // Clear cache to force fresh fetch
					const userId = await getCurrentUserId();
					if (userId) {
						try {
							await ensureUserCollection(userId);
						} catch (collErr) {
							console.warn('Could not ensure user collection:', collErr);
						}
					}
					
					window.location.href = '/dashboard/';
				} catch (err) { console.error('Signup error',err); showFormError(signupForm, err.message || 'Sign up failed'); }
			});
		}

		// Helper to open create modal
		function openCreateModal(dateStr, timeStr) {
			const modal = document.getElementById('taskModal');
			if (!modal) return;
			const dateEl = document.getElementById('taskDueDate');
			const timeEl = document.getElementById('taskAssignedTime');
			const title = document.getElementById('taskTitle');
			const smartInput = document.getElementById('smartInput');
			
			// Reset
			document.getElementById('taskCreateForm').reset();
			if (dateEl) dateEl.value = dateStr || formatDateISO(currentDay);
			if (timeEl) timeEl.value = timeStr || '';
			
			modal.style.display = 'flex';
			// Focus smart input if available, else title
			if (smartInput) {
				smartInput.focus();
			} else if (title) {
				title.focus();
			}
		}

		function parseSmartInput(text) {
			const result = { title: text, date: null, time: null, duration: null };
			if (!text) return result;

			const lower = text.toLowerCase();
			const now = new Date();

			// --- Duration parsing ---
			// Explicit: "for 30m", "for 1h", "20 mins"
			const durRegex = /\b(?:for\s+)?(\d+(?:\.\d+)?)\s*(m|min|mins|minutes|h|hr|hours)\b/i;
			const durMatch = text.match(durRegex);
			if (durMatch) {
				const val = parseFloat(durMatch[1]);
				const unit = durMatch[2].toLowerCase().startsWith('h') ? 60 : 1;
				result.duration = Math.round(val * unit);
				result.title = result.title.replace(durMatch[0], '');
			} else {
				// Implicit heuristics
				if (/\b(quick|chat|check|email|standup)\b/i.test(text)) result.duration = 15;
				else if (/\b(call|meeting|sync|discussion)\b/i.test(text)) result.duration = 30;
				else if (/\b(review|draft|analysis)\b/i.test(text)) result.duration = 45;
				else if (/\b(deep|focus|write|code|coding|plan|lab)\b/i.test(text)) result.duration = 60;
			}

			// 1. Time parsing (e.g. 5pm, 5:30pm, 17:00, 5 am)
			// Matches: 5:30pm, 5pm, 5 pm, 5:30, 17:00
			const timeRegex = /(\d{1,2})(?::(\d{2}))?\s*(am|pm|a\.m\.|p\.m\.)?/i;
			const timeMatch = text.match(timeRegex);
			
			if (timeMatch) {
				let [match, h, m, meridiem] = timeMatch;
				let hour = parseInt(h, 10);
				let minute = m ? parseInt(m, 10) : 0;
				
				if (meridiem) {
					meridiem = meridiem.toLowerCase().replace(/\./g, '');
					if (meridiem === 'pm' && hour < 12) hour += 12;
					if (meridiem === 'am' && hour === 12) hour = 0;
				} else {
					// Guessing: if user types "5", likely 5pm unless it's "9" or "10" or "11" (could be am)
					// Logic: < 7 -> pm, >= 7 -> am (business hours bias)
					// Exception: if typing "13:00" -> explicit 24h
					if (!m && hour < 7) hour += 12; 
				}

				if (hour >= 0 && hour < 24 && minute >= 0 && minute < 60) {
					result.time = `${String(hour).padStart(2,'0')}:${String(minute).padStart(2,'0')}`;
					// Remove the time string from title
					result.title = result.title.replace(match, '').replace(/\s+/g, ' ').trim();
				}
			}

			// 2. Date parsing (tomorrow, today, next monday, friday)
			let addedDays = 0;
			if (lower.includes('tomorrow') || lower.includes('tmrw')) {
				addedDays = 1;
				result.title = result.title.replace(/tomorrow|tmrw/i, '');
			} else if (lower.includes('today')) {
				addedDays = 0;
				result.title = result.title.replace(/today/i, '');
			} else {
				// Day names
				const days = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
				const shortDays = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];
				
				// Find first mention
				for (let i=0; i<7; i++) {
					if (lower.includes(days[i]) || lower.match(new RegExp(`\\b${shortDays[i]}\\b`))) {
						const currentDayIdx = now.getDay();
						let diff = i - currentDayIdx;
						if (diff <= 0) diff += 7; // next instance
						addedDays = diff;
						result.title = result.title.replace(new RegExp(`(${days[i]}|\\b${shortDays[i]}\\b)`, 'i'), '');
						break;
					}
				}
			}

			if (addedDays > 0) {
				const targetDate = new Date();
				targetDate.setDate(now.getDate() + addedDays);
				result.date = formatDateISO(targetDate);
			} else {
				// Default to currently viewed day logic handling by caller, or today
			}

			// Cleanup title
			result.title = result.title.replace(/\s+/g, ' ').replace(/\bat\b/gi, '').replace(/\bon\b/gi, '').replace(/\bfor\b/gi, '').trim();
			return result;
		}

		function initModalAndForm() {
			const createBtn = document.getElementById('createTaskBtn'); const modal = document.getElementById('taskModal'); const cancelBtn = document.getElementById('cancelTask'); const taskForm = document.getElementById('taskCreateForm');
			if (createBtn && modal) {
				createBtn.addEventListener('click', () => { 
					openCreateModal(formatDateISO(currentDay));
				});
			}
			if (cancelBtn && modal) cancelBtn.addEventListener('click', () => { modal.style.display = 'none'; });
			
			// Smart Input Handler
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
				// Allow pressing Enter to just trigger the parsing without submitting yet (or submit if valid)
				smartInput.addEventListener('keydown', (e) => {
					if (e.key === 'Enter') {
						e.preventDefault();
						smartInput.blur(); // trigger change
						// Optional: if title is present, we could auto-submit
						// but better to let user verify
						document.getElementById('taskTitle').focus();
					}
				});
			}

			if (!taskForm) return;
			taskForm.addEventListener('submit', async (e) => {
				e.preventDefault(); 
				const name = document.getElementById('taskTitle').value.trim(); 
				const dueDate = document.getElementById('taskDueDate').value; 
				const estimateStr = (document.getElementById('taskEstimateMinutes')||{}).value || '';
				const estimateMinutes = estimateStr ? Math.max(1, parseInt(estimateStr, 10)) : 60; // default to 60 if not provided
				const priorityVal = (document.getElementById('taskPriority')||{}).value || 'medium';
				const assignedTime = (document.getElementById('taskAssignedTime')||{}).value;
				
				if (!name || !dueDate) return showToast('Please enter task name and due date', 'error');
				
				let assignedIso = null;
				let dueIso = new Date(dueDate + 'T23:59:59.000Z').toISOString();
				if (assignedTime) {
					assignedIso = new Date(`${dueDate}T${assignedTime}:00.000Z`).toISOString();
				}
				
				// Send null category - will be categorized later by background process
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
					modal.style.display='none'; 
					taskForm.reset(); 
					await loadAndRender(); 
					showToast('Task created', 'success');
					// Trigger categorization of uncategorized tasks
					await categorizeTasks();
					// Trigger auto-scheduler to fit new task if possible
					await autoSchedule();
				} catch (err) { 
					console.error('create task error',err); 
					showToast(err.message || 'Could not create task', 'error'); 
				}
			});
		}

		function initNavigation() {
			const prevDay = document.getElementById('prevDay'); const nextDay = document.getElementById('nextDay');
			if (prevDay) prevDay.addEventListener('click', () => { currentDay.setDate(currentDay.getDate()-1); loadAndRender(); });
			if (nextDay) nextDay.addEventListener('click', () => { currentDay.setDate(currentDay.getDate()+1); loadAndRender(); });
		}

		function initFabScrollBehavior() {
			const fab = document.getElementById('createTaskBtn-wrapper');
			const footer = document.querySelector('footer');
			if (!fab || !footer) return;

			// standard FAB layout
			const FAB_BOTTOM_MARGIN = 28;
			const FAB_HEIGHT = 56;

			function updatePosition() {
				const footerRect = footer.getBoundingClientRect();
				const winHeight = window.innerHeight;
				
				// Standard FAB zone in viewport Y: [winHeight - 84, winHeight - 28]
				const fabTopY = winHeight - FAB_BOTTOM_MARGIN - FAB_HEIGHT;
				const fabBottomY = winHeight - FAB_BOTTOM_MARGIN;

				// Check for overlap:
				// Footer overlaps if its top is above FAB bottom AND its bottom is below FAB top
				const isOverlap = (footerRect.top < fabBottomY) && (footerRect.bottom > fabTopY);

				if (isOverlap) {
					// Push FAB up so it sits 10px above the footer
					// desired FAB bottom Y = footerRect.top - 10
					// bottom style = winHeight - desired FAB bottom Y
					//              = winHeight - (footerRect.top - 10)
					//              = winHeight - footerRect.top + 10
					const newBottom = winHeight - footerRect.top + 10;
					fab.style.bottom = newBottom + 'px';
				} else {
					fab.style.bottom = FAB_BOTTOM_MARGIN + 'px';
				}
			}

			window.addEventListener('scroll', updatePosition, { passive: true });
			window.addEventListener('resize', updatePosition);
			// Also watch for content changes (like calendar loading)
			if (window.ResizeObserver) {
				const ro = new ResizeObserver(updatePosition);
				ro.observe(document.body);
			}
			// Check initially
			updatePosition();
		}

		async function checkDueTasks() {
			if (!('Notification' in window) || Notification.permission !== 'granted') return;

			try {
				const tasks = await listUserTasks();
				const now = new Date();
				const fifteenMinsLater = new Date(now.getTime() + 15 * 60000);

				tasks.forEach(task => {
					if (task.complete || !task.due) return;
					
					const dueDate = new Date(task.due);
					// Check if due in the future (now < dueDate) AND within 15 mins (dueDate <= fifteenMinsLater)
					if (dueDate > now && dueDate <= fifteenMinsLater) {
						const notifiedKey = `notified_task_${task.$id}`;
						if (!sessionStorage.getItem(notifiedKey)) {
							new Notification(`Task Due Soon: ${task.name}`, {
								body: `This task is due at ${dueDate.toLocaleTimeString()}`,
							});
							sessionStorage.setItem(notifiedKey, 'true');
						}
					}
				});
			} catch (err) {
				console.error('Error checking due tasks:', err);
			}
		}

		// --- local notifications -------------------------------------------
		// Duplicate checkDueTasks removed.


		// --- Focus Timer ---
		let focusInterval = null;
		let focusTime = 25 * 60;
		let isFocusing = false;
		let focusTarget = null;
		const FOCUS_KEY = 'pt_focus_state';

		function loadFocusState() {
			try {
				const saved = sessionStorage.getItem(FOCUS_KEY);
				if (saved) {
					const data = JSON.parse(saved);
					if (data.isFocusing && data.target) {
						const now = Date.now();
						const remaining = Math.ceil((data.target - now) / 1000);
						if (remaining > 0) {
							focusTime = remaining;
							focusTarget = data.target;
							isFocusing = true;
							startFocusInterval();
						} else {
							focusTime = 25*60; 
							isFocusing = false;
						}
					} else if (data.timeLeft) {
						focusTime = data.timeLeft;
					}
				}
			} catch(e) { console.error(e); }
			updateFocusDisplay();
		}

		function saveFocusState() {
			const state = { isFocusing, timeLeft: focusTime, target: focusTarget };
			sessionStorage.setItem(FOCUS_KEY, JSON.stringify(state));
		}

		function updateFocusDisplay() {
			const display = document.getElementById('focusDisplay');
			if (!display) return;
			const m = Math.max(0, Math.floor(focusTime / 60));
			const s = Math.max(0, focusTime % 60);
			display.textContent = `${m}:${s < 10 ? '0' : ''}${s}`;
			const btn = document.getElementById('focusToggleBtn');
			if (btn) {
				btn.textContent = isFocusing ? 'Pause' : 'Start Focus';
				btn.className = isFocusing ? 'btn btn-outline' : 'btn primary';
			}
			if(isFocusing) document.title = `${m}:${s < 10 ? '0' : ''}${s} - Focus`;
			else document.title = 'PolyTask';
		}

		function startFocusInterval() {
			if (focusInterval) clearInterval(focusInterval);
			focusInterval = setInterval(() => {
				if (focusTarget) {
					const now = Date.now();
					// Sync time
					const diff = Math.ceil((focusTarget - now) / 1000);
					focusTime = diff;
				} else {
					focusTime--;
				}
				
				if (focusTime <= 0) {
					clearInterval(focusInterval);
					isFocusing = false;
					focusTime = 25 * 60;
					focusTarget = null;
					saveFocusState();
					updateFocusDisplay();
					
					if (Notification.permission === 'granted') new Notification('Focus Session Complete!');
					else alert('Focus Session Complete!');
					
					if (window.PolyTask && window.PolyTask.fireConfetti) window.PolyTask.fireConfetti();
				} else {
					updateFocusDisplay();
				}
			}, 1000);
		}

		function toggleFocus() {
			if (isFocusing) {
				clearInterval(focusInterval);
				isFocusing = false;
				focusTarget = null;
				saveFocusState();
				updateFocusDisplay();
			} else {
				isFocusing = true;
				if (focusTime <= 0) focusTime = 25*60;
				focusTarget = Date.now() + (focusTime * 1000);
				saveFocusState();
				updateFocusDisplay();
				startFocusInterval();
			}
		}

		function resetFocus() {
			clearInterval(focusInterval);
			isFocusing = false;
			focusTime = 25 * 60;
			focusTarget = null;
			sessionStorage.removeItem(FOCUS_KEY);
			updateFocusDisplay();
		}

		// --- public init ----------------------------------------------------
		async function init() {
			// 1. Request Notification permission
			if ('Notification' in window && Notification.permission === 'default') {
				Notification.requestPermission();
			}

			// 2. Start checking due tasks
			setInterval(checkDueTasks, 60000); // Check every minute
			checkDueTasks(); // Check immediately

			initPasswordToggles();
			loadFocusState();
			initAuthHandlers();
			initModalAndForm();
			initNavigation();
			initFabScrollBehavior();

			function initGlobalKeys() {
				function handleGlobalKeys(e) {
					// Escape to close modals
					if (e.key === 'Escape') {
						if (editEventModal && editEventModal.style.display !== 'none') { closeEditModal(); return; }
						const tm = document.getElementById('taskModal');
						if (tm && tm.style.display !== 'none') { tm.style.display='none'; return; }
						if (defineScheduleModal && defineScheduleModal.style.display !== 'none') { defineScheduleModal.style.display='none'; return; }
						const hm = document.getElementById('helpModal');
						if (hm && hm.style.display !== 'none') { hm.remove(); return; }
					}

					// Ignore inputs for other shortcuts
					if ( e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.isContentEditable ) return;

					// '?' for Shortcuts Help
					if (e.key === '?') {
						let hm = document.getElementById('helpModal');
						if (hm) { hm.remove(); return; }
						else {
							hm = document.createElement('div');
							hm.id = 'helpModal';
							hm.className = 'modal';
							hm.style.display = 'flex';
							hm.innerHTML = `
								<div class="modal-content" style="max-width:400px">
									<div class="modal-header"><h3>Keyboard Shortcuts</h3><span class="close" onclick="this.closest('.modal').remove()">&times;</span></div>
									<div class="modal-body" style="line-height:1.8">
										<div><kbd>N</kbd> New Task</div>
										<div><kbd>?</kbd> Show this help</div>
										<div><kbd>Ctrl</kbd>+<kbd>Z</kbd> Undo</div>
										<div><kbd>Esc</kbd> Close Modals</div>
										<div><kbd>Del</kbd> Delete hovered task</div>
									</div>
								</div>
							`;
							document.body.appendChild(hm);
						}
					}

					// 'N' for New Task
					if (e.key.toLowerCase() === 'n') {
						e.preventDefault();
						// check if openCreateModal exists (it's internal to this scope or exposed?)
						// It is internal: function openCreateModal(dateStr, timeStr) ...
						// But handleGlobalKeys is defined inside initGlobalKeys which is inside init which is inside the IIFE.
						// openCreateModal is defined at line 1800+ inside IIFE scope.
						// So we can call it if it is in scope.
						// Let's check scope.
						if (typeof openCreateModal === 'function') openCreateModal(formatDateISO(currentDay));
					}
					
					// Ctrl+Z to Undo
					if ((e.ctrlKey || e.metaKey) && e.key === 'z') {
						if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
						e.preventDefault();
						if (typeof performUndo === 'function') performUndo();
					}

					// Delete/Backspace
					if (e.key === 'Delete' || e.key === 'Backspace') {
						if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
						// Find hovered event card
						const hoveredInfo = document.querySelector('.event-card:hover');
						if (hoveredInfo && hoveredInfo.dataset.id) {
							e.preventDefault();
							if (confirm('Delete this task?')) {
								pushUndoAction({
									type: 'delete',
									id: hoveredInfo.dataset.id,
									// For strict undo, we need the payload, but we don't have it easily here without fetching.
									// Simplified delete for now (non-undoable or basic undo).
									// Logic: we can try to fetch the item state from the DOM or just fetch it first.
									// Let's rely on simple delete for now, or just warn.
									// Actually, let's fetch it first to enable Undo!
								});
								// Fetch first for undo context?
								// Since this is sync-ish UI, we can just grab from DOM dataset if we stored everything.
								// But we only stored basic visuals.
								// Let's just delete for now.
								deleteUserTask(hoveredInfo.dataset.id).then(() => {
									loadAndRender();
									showToast('Task deleted', 'success');
								});
							}
						}
					}
				}
				document.addEventListener('keydown', handleGlobalKeys);
			}
			initGlobalKeys();

			// --- Dark Mode Logic ---
			const storedTheme = localStorage.getItem('theme');
			if (storedTheme === 'dark') {
				document.body.classList.add('dark');
			}
			window.toggleDarkMode = function() {
				document.body.classList.toggle('dark');
				const isDark = document.body.classList.contains('dark');
				localStorage.setItem('theme', isDark ? 'dark' : 'light');
				
				// Sync all toggles
				document.querySelectorAll('.dm-toggle, #dmToggle, #darkModeToggle').forEach(el => {
					if(el.type === 'checkbox') el.checked = isDark;
				});
			};
			
			// Initial Sync
			document.querySelectorAll('.dm-toggle, #dmToggle, #darkModeToggle').forEach(el => {
				if(el.type === 'checkbox') {
					el.checked = (storedTheme === 'dark');
					el.addEventListener('change', window.toggleDarkMode);
				}
			});

			const userId = await getCurrentUserId();
			if (!userId) {
					// Not logged in: send to login page
					if (!window.location.pathname.includes('/login') && !window.location.pathname.endsWith('index.html') && window.location.pathname !== '/') {
						console.info('User not authenticated — redirecting to /login');
						window.location.href = '/login'; // Or root
					}
					return;
			}
			
			// If we are on dashboard or calendar, render is handled by page specific script usually
			// checking for #calendar element is done in loadAndRender
			await loadAndRender();
			}

		// expose a couple helpers for tests
		return { init, checkDueTasks, createUserTask, listUserTasks, getCurrentUserId, deleteUserTask, updateUserTask, autoSchedule, loadAndRender, parseSmartInput, toggleFocus, resetFocus, fireConfetti, logout: async () => {
			const client = await ensureAppwriteClient();
			const account = new AppwriteModule.Account(client);
			await account.deleteSession('current');
			window.location.href = '../index.html';
		}};
	})();

		// expose globally for modal helpers and tests
		try { window.PolyTask = PolyTask; } catch (e) {}
		// start
		PolyTask.init().catch(err => console.error('Failed to initialize PolyTask', err));

	});


