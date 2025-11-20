// Import Transformers.js for semantic categorization
import { pipeline } from 'https://cdn.jsdelivr.net/npm/@xenova/transformers@2.6.0';

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
					if (errors.length) { alert(errors[0]); return; }
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
							if (!toCreate.length && !toUpdate.length) { alert('Add at least one time range'); return; }
							try {
									// Update existing rows
									for (const { id, payload } of toUpdate) {
										await PolyTask.updateUserTask(id, payload);
									}
									// Create new rows
									for (const payload of toCreate) {
										await PolyTask.createUserTask(payload);
									}
									// Delete removed rows (docs we loaded but didn't keep)
									const existing = Array.isArray(scheduleExistingDocs) ? scheduleExistingDocs : [];
									for (const d of existing) {
										if (d && d.$id && !seenDocIds.has(d.$id)) {
											await PolyTask.deleteUserTask(d.$id);
										}
									}
								defineScheduleModal.style.display = 'none';
								// refresh to show new blocked events
								window.location.reload();
					} catch (err) {
						console.error('Failed to create blocked events', err);
						alert(err.message || 'Failed to create blocked events');
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
						if (!editDocId || !editDocId.value) { alert('Missing event id.'); return; }
						const title = (editTitle && editTitle.value.trim()) || '';
						const dateStr = (editDate && editDate.value) || '';
						const timeStr = (editStart && editStart.value) || '';
						const duration = Math.max(1, parseInt((editDuration && editDuration.value) || '60', 10));
						if (!title || !dateStr || !/^[0-9]{2}:[0-9]{2}$/.test(timeStr)) { alert('Please complete required fields.'); return; }
						const [hh, mm] = timeStr.split(':').map(n => parseInt(n,10));
						const assigned = new Date(dateStr + 'T00:00:00'); assigned.setHours(hh, mm, 0, 0);
						const payload = {
							name: title,
							assigned: assigned.toISOString(),
							due: new Date(dateStr + 'T23:59:59').toISOString(),
							estimated_time: duration,
							category: (editCategory && editCategory.value.trim()) || null,
							color: (editColor && editColor.value) || '#3b82f6',
							repeat: !!(editRepeatWeekly && editRepeatWeekly.checked),
							complete: !!(editCompleted && editCompleted.checked)
						};
						try {
							await PolyTask.updateUserTask(editDocId.value, payload);
							closeEditModal();
							window.location.reload();
						} catch (err) {
							console.error('Update event failed', err);
							alert(err.message || 'Failed to update event');
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
		function showFormError(form, msg) {
			if (!form) return;
			const el = form.querySelector('#formError') || form.querySelector('#pwMismatch');
			if (el) {
				el.style.display = msg ? 'block' : 'none';
				el.textContent = msg || '';
			} else if (msg) alert(msg);
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
					return existing.value;
				}
				// Try legacy signature
				const col = await db.getCollection(APPWRITE_DATABASE, userId);
				if (col) {
					console.log(`Collection ${userId} already exists`);
					collectionExists = true;
					try { await invokeWithCompat(db, 'createBooleanAttribute', [APPWRITE_DATABASE, userId, 'completed', false, false], { databaseId: APPWRITE_DATABASE, collectionId: userId, key: 'completed', required: false, default: false }); } catch (e) {}
					try { await invokeWithCompat(db, 'createStringAttribute', [APPWRITE_DATABASE, userId, 'repeat', 20, false], { databaseId: APPWRITE_DATABASE, collectionId: userId, key: 'repeat', size: 20, required: false }); } catch (e) {}
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
				repeat: data.repeat || null
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
				
				// Filter tasks that need categorization (null category)
				// Skip "Blocked" tasks - they should never be auto-categorized
				const uncategorized = allDocs.filter(d => {
					const isBlocked = String(d.name || '').toLowerCase().includes('blocked') || 
					                  String(d.category || '').toLowerCase() === 'blocked';
					const needsCategorization = !d.category || d.category === null;
					return needsCategorization && !isBlocked;
				});
				if (uncategorized.length === 0) return;
				
				// Build knowledge base from categorized tasks (exclude Blocked)
				const categorized = allDocs.filter(d => d.category && d.category !== 'Blocked');
				
				// Initialize ML model
				const extractor = await pipeline('feature-extraction', 'Xenova/all-MiniLM-L6-v2');
				
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
					if (Array.isArray(out)) return mean(out[0]);
					return out.data ? out.data : out;
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
				
				// Categorize each uncategorized task
				for (const task of uncategorized) {
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
					
					await invokeWithCompat(db, 'updateDocument', [APPWRITE_DATABASE, userId, task.$id, updatePayload], {
						databaseId: APPWRITE_DATABASE,
						collectionId: userId,
						documentId: task.$id,
						data: updatePayload
					});
				}
				
				console.log(`Categorized ${uncategorized.length} tasks`);
			} catch (err) {
				console.error('Error categorizing tasks:', err);
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
			calendarEl.innerHTML = '<p>Loading tasks...</p>';
			
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
					repeat: d.repeat || null
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
				const header = document.createElement('div'); header.style.display = 'flex'; header.style.justifyContent = 'space-between'; header.style.alignItems = 'center';
				header.innerHTML = `<strong>${friendlyDayLabel(currentDay)}</strong><small>${key}</small>`;
				wrapper.appendChild(header);
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
				// Update header with new date
				const header = wrapper.querySelector('div[style*="display"]');
				if (header) {
					header.innerHTML = `<strong>${friendlyDayLabel(currentDay)}</strong><small>${key}</small>`;
				}
			}

			const firstSlot = timeCol.querySelector('.time-slot');
			const slotHeight = firstSlot ? firstSlot.clientHeight : 20; // height per 15-minute slot
			const dayHeight = slotHeight * 72; // 72 quarters in our visible range (06:00-24:00)
			// events layer
			const layer = document.createElement('div');
			layer.style.position = 'relative';
			layer.style.height = dayHeight + 'px';
			layer.style.marginTop = '12px';
			eventsCol.innerHTML = '';
			eventsCol.appendChild(layer);

			const items = byDate[key] || [];
			if (!items.length) {
				const empty = document.createElement('div'); empty.className = 'empty'; empty.textContent = 'No tasks for this day'; empty.style.marginTop = '12px';
				eventsCol.appendChild(empty);
				return;
			}

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
				card.className = 'event-card';
				card.style.position = 'absolute';
				card.style.left = '0';
				card.style.right = '0';
				card.style.top = top + 'px';
				card.style.height = height + 'px';
				card.style.overflow = 'hidden';
				card.style.padding = '4px 8px';
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
					// Non-blocked: clicking the card opens the general edit modal
					card.style.cursor = 'pointer';
					card.addEventListener('click', (e) => {
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
			// If we're on the login page and already authenticated, skip to /home
			if (loginForm) {
				(async () => {
					try {
						const client = await ensureAppwriteClient(); const App = AppwriteModule; const account = new App.Account(client);
						const u = await account.get();
						if (u && (u.$id || u.id)) { window.location.href = '/home'; return; }
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
					window.location.href = '/home';
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
					
					window.location.href = '/home';
				} catch (err) { console.error('Signup error',err); showFormError(signupForm, err.message || 'Sign up failed'); }
			});
		}

		function initModalAndForm() {
			const createBtn = document.getElementById('createTaskBtn'); const modal = document.getElementById('taskModal'); const cancelBtn = document.getElementById('cancelTask'); const taskForm = document.getElementById('taskCreateForm');
			if (createBtn && modal) {
				createBtn.addEventListener('click', () => { 
					const dateEl = document.getElementById('taskDueDate'); 
					if (dateEl) dateEl.value = formatDateISO(currentDay); 
					modal.style.display = 'flex'; 
					const title = document.getElementById('taskTitle'); 
					if (title) title.focus(); 
				});
			}
			if (cancelBtn && modal) cancelBtn.addEventListener('click', () => { modal.style.display = 'none'; });
			if (!taskForm) return;
			taskForm.addEventListener('submit', async (e) => {
				e.preventDefault(); 
				const name = document.getElementById('taskTitle').value.trim(); 
				const dueDate = document.getElementById('taskDueDate').value; 
				const estimateStr = (document.getElementById('taskEstimateMinutes')||{}).value || '';
				const estimateMinutes = estimateStr ? Math.max(1, parseInt(estimateStr, 10)) : 60; // default to 60 if not provided
				
				if (!name || !dueDate) return alert('Please enter task name and due date');
				
				// Convert date string to ISO datetime format for Appwrite
				const dueDateTime = new Date(dueDate + 'T23:59:59.000Z').toISOString();
				
				// Send null category - will be categorized later by background process
				const taskData = {
					name: name,
					due: dueDateTime,
					category: null,
					color: null,
					assigned: null,
					estimated_time: estimateMinutes,
					complete: false
				};
				
				try { 
					await createUserTask(taskData); 
					modal.style.display='none'; 
					taskForm.reset(); 
					await loadAndRender(); 
					// Trigger categorization of uncategorized tasks
					await categorizeTasks();
				} catch (err) { 
					console.error('create task error',err); 
					alert(err.message || 'Could not create task'); 
				}
			});
		}

		function initNavigation() {
			const prevDay = document.getElementById('prevDay'); const nextDay = document.getElementById('nextDay');
			if (prevDay) prevDay.addEventListener('click', () => { currentDay.setDate(currentDay.getDate()-1); loadAndRender(); });
			if (nextDay) nextDay.addEventListener('click', () => { currentDay.setDate(currentDay.getDate()+1); loadAndRender(); });
		}

		// --- public init ----------------------------------------------------
		async function init() {
			initPasswordToggles();
			initAuthHandlers();
			initModalAndForm();
			initNavigation();
			// If we're on the home/calendar page, ensure the user is authenticated.
			if (document.getElementById('calendar')) {
				const userId = await getCurrentUserId();
				if (!userId) {
					// Not logged in: send to login page
					console.info('User not authenticated — redirecting to /login');
					window.location.href = '/login';
					return;
				}
				await loadAndRender();
			}
		}

		// expose a couple helpers for tests
		return { init, createUserTask, listUserTasks, getCurrentUserId, deleteUserTask, updateUserTask };
	})();

		// expose globally for modal helpers and tests
		try { window.PolyTask = PolyTask; } catch (e) {}
		// start
		PolyTask.init().catch(err => console.error('Failed to initialize PolyTask', err));

	});


