
import { pipeline, env } from 'https://cdn.jsdelivr.net/npm/@xenova/transformers@2.6.0';
import { 
    ensureAppwriteClient, 
    getAppwriteModule, 
    invokeWithCompat, 
    ensureUserCollection, 
    getCurrentUserId, 
    APPWRITE_DATABASE,
    APPWRITE_COLLECTION_TASKS
} from './appwrite.js';
import { showToast, formatDateISO } from './ui.js';
import { trackTaskCreation, trackTaskCompletion } from './achievement-tracker.js';

// Skip local model checks since we are running in a browser environment
env.allowLocalModels = false;
env.useBrowserCache = true;

let extractor = null;

// --- db helpers ----------------------------------------------------
export async function listUserTasks() {
    const userId = await getCurrentUserId();
    if (!userId) return [];
    const client = await ensureAppwriteClient();
    const App = getAppwriteModule();
    const db = new App.Databases(client);
    if (!App.Query || typeof db.listDocuments !== 'function') throw new Error('Appwrite Databases.listDocuments or Query not available');
    
    // Use SINGLE TASKS COLLECTION
    // Filter by user ID (permissions handle this automatically if doc security is on, but explicit query is faster/cleaner)
    try {
        const query = [ 
            App.Query.equal('userId', userId),
            // Optimize payload: Only fetch fields used by the UI
            App.Query.select(['$id', 'userId', 'name', 'due', 'assigned', 'category', 'color', 'estimated_time', 'complete', 'repeat', 'priority'])
        ];
        
        const modern = await invokeWithCompat(db, 'listDocuments', [APPWRITE_DATABASE, APPWRITE_COLLECTION_TASKS, query], {
            databaseId: APPWRITE_DATABASE,
            collectionId: APPWRITE_COLLECTION_TASKS,
            queries: query
        });
        const res = modern.called ? modern.value : await db.listDocuments(APPWRITE_DATABASE, APPWRITE_COLLECTION_TASKS, query);
        
        // Client-side safety filter: Ensure we only return tasks belonging to this user
        // This acts as a second line of defense if the DB query is ignored or malformed
        const docs = (res && res.documents) || [];
        return docs.filter(doc => doc.userId === userId);
    } catch (err) {
        // If collection missing (404), client can't fix it.
        throw err;
    }
}

export async function createUserTask(data) {
    const userId = await getCurrentUserId();
    if (!userId) throw new Error('User not authenticated');
    const client = await ensureAppwriteClient();
    const App = getAppwriteModule();
    const db = new App.Databases(client);
    // No need for ownerId - each user has their own collection
    if (typeof db.createDocument !== 'function') throw new Error('Appwrite Databases.createDocument not available');
    const uniqueId = App.ID && typeof App.ID.unique === 'function' ? App.ID.unique() : 'unique()';
    // Normalize payload to match collection schema
    const payload = {
        userId: userId, // CRITICAL: Tag with user ID for the single-collection model
        name: data.name,
        due: data.due || null,
        assigned: data.assigned || null,
        category: data.category || null,
        // color: data.color || 'cadet', // Optional: Let DB default handle it if possible, or send value
        estimated_time: typeof data.estimated_time === 'number' ? data.estimated_time : (typeof data.estimateMinutes === 'number' ? data.estimateMinutes : 60),
        complete: data.complete === true ? true : false,
        repeat: data.repeat === true ? true : false,
        priority: data.priority || 'medium'
    };

    // Remove keys that are strictly null if we want to rely on DB defaults (though Appwrite usually handles null for optional fields fine)
    // However, for required fields, they must be present.
    // The previous error was "Missing required attribute", which means 'assigned' IS required in DB but we sent null.
    // If we can't change the DB right now, we can try to send a fallback, but that's logically wrong for a calendar.
    // The user MUST fix the schema.

    
    // Explicit permissions (Read/Write for this user only)
    const permissions = [
        App.Permission.read(App.Role.user(userId)),
        App.Permission.update(App.Role.user(userId)),
        App.Permission.delete(App.Role.user(userId))
    ];

    try {
        const created = await invokeWithCompat(db, 'createDocument', [APPWRITE_DATABASE, APPWRITE_COLLECTION_TASKS, uniqueId, payload, permissions], {
            databaseId: APPWRITE_DATABASE,
            collectionId: APPWRITE_COLLECTION_TASKS,
            documentId: uniqueId,
            data: payload,
            permissions: permissions
        });
        
        // Track achievement
        trackTaskCreation();
        
        if (created.called) return created.value;
        return await db.createDocument(APPWRITE_DATABASE, APPWRITE_COLLECTION_TASKS, uniqueId, payload, permissions);
    } catch (err) {
        throw err;
    }
}

export async function updateUserTask(documentId, patch) {
    const userId = await getCurrentUserId();
    if (!userId) throw new Error('User not authenticated');
    const client = await ensureAppwriteClient();
    const App = getAppwriteModule();
    const db = new App.Databases(client);
    const isCompletingTask = patch.complete === true;
    let previousTaskData = null;

    if (isCompletingTask) {
        try {
            const before = await invokeWithCompat(db, 'getDocument', [APPWRITE_DATABASE, APPWRITE_COLLECTION_TASKS, documentId], {
                databaseId: APPWRITE_DATABASE,
                collectionId: APPWRITE_COLLECTION_TASKS,
                documentId: documentId
            });
            previousTaskData = before.called ? before.value : await db.getDocument(APPWRITE_DATABASE, APPWRITE_COLLECTION_TASKS, documentId);
        } catch (e) {
            console.warn('Could not load task before completion update:', e);
        }
    }

    const upd = await invokeWithCompat(db, 'updateDocument', [APPWRITE_DATABASE, APPWRITE_COLLECTION_TASKS, documentId, patch], {
        databaseId: APPWRITE_DATABASE,
        collectionId: APPWRITE_COLLECTION_TASKS,
        documentId: documentId,
        data: patch
    });
    const updatedTask = upd.called ? upd.value : await db.updateDocument(APPWRITE_DATABASE, APPWRITE_COLLECTION_TASKS, documentId, patch);
    
    // Track achievement if task is being completed
    if (isCompletingTask) {
        trackTaskCompletion(updatedTask);

        if (previousTaskData && previousTaskData.complete !== true) {
            try {
                await bumpFollowingTasksAfterEarlyCompletion(previousTaskData, new Date());
            } catch (e) {
                console.warn('Failed to bump following tasks after early completion:', e);
            }
        }
    }
    
    return updatedTask;
}

async function bumpFollowingTasksAfterEarlyCompletion(completedTask, completionTime = new Date()) {
    if (!completedTask || !completedTask.assigned) return 0;

    const completedId = completedTask.$id || completedTask.id;
    const scheduledStart = new Date(completedTask.assigned);
    if (isNaN(scheduledStart)) return 0;

    const scheduledDuration = typeof completedTask.estimated_time === 'number'
        ? completedTask.estimated_time
        : (typeof completedTask.estimateMinutes === 'number' ? completedTask.estimateMinutes : 60);

    const safeDuration = Math.max(1, scheduledDuration);
    const scheduledEnd = new Date(scheduledStart.getTime() + safeDuration * 60000);
    if (!(completionTime instanceof Date) || isNaN(completionTime)) return 0;
    if (completionTime >= scheduledEnd) return 0;

    const allTasks = await listUserTasks();

    const followingTasks = allTasks
        .filter(task => {
            if (!task || task.complete || !task.assigned) return false;
            const taskId = task.$id || task.id;
            if (taskId === completedId) return false;
            const taskStart = new Date(task.assigned);
            if (isNaN(taskStart)) return false;
            return taskStart > completionTime;
        })
        .sort((a, b) => new Date(a.assigned) - new Date(b.assigned));

    if (!followingTasks.length) return 0;

    let availableShiftMs = scheduledEnd.getTime() - completionTime.getTime();
    if (availableShiftMs <= 0) return 0;

    let previousTaskEnd = new Date(completionTime);
    let shiftedCount = 0;

    for (const task of followingTasks) {
        if (availableShiftMs <= 0) break;

        const taskStart = new Date(task.assigned);
        if (isNaN(taskStart)) continue;

        const taskDuration = typeof task.estimated_time === 'number'
            ? task.estimated_time
            : (typeof task.estimateMinutes === 'number' ? task.estimateMinutes : 60);
        const safeTaskDuration = Math.max(1, taskDuration);

        const desiredStart = new Date(taskStart.getTime() - availableShiftMs);
        const minAllowedStartMs = Math.max(previousTaskEnd.getTime(), completionTime.getTime());
        const newStart = new Date(Math.max(desiredStart.getTime(), minAllowedStartMs));

        if (newStart < taskStart) {
            const shiftedByMs = taskStart.getTime() - newStart.getTime();
            await updateUserTask(task.$id || task.id, { assigned: newStart.toISOString() });
            availableShiftMs -= shiftedByMs;
            shiftedCount++;
            previousTaskEnd = new Date(newStart.getTime() + safeTaskDuration * 60000);
        } else {
            previousTaskEnd = new Date(taskStart.getTime() + safeTaskDuration * 60000);
        }
    }

    if (shiftedCount > 0) {
        showToast(`Bumped ${shiftedCount} later task${shiftedCount === 1 ? '' : 's'} earlier`, 'info');
    }

    return shiftedCount;
}

export async function deleteUserTask(documentId) {
    const userId = await getCurrentUserId();
    if (!userId) throw new Error('User not authenticated');
    const client = await ensureAppwriteClient();
    const App = getAppwriteModule();
    const db = new App.Databases(client);
    const del = await invokeWithCompat(db, 'deleteDocument', [APPWRITE_DATABASE, APPWRITE_COLLECTION_TASKS, documentId], {
        databaseId: APPWRITE_DATABASE,
        collectionId: APPWRITE_COLLECTION_TASKS,
        documentId
    });
    if (del.called) return del.value;
    return await db.deleteDocument(APPWRITE_DATABASE, APPWRITE_COLLECTION_TASKS, documentId);
}

// Categorize tasks with null categories using semantic ML
export async function categorizeTasks() {
    try {
        const userId = await getCurrentUserId();
        if (!userId) return;
        
        const client = await ensureAppwriteClient();
        const App = getAppwriteModule();
        const db = new App.Databases(client);
        
        // Get tasks for ML context (optimized fetch)
        // We only need name/category/color/id for determining categories
        const query = [ 
            App.Query.equal('userId', userId),
            App.Query.select(['$id', 'userId', 'name', 'category', 'color'])
        ];

        const listResult = await invokeWithCompat(db, 'listDocuments', [APPWRITE_DATABASE, APPWRITE_COLLECTION_TASKS, query], {
            databaseId: APPWRITE_DATABASE,
            collectionId: APPWRITE_COLLECTION_TASKS,
            queries: query
        });
        const rawDocs = listResult.called ? listResult.value.documents : (await db.listDocuments(APPWRITE_DATABASE, APPWRITE_COLLECTION_TASKS, query)).documents;
        // Safety filter to prevent categorizing tasks that don't belong to current user
        const allDocs = (rawDocs || []).filter(d => d.userId === userId);
        
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
                category: finalCategory,
                color: finalColor
            };
            
            return invokeWithCompat(db, 'updateDocument', [APPWRITE_DATABASE, APPWRITE_COLLECTION_TASKS, task.$id, updatePayload], {
                databaseId: APPWRITE_DATABASE,
                collectionId: APPWRITE_COLLECTION_TASKS,
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
// Returns count of scheduled tasks
export async function autoSchedule(currentDay) {
    try {
        const userId = await getCurrentUserId();
        if (!userId) return 0;
        const nowTs = Date.now();

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

        // Floating candidates: unassigned, has due date, not completed
        // (includes overdue tasks so they can be prioritized first)
        const candidates = allTasks.filter(t => {
            if (t.assigned || t.complete) return false;
            if (!t.due) return false;
            const dueTime = new Date(t.due).getTime();
            return Number.isFinite(dueTime);
        });

        if (candidates.length === 0) {
            console.log('No unassigned tasks to schedule.');
            return 0;
        }

        // --- Improved Sorting Logic ---
        // Weights: High=3, Medium=2, Low=1
        const priorityWeight = p => (p === 'high' ? 3 : p === 'low' ? 1 : 2);

        candidates.sort((a, b) => {
            // 1. Overdue first
            const dueTsA = new Date(a.due).getTime();
            const dueTsB = new Date(b.due).getTime();
            const isOverdueA = dueTsA < nowTs;
            const isOverdueB = dueTsB < nowTs;

            if (isOverdueA && !isOverdueB) return -1;
            if (!isOverdueA && isOverdueB) return 1;

            // 2. Due Today next
            const dateA = new Date(a.due).toISOString().slice(0,10);
            const dateB = new Date(b.due).toISOString().slice(0,10);
            const isTodayA = (dateA === currentDayStr);
            const isTodayB = (dateB === currentDayStr);
            
            if (isTodayA && !isTodayB) return -1;
            if (!isTodayA && isTodayB) return 1;

            // 3. Priority Descending (High first)
            const pA = priorityWeight(a.priority);
            const pB = priorityWeight(b.priority);
            if (pA !== pB) return pB - pA;

            // 4. Due Date Ascending (Earliest first)
            const dueA = new Date(a.due).getTime();
            const dueB = new Date(b.due).getTime();
            if (dueA !== dueB) return dueA - dueB;
            
            // 5. Duration Descending (Big blocks first)
            const estA = typeof a.estimated_time === 'number' ? a.estimated_time : (typeof a.estimateMinutes === 'number' ? a.estimateMinutes : 60);
            const estB = typeof b.estimated_time === 'number' ? b.estimated_time : (typeof b.estimateMinutes === 'number' ? b.estimateMinutes : 60);
            return estB - estA;
        });

        // 3. Build a map of occupied minutes (06:00 to 24:00)
        const occupied = new Uint8Array(24 * 60); // 0 to 1439 minutes
        
        // Calculate scheduling horizon based on real time (Prevent past scheduling)
        const now = new Date();
        const pastMinutes = Math.floor((now - startOfCurrentDay) / 60000);
        
        // Block at least until 6 AM (360 mins), or until Now + 5m buffer if later
        const blackoutUntil = Math.max(6 * 60, pastMinutes + 5);

        // Mark blackout period as occupied
        for (let i = 0; i < blackoutUntil && i < 1440; i++) occupied[i] = 1;

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
        
        return scheduledCount;

    } catch (err) {
        console.error('Auto-schedule error', err);
        throw err;
    }
}

// Auto-reschedule tasks that are incomplete for 60+ minutes past scheduled end.
// Scheduled end is derived from: assigned + estimated_time (minutes).
// Returns number of tasks rescheduled.
export async function autoRescheduleOverdueTasks(referenceTime = new Date()) {
    const tasks = await listUserTasks();
    if (!tasks.length) return 0;

    const now = referenceTime instanceof Date ? referenceTime : new Date(referenceTime);
    if (isNaN(now)) return 0;

    const RESCHEDULE_THRESHOLD_MS = 60 * 60 * 1000;
    const RESCHEDULE_OFFSET_MINUTES = 15;
    const ROUND_TO_MINUTES = 15;
    const SEARCH_HORIZON_DAYS = 30;

    const ceilToStep = (date, stepMinutes) => {
        const copy = new Date(date);
        copy.setSeconds(0, 0);
        const mins = copy.getMinutes();
        const mod = mins % stepMinutes;
        if (mod !== 0) copy.setMinutes(mins + (stepMinutes - mod));
        return copy;
    };

    const overdueTasks = tasks.filter(task => {
        if (task.complete || !task.assigned) return false;
        const assigned = new Date(task.assigned);
        if (isNaN(assigned)) return false;

        const durationMinutes = typeof task.estimated_time === 'number'
            ? task.estimated_time
            : (typeof task.estimateMinutes === 'number' ? task.estimateMinutes : 60);

        const scheduledEnd = new Date(assigned.getTime() + Math.max(1, durationMinutes) * 60000);
        const overdueBy = now.getTime() - scheduledEnd.getTime();
        return overdueBy >= RESCHEDULE_THRESHOLD_MS;
    });

    if (!overdueTasks.length) return 0;

    const overdueIds = new Set(overdueTasks.map(task => task.$id || task.id));

    const occupied = tasks
        .filter(task => task.assigned && !overdueIds.has(task.$id || task.id))
        .map(task => {
            const start = new Date(task.assigned);
            if (isNaN(start)) return null;
            const durationMinutes = typeof task.estimated_time === 'number'
                ? task.estimated_time
                : (typeof task.estimateMinutes === 'number' ? task.estimateMinutes : 60);
            const safeDuration = Math.max(1, durationMinutes);
            const end = new Date(start.getTime() + safeDuration * 60000);
            return { start, end };
        })
        .filter(Boolean)
        .sort((a, b) => a.start - b.start);

    const overlapsExisting = (start, end) => {
        for (const interval of occupied) {
            if (start < interval.end && end > interval.start) return true;
        }
        return false;
    };

    const findNextFreeSlot = (startAfter, durationMinutes) => {
        const safeDuration = Math.max(1, durationMinutes);
        const stepMs = ROUND_TO_MINUTES * 60000;
        const horizonMs = SEARCH_HORIZON_DAYS * 24 * 60 * 60000;
        const searchStart = ceilToStep(startAfter, ROUND_TO_MINUTES).getTime();
        const searchEnd = searchStart + horizonMs;

        for (let cursorMs = searchStart; cursorMs <= searchEnd; cursorMs += stepMs) {
            const candidateStart = new Date(cursorMs);
            const candidateEnd = new Date(cursorMs + safeDuration * 60000);
            if (!overlapsExisting(candidateStart, candidateEnd)) {
                return { start: candidateStart, end: candidateEnd };
            }
        }

        return null;
    };

    // Reschedule oldest overdue tasks first.
    overdueTasks.sort((a, b) => {
        const aAssigned = new Date(a.assigned).getTime();
        const bAssigned = new Date(b.assigned).getTime();
        return aAssigned - bAssigned;
    });

    let rescheduledCount = 0;
    for (const task of overdueTasks) {
        const durationMinutes = typeof task.estimated_time === 'number'
            ? task.estimated_time
            : (typeof task.estimateMinutes === 'number' ? task.estimateMinutes : 60);

        const rescheduleBase = new Date(now.getTime() + RESCHEDULE_OFFSET_MINUTES * 60000);
        const freeSlot = findNextFreeSlot(rescheduleBase, durationMinutes);
        if (!freeSlot) continue;

        const patch = { assigned: freeSlot.start.toISOString() };
        const nextDueDate = freeSlot.start.toISOString().slice(0, 10);

        if (!task.due || String(task.due) < nextDueDate) {
            patch.due = nextDueDate;
        }

        await updateUserTask(task.$id || task.id, patch);
        occupied.push({ start: freeSlot.start, end: freeSlot.end });
        occupied.sort((a, b) => a.start - b.start);
        rescheduledCount++;
    }

    return rescheduledCount;
}

export function calculateStreak(tasks) {
    if (!tasks || !tasks.length) return 0;
    const completed = tasks.filter(t => t.complete);
    if (!completed.length) return 0;
    const dates = completed.map(t => {
        const d = new Date(t.$updatedAt || t.$createdAt);
        return d.toISOString().split('T')[0];
    }).sort().reverse();
    const uniqueDates = [...new Set(dates)];
    if (uniqueDates.length === 0) return 0;
    let checkDate = new Date();
    let checkStr = checkDate.toISOString().split('T')[0];
    if (!uniqueDates.includes(checkStr)) {
        checkDate.setDate(checkDate.getDate() - 1);
        checkStr = checkDate.toISOString().split('T')[0];
        if (!uniqueDates.includes(checkStr)) return 0;
    }
    let streak = 0;
    while (true) {
        if (uniqueDates.includes(checkStr)) {
            streak++;
            checkDate.setDate(checkDate.getDate() - 1);
            checkStr = checkDate.toISOString().split('T')[0];
        } else {
            break;
        }
    }
    return streak;
}

export async function clearCompletedTasks() {
    const tasks = await listUserTasks();
    const completed = tasks.filter(t => t.complete);
    if (!completed.length) return 0;
    await Promise.all(completed.map(t => deleteUserTask(t.$id)));
    return completed.length;
}
