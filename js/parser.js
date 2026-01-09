
import { formatDateISO } from './ui.js';

export function parseSmartInput(text) {
    const result = { title: text || '', date: null, time: null, duration: null };
    if (!text) return result;
    const lower = text.toLowerCase();
    const now = new Date();

    const durRegex = /\b(?:for\s+)?(\d+(?:\.\d+)?)\s*(m|min|mins|minutes|h|hr|hours)\b/i;
    const durMatch = text.match(durRegex);
    if (durMatch) {
        const val = parseFloat(durMatch[1]);
        const unit = durMatch[2].toLowerCase().startsWith('h') ? 60 : 1;
        result.duration = Math.round(val * unit);
        result.title = result.title.replace(durMatch[0], '');
    } else {
        if (/\b(quick|chat|check|email|standup)\b/i.test(text)) result.duration = 15;
        else if (/\b(call|meeting|sync|discussion)\b/i.test(text)) result.duration = 30;
        else if (/\b(review|draft|analysis)\b/i.test(text)) result.duration = 45;
        else if (/\b(deep|focus|write|code|coding|plan|lab)\b/i.test(text)) result.duration = 60;
    }

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
            if (!m && hour < 7) hour += 12; 
        }
        if (hour >= 0 && hour < 24 && minute >= 0 && minute < 60) {
            result.time = `${String(hour).padStart(2,'0')}:${String(minute).padStart(2,'0')}`;
            result.title = result.title.replace(match, '').replace(/\s+/g, ' ').trim();
        }
    }

    let addedDays = 0;
    if (lower.includes('tomorrow') || lower.includes('tmrw')) {
        addedDays = 1; result.title = result.title.replace(/tomorrow|tmrw/i, '');
    } else if (lower.includes('today')) {
        addedDays = 0; result.title = result.title.replace(/today/i, '');
    } else {
        const days = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
        const shortDays = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];
        for (let i=0; i<7; i++) {
            if (lower.includes(days[i]) || lower.match(new RegExp(`\\b${shortDays[i]}\\b`))) {
                const currentDayIdx = now.getDay();
                let diff = i - currentDayIdx;
                if (diff <= 0) diff += 7;
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
    } else if (!result.date) {
        // Default to today if only time is specified? Or keep null for "Someday"?
        // If time is specified, implies upcoming occurrence.
        if (result.time) {
            result.date = formatDateISO(now);
        }
    }

    result.title = result.title.replace(/\s+/g, ' ').replace(/\bat\b/gi, '').replace(/\bon\b/gi, '').replace(/\bfor\b/gi, '').trim();
    // Return clean property for UI use
    result.cleanText = result.title;
    return result;
}
