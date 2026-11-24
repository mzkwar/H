const express = require('express');
const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const path = require('path');
const crypto = require('crypto');

const app = express();
app.use(express.json({ limit: '10mb' }));
app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));

// ==========================================
// 1. إعداد قاعدة البيانات المحلية (SQLite) + دعم Firebase الاختياري
// ==========================================
const dbPath = path.resolve(__dirname, 'database.db');
const db = new sqlite3.Database(dbPath, (err) => {
    if (err) console.error("خطأ في قاعدة البيانات:", err.message);
    else console.log("✅ تم الاتصال بقاعدة البيانات المحلية (SQLite) بنجاح.");
});

// تهيئة Firebase Admin تلقائياً إذا توفرت بيانات الاعتماد في متغيرات البيئة
let firestoreDb = null;
try {
    if (process.env.FIREBASE_SERVICE_ACCOUNT) {
        const admin = require('firebase-admin');
        const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
        admin.initializeApp({
            credential: admin.credential.cert(serviceAccount)
        });
        firestoreDb = admin.firestore();
        console.log("🔥 تم تفعيل المزامنة السحابية مع Firebase Firestore بنجاح.");
    } else {
        console.log("ℹ️ يعمل النظام بقاعدة البيانات المدمجة السريعة (SQLite Cache) وجاهز للربط مع Firebase.");
    }
} catch (fbErr) {
    console.error("⚠️ تعذر تهيئة Firebase، سيتم الاعتماد على قاعدة البيانات المحلية:", fbErr.message);
}

db.serialize(() => {
    // جدول الوكلاء الثانويين
    db.run(`CREATE TABLE IF NOT EXISTS sub_agents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        password TEXT,
        name TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    // جدول ربط المشتركين بالوكلاء
    db.run(`CREATE TABLE IF NOT EXISTS assigned_subscribers (
        subscriber_id TEXT PRIMARY KEY,
        sub_agent_id INTEGER,
        assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(sub_agent_id) REFERENCES sub_agents(id) ON DELETE CASCADE
    )`);

    // جدول تخزين المشتركين المؤقت والذكي (بدون تكرار + بصمة التغيير)
    db.run(`CREATE TABLE IF NOT EXISTS cached_subscribers (
        username TEXT PRIMARY KEY,
        subscriber_id TEXT,
        customer_id TEXT,
        customer_name TEXT,
        rx_power REAL,
        status TEXT,
        is_active INTEGER,
        has_active_session INTEGER,
        plan_type TEXT,
        expires TEXT,
        started_at TEXT,
        raw_json TEXT,
        data_hash TEXT,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    db.run(`CREATE INDEX IF NOT EXISTS idx_sub_customer_name ON cached_subscribers(customer_name)`);
    db.run(`CREATE INDEX IF NOT EXISTS idx_sub_status ON cached_subscribers(is_active, has_active_session)`);
});

// دوال مساعدة لـ SQLite
const dbAll = (sql, params = []) => new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => err ? reject(err) : resolve(rows));
});
const dbGet = (sql, params = []) => new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => err ? reject(err) : resolve(row));
});
const dbRun = (sql, params = []) => new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
        if (err) reject(err);
        else resolve(this);
    });
});

function extractVal(field) {
    if (!field && field !== false && field !== 0) return '';
    if (typeof field === 'object') return field.displayValue || field.name || field.title || field.id || '';
    return String(field);
}

function cleanUser(u) {
    const raw = extractVal(u);
    return raw.startsWith('@') ? raw.substring(1).trim() : raw.trim();
}

function computeSubscriberHash(sub) {
    const relevant = {
        c: extractVal(sub.customer) || extractVal(sub.subscriber),
        s: extractVal(sub.status),
        a: Boolean(sub.isActive),
        o: Boolean(sub.hasActiveSession),
        p: sub.rxPower ?? null,
        e: sub.expires || sub.expirationDate || '',
        pkg: extractVal(sub.package) || extractVal(sub.self)
    };
    return crypto.createHash('md5').update(JSON.stringify(relevant)).digest('hex');
}

// ==========================================
// 2. مسار فحص النبض ومنع السكون (Keep-Alive على Render)
// ==========================================
app.get('/health', (req, res) => {
    res.status(200).json({ status: 'awake', timestamp: new Date().toISOString(), uptime: process.uptime() });
});

app.get('/ping', (req, res) => {
    res.status(200).send('pong');
});

// ==========================================
// 3. تسجيل الدخول
// ==========================================
app.post('/api/ftth/login', async (req, res) => {
    const { username, password } = req.body;
    try {
        const params = new URLSearchParams();
        params.append('grant_type', 'password');
        params.append('client_id', 'earthlink-portals');
        params.append('username', username);
        params.append('password', password);

        const response = await axios.post('https://admin.ftth.iq/api/auth/Contractor/token', params, {
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json'
            },
            timeout: 15000
        });
        res.json(response.data);
    } catch (error) {
        console.error("خطأ تسجيل الدخول:", error.response?.data || error.message);
        res.status(401).json({ message: "فشل تسجيل الدخول، تحقق من البيانات", error: error.response?.data || error.message });
    }
});

// ==========================================
// 4. جلب المشتركين المخزنين محلياً بسرعة البرق (بدون ضرب الـ API)
// ==========================================
app.get('/api/ftth/subscriptions/cached', async (req, res) => {
    try {
        const rows = await dbAll(`SELECT raw_json, updated_at FROM cached_subscribers ORDER BY customer_name ASC`);
        const items = rows.map(r => {
            try {
                return JSON.parse(r.raw_json);
            } catch {
                return null;
            }
        }).filter(Boolean);

        const lastSyncRow = await dbGet(`SELECT MAX(updated_at) as lastSync FROM cached_subscribers`);
        res.json({
            items,
            total: items.length,
            lastSync: lastSyncRow?.lastSync || null,
            source: firestoreDb ? 'sqlite+firebase' : 'sqlite-cache'
        });
    } catch (error) {
        console.error("خطأ قراءة الكاش:", error);
        res.status(500).json({ error: "تعذر قراءة البيانات المخزنة" });
    }
});

// ==========================================
// 5. دالة سحب جميع المشتركين وتجاوز حد الـ 500 مشترك
// ==========================================
async function fetchAllSubscriptionsFromApi(token) {
    const uniqueMap = new Map();
    const pageSize = 500;
    let pageNumber = 1;
    let keepFetching = true;
    const maxPages = 40; // يدعم حتى 20,000 مشترك بأمان

    while (keepFetching && pageNumber <= maxPages) {
        const url = `https://admin.ftth.iq/api/subscriptions?hierarchyLevel=0&pageSize=${pageSize}&pageNumber=${pageNumber}`;
        const response = await axios.get(url, {
            headers: { 'Authorization': token, 'Accept': 'application/json' },
            timeout: 25000
        });

        const data = response.data;
        const batch = Array.isArray(data) ? data : (data.items || data.subscribers || data.data || []);
        const totalCount = data.totalCount || data.total || null;

        if (!batch || batch.length === 0) {
            keepFetching = false;
            break;
        }

        let newlyAddedInBatch = 0;
        for (const sub of batch) {
            const key = cleanUser(sub.username) || String(sub.id || '');
            if (key && !uniqueMap.has(key)) {
                uniqueMap.set(key, sub);
                newlyAddedInBatch++;
            }
        }

        // إذا أرجع السيرفر نفس البيانات (تجاهل pageNumber) أو وصلنا للعدد الكلي، نتوقف
        if (newlyAddedInBatch === 0 || batch.length < pageSize || (totalCount && uniqueMap.size >= totalCount)) {
            keepFetching = false;
        } else {
            pageNumber++;
            await new Promise(r => setTimeout(r, 150)); // مهلة خفيفة لتجنب الحظر
        }
    }

    // خطة احتياطية: إذا كان الـ API يتجاهل pageNumber ويقفل عند 500 سجل بالترتيب الافتراضي،
    // نسحب بالترتيب التصاعدي والتنازلي والنشط والمنتهي لجمع أكبر عدد ممكن بدون تكرار
    if (uniqueMap.size === 500) {
        const fallbackQueries = [
            'https://admin.ftth.iq/api/subscriptions?hierarchyLevel=0&pageSize=1000&sortCriteria.property=username&sortCriteria.direction=desc',
            'https://admin.ftth.iq/api/subscriptions?hierarchyLevel=0&pageSize=1000&sortCriteria.property=createdAt&sortCriteria.direction=asc',
            'https://admin.ftth.iq/api/subscriptions?hierarchyLevel=0&pageSize=1000&sortCriteria.property=createdAt&sortCriteria.direction=desc',
            'https://admin.ftth.iq/api/subscriptions?hierarchyLevel=1&pageSize=1000'
        ];

        for (const fbUrl of fallbackQueries) {
            try {
                const fbRes = await axios.get(fbUrl, {
                    headers: { 'Authorization': token, 'Accept': 'application/json' },
                    timeout: 20000
                });
                const fbData = fbRes.data;
                const fbBatch = Array.isArray(fbData) ? fbData : (fbData.items || fbData.subscribers || fbData.data || []);
                for (const sub of fbBatch) {
                    const key = cleanUser(sub.username) || String(sub.id || '');
                    if (key && !uniqueMap.has(key)) {
                        uniqueMap.set(key, sub);
                    }
                }
            } catch (_) {
                // تجاهل أي استعلام احتياطي غير مدعوم
            }
        }
    }

    return Array.from(uniqueMap.values());
}

// ==========================================
// 6. المزامنة الذكية (Delta Sync: تحديث المتغير وإضافة الجديد فقط بدون تكرار)
// ==========================================
let isSyncingNow = false;

app.post('/api/ftth/sync', async (req, res) => {
    const token = req.headers.authorization;
    if (!token) return res.status(401).json({ error: "غير مصرح" });

    if (isSyncingNow) {
        return res.status(429).json({ message: "عملية مزامنة جارية حالياً، يرجى الانتظار لحظات..." });
    }

    isSyncingNow = true;
    try {
        const remoteSubs = await fetchAllSubscriptionsFromApi(token);
        const existingRows = await dbAll(`SELECT username, data_hash FROM cached_subscribers`);
        const existingHashMap = new Map(existingRows.map(r => [r.username, r.data_hash]));

        let addedCount = 0;
        let updatedCount = 0;
        let unchangedCount = 0;
        const changedDocsForFirebase = [];

        await dbRun('BEGIN TRANSACTION');
        const stmt = db.prepare(`
            INSERT INTO cached_subscribers (
                username, subscriber_id, customer_id, customer_name, rx_power,
                status, is_active, has_active_session, plan_type, expires,
                started_at, raw_json, data_hash, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(username) DO UPDATE SET
                subscriber_id = excluded.subscriber_id,
                customer_id = excluded.customer_id,
                customer_name = excluded.customer_name,
                rx_power = excluded.rx_power,
                status = excluded.status,
                is_active = excluded.is_active,
                has_active_session = excluded.has_active_session,
                plan_type = excluded.plan_type,
                expires = excluded.expires,
                started_at = excluded.started_at,
                raw_json = excluded.raw_json,
                data_hash = excluded.data_hash,
                updated_at = CURRENT_TIMESTAMP
        `);

        for (const sub of remoteSubs) {
            const username = cleanUser(sub.username) || cleanUser(sub.id);
            if (!username) continue;

            const newHash = computeSubscriberHash(sub);
            const oldHash = existingHashMap.get(username);

            if (oldHash === newHash) {
                unchangedCount++;
                continue; // لم يتغير شيء، لا داعي للكتابة في القاعدة
            }

            if (!oldHash) addedCount++;
            else updatedCount++;

            const statusVal = extractVal(sub.status);
            const isActive = (sub.isActive || statusVal.toLowerCase() === 'active' || statusVal === 'فعال') ? 1 : 0;
            const isOnline = sub.hasActiveSession ? 1 : 0;
            const customerName = extractVal(sub.customer) || extractVal(sub.subscriber) || 'غير محدد';
            const customerId = String(sub.customerId || sub.accountId || (typeof sub.customer === 'object' ? sub.customer.id : '') || sub.id || '');
            const planType = extractVal(sub.package) || extractVal(sub.self) || 'SELF';
            const expires = sub.expires || sub.expirationDate || '';
            const startedAt = sub.startedAt || sub.createdAt || '';
            const rxPower = sub.rxPower !== undefined && sub.rxPower !== null ? Number(sub.rxPower) : null;

            stmt.run(
                username,
                String(sub.id || username),
                customerId,
                customerName,
                rxPower,
                statusVal,
                isActive,
                isOnline,
                planType,
                expires,
                startedAt,
                JSON.stringify(sub),
                newHash
            );

            if (firestoreDb) {
                changedDocsForFirebase.push({ username, sub, newHash });
            }
        }

        await new Promise((resolve, reject) => stmt.finalize(err => err ? reject(err) : resolve()));
        await dbRun('COMMIT');

        // مزامنة التغييرات فقط إلى Firebase Firestore في الخلفية (بدون تكرار عبر doc(username))
        if (firestoreDb && changedDocsForFirebase.length > 0) {
            (async () => {
                try {
                    const chunkSize = 400;
                    for (let i = 0; i < changedDocsForFirebase.length; i += chunkSize) {
                        const batch = firestoreDb.batch();
                        const chunk = changedDocsForFirebase.slice(i, i + chunkSize);
                        for (const item of chunk) {
                            const ref = firestoreDb.collection('ftth_subscribers').doc(item.username);
                            batch.set(ref, {
                                ...item.sub,
                                _usernameKey: item.username,
                                _hash: item.newHash,
                                _syncedAt: new Date().toISOString()
                            }, { merge: true });
                        }
                        await batch.commit();
                    }
                } catch (fbSyncErr) {
                    console.error("خطأ مزامنة Firebase بالخلفية:", fbSyncErr.message);
                }
            })();
        }

        const allRows = await dbAll(`SELECT raw_json FROM cached_subscribers ORDER BY customer_name ASC`);
        const items = allRows.map(r => JSON.parse(r.raw_json));

        res.json({
            message: `اكتملت المزامنة: +${addedCount} جديد، ${updatedCount} محدث، ${unchangedCount} بدون تغيير`,
            stats: { added: addedCount, updated: updatedCount, unchanged: unchangedCount, total: items.length },
            items
        });
    } catch (error) {
        try { await dbRun('ROLLBACK'); } catch (_) {}
        console.error("خطأ أثناء المزامنة:", error.response?.data || error.message);
        res.status(error.response?.status || 500).json({ error: "فشل الاتصال بسيرفر الشركة للمزامنة" });
    } finally {
        isSyncingNow = false;
    }
});

// الإبقاء على المسار القديم للتوافق
app.get('/api/ftth/subscriptions', async (req, res) => {
    const token = req.headers.authorization;
    if (!token) return res.status(401).json({ error: "غير مصرح" });

    try {
        const items = await fetchAllSubscriptionsFromApi(token);
        res.json(items);
    } catch (error) {
        res.status(error.response?.status || 500).json({ error: "تعذر جلب الاشتراكات" });
    }
});

// تفاصيل الـ ONT حسب الـ Username
app.get('/api/ftth/ont', async (req, res) => {
    const token = req.headers.authorization;
    const username = req.query.username;
    if (!token) return res.status(401).json({ error: "غير مصرح" });

    try {
        const response = await axios.get(`https://admin.ftth.iq/api/subscriptions/device/ont?username=${username}`, {
            headers: { 'Authorization': token, 'Accept': 'application/json' },
            timeout: 15000
        });
        res.json(response.data);
    } catch (error) {
        res.status(500).json({ error: "تعذر جلب معلومات الـ ONT" });
    }
});

// ملف العميل الشامل واشتراكاته بالاعتماد على customerId
app.get('/api/ftth/customer/:id', async (req, res) => {
    const token = req.headers.authorization;
    const customerId = req.params.id;
    if (!token) return res.status(401).json({ error: "غير مصرح" });

    try {
        const [custRes, subsRes] = await Promise.all([
            axios.get(`https://admin.ftth.iq/api/customers/${customerId}`, { headers: { 'Authorization': token, 'Accept': 'application/json' }, timeout: 15000 }).catch(() => ({ data: {} })),
            axios.get(`https://admin.ftth.iq/api/customers/subscriptions?customerId=${customerId}`, { headers: { 'Authorization': token, 'Accept': 'application/json' }, timeout: 15000 }).catch(() => ({ data: {} }))
        ]);

        res.json({
            customer: custRes.data.model || custRes.data,
            customerSubscriptions: subsRes.data.items || subsRes.data.model || []
        });
    } catch (error) {
        res.status(500).json({ error: "تعذر جلب بيانات العميل" });
    }
});

// محفظة الشريك
app.get('/api/ftth/wallet/:partnerId', async (req, res) => {
    const token = req.headers.authorization;
    const partnerId = req.params.partnerId;
    if (!token) return res.status(401).json({ error: "غير مصرح" });

    try {
        const response = await axios.get(`https://admin.ftth.iq/api/partners/${partnerId}/wallets/balance`, {
            headers: { 'Authorization': token, 'Accept': 'application/json' },
            timeout: 15000
        });
        res.json(response.data);
    } catch (error) {
        res.status(500).json({ error: "تعذر جلب رصيد المحفظة" });
    }
});

// الجلسات
app.get('/api/ftth/sessions', async (req, res) => {
    const token = req.headers.authorization;
    if (!token) return res.status(401).json({ error: "غير مصرح" });

    try {
        const response = await axios.get('https://admin.ftth.iq/api/sessions?pageSize=50&pageNumber=1&sortCriteria.property=username&sortCriteria.direction=asc&status=1&hierarchyLevel=1', {
            headers: { 'Authorization': token, 'Accept': 'application/json' },
            timeout: 15000
        });
        res.json(response.data);
    } catch (error) {
        res.status(500).json({ error: "تعذر جلب الجلسات" });
    }
});

// التذاكر
app.get('/api/ftth/tickets', async (req, res) => {
    const token = req.headers.authorization;
    if (!token) return res.status(401).json({ error: "غير مصرح" });

    try {
        const response = await axios.get('https://admin.ftth.iq/api/support/tickets?pageSize=12&pageNumber=1&sortCriteria.property=createdAt&sortCriteria.direction=desc&hierarchyLevel=1&additionalFilter=ForMe', {
            headers: { 'Authorization': token, 'Accept': 'application/json' },
            timeout: 15000
        });
        res.json(response.data);
    } catch (error) {
        res.status(500).json({ error: "تعذر جلب التذاكر" });
    }
});

// المهام والطلبات
app.get('/api/ftth/requests', async (req, res) => {
    const token = req.headers.authorization;
    if (!token) return res.status(401).json({ error: "غير مصرح" });

    try {
        const response = await axios.get('https://admin.ftth.iq/api/requests?pageSize=10&pageNumber=1&sortCriteria.property=CreatedAt&sortCriteria.direction=desc&typeIds=28be9a02-1365-400b-9db4-003829eb962c&status=Open&additionalFilters=MyRequests', {
            headers: { 'Authorization': token, 'Accept': 'application/json' },
            timeout: 15000
        });
        res.json(response.data);
    } catch (error) {
        res.status(500).json({ error: "تعذر جلب المهام" });
    }
});

// ==========================================
// 7. مسارات الوكلاء الثانويين
// ==========================================
app.post('/api/admin/sub-agents', (req, res) => {
    const { username, password, name } = req.body;
    if (!username || !password || !name) return res.status(400).json({ error: "جميع الحقول مطلوبة" });

    db.run(`INSERT INTO sub_agents (username, password, name) VALUES (?, ?, ?)`,
        [username, password, name],
        function (err) {
            if (err) return res.status(400).json({ error: "اسم المستخدم مستعمل بالفعل" });
            res.json({ id: this.lastID, username, name });
        }
    );
});

app.get('/api/admin/sub-agents', (req, res) => {
    const query = `
        SELECT sa.id, sa.username, sa.name, sa.created_at, COUNT(sub.subscriber_id) as subscriber_count
        FROM sub_agents sa
        LEFT JOIN assigned_subscribers sub ON sa.id = sub.sub_agent_id
        GROUP BY sa.id
    `;
    db.all(query, [], (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json(rows);
    });
});

app.post('/api/admin/assign', (req, res) => {
    const { subscriberIds, subAgentId } = req.body;
    if (!subscriberIds || !subAgentId) return res.status(400).json({ error: "بيانات ناقصة" });

    const stmt = db.prepare(`INSERT OR REPLACE INTO assigned_subscribers (subscriber_id, sub_agent_id) VALUES (?, ?)`);
    subscriberIds.forEach(id => stmt.run(id, subAgentId));
    stmt.finalize();

    res.json({ message: `تم تحويل ${subscriberIds.length} مشترك بنجاح` });
});

app.post('/api/subagent/login', (req, res) => {
    const { username, password } = req.body;
    db.get(`SELECT id, name, username FROM sub_agents WHERE username = ? AND password = ?`, [username, password], (err, row) => {
        if (err || !row) return res.status(401).json({ error: "بيانات دخول الوكيل غير صحيحة" });
        res.json(row);
    });
});

// إرجاع بيانات المشتركين الكاملة المخزنة للوكيل الثانوي (وليس فقط المعرف)
app.get('/api/subagent/my-subscribers/:subAgentId', async (req, res) => {
    const { subAgentId } = req.params;
    try {
        const rows = await dbAll(`
            SELECT a.subscriber_id, c.customer_name, c.rx_power, c.is_active, c.has_active_session, c.plan_type, c.expires
            FROM assigned_subscribers a
            LEFT JOIN cached_subscribers c ON a.subscriber_id = c.username
            WHERE a.sub_agent_id = ?
        `, [subAgentId]);
        res.json(rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ==========================================
// 8. آلية منع السكون والخمول على Render (Anti-Sleep Keep-Alive)
// ==========================================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`🚀 السيرفر يعمل على Port ${PORT}`);

    const externalUrl = process.env.RENDER_EXTERNAL_URL || process.env.APP_URL;
    if (externalUrl) {
        console.log(`🛡️ تم تفعيل مانع الخمول الذاتي على الرابط: ${externalUrl}/health`);
        // إرسال نبضة كل 10 دقائق (Render يدخل في السكون بعد 15 دقيقة من الخمول)
        setInterval(async () => {
            try {
                await axios.get(`${externalUrl}/health`, { timeout: 10000 });
            } catch (e) {
                console.warn("⚠️ تعذر إرسال نبضة منع السكون:", e.message);
            }
        }, 10 * 60 * 1000);
    }
});
