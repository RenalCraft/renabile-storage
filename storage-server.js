const WebSocket = require('ws');
const { Pool } = require('pg');
const crypto = require('crypto');

const PORT = process.env.PORT || 10000;

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

const wss = new WebSocket.Server({ port: PORT });
const activeConnections = new Map(); // Сокет -> Username

// Генерация SHA-256 в нижнем регистре
function computeHash(text) {
    return crypto.createHash('sha256').update(text).digest('hex').toLowerCase();
}

// Умная многоуровневая проверка пароля на бэкенде
function passwordMatches(incomingPassword, storedPassword) {
    if (!incomingPassword || !storedPassword) return false;

    const incomingLower = incomingPassword.toLowerCase();
    const storedLower = storedPassword.toLowerCase();

    // 1. Прямое совпадение клиентского хэша
    if (incomingLower === storedLower) return true;

    // 2. Совпадение если входящий пароль был в открытом виде
    const hashedOnce = computeHash(incomingPassword);
    if (hashedOnce === storedLower) return true;

    // 3. Совпадение для унаследованных записей с двойным хэшированием
    const doubleHashed = computeHash(hashedOnce);
    if (doubleHashed === storedLower) return true;

    return false;
}

function hashPasswordForStorage(password) {
    // Если это уже хэш (64 hex-символа), сохраняем как есть
    if (password.length === 64 && /^[0-9a-fA-F]+$/.test(password)) {
        return password.toLowerCase();
    }
    return computeHash(password);
}

async function initDB() {
    try {
        await pool.query(`
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password TEXT NOT NULL,
            user_code TEXT UNIQUE NOT NULL,
            avatar_base64 TEXT
        );`);
        await pool.query(`
        CREATE TABLE IF NOT EXISTS friends (
            username TEXT,
            friend_name TEXT,
            PRIMARY KEY (username, friend_name)
        );`);
        await pool.query(`
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            room TEXT NOT NULL,
            sender TEXT NOT NULL,
            text TEXT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );`);
        console.log('[БАЗА] Все таблицы PostgreSQL успешно верифицированы!');
    } catch (err) {
        console.error('[БАЗА ОШИБКА] Ошибка инициализации таблиц:', err.message);
    }
}
initDB();

wss.on('connection', (ws) => {
    // Расширенный вывод сырого лога сетевых соединений
    console.log(`[СЕТЬ] Новое входящее подключение WebSocket со стороны: ${ws._socket?.remoteAddress || 'Неизвестно'}`);

    ws.on('message', async (rawMessage) => {
        try {
            const lines = rawMessage.toString().split('\n');

            for (let line of lines) {
                if (!line.trim()) continue;

                const packet = JSON.parse(line.trim());
                const { type, data } = packet;
                const loggedUser = activeConnections.get(ws);

                if (type === 'REG' || type === 'AUTH') {
                    console.log(`[ПАКЕТ] Запрос авторизации/регистрации. [КРИПТОЗАЩИЩЕНО]`);
                } else {
                    console.log(`[ПАКЕТ] Тип: ${type}`);
                }

                switch (type) {
                    case 'REG':
                        await handleRegister(ws, data);
                        break;
                    case 'AUTH':
                        await handleAuth(ws, data);
                        break;
                    case 'UPDATE_PROFILE':
                        if (loggedUser) await handleUpdateProfile(ws, loggedUser, data);
                        break;
                    case 'MSG':
                        await handleMessage(ws, data);
                        break;
                    case 'ADD_FRIEND':
                        await handleAddFriend(ws, data);
                        break;
                    case 'GET_HISTORY':
                        if (data && data.room) await sendRoomHistory(ws, data.room);
                        break;
                }
            }
        } catch (err) {
            console.error('[СИСТЕМА] Ошибка обработки пакета:', err.message);
        }
    });

    ws.on('close', () => {
        const username = activeConnections.get(ws);
        if (username) {
            console.log(`[СЕТЬ] Пользователь ${username} отключился`);
            activeConnections.delete(ws);
            broadcastStatusUpdate(username);
        }
    });

    ws.on('error', (err) => {
        console.error('[СЕТЬ ОШИБКА] Ошибка на сокете:', err.message);
    });
});

async function handleRegister(ws, data) {
    const { username, password, avatar } = data;
    if (!username || !password) return;

    try {
        const check = await pool.query('SELECT username FROM users WHERE username = $1', [username]);
        if (check.rows.length > 0) {
            ws.send(JSON.stringify({ type: 'ERROR', data: { message: 'Логин занят' } }));
            return;
        }

        const securePassword = hashPasswordForStorage(password);
        let code;
        while (true) {
            code = Math.floor(1000 + Math.random() * 9000).toString();
            const codeCheck = await pool.query('SELECT user_code FROM users WHERE user_code = $1', [code]);
            if (codeCheck.rows.length === 0) break;
        }

        await pool.query('INSERT INTO users (username, password, user_code, avatar_base64) VALUES ($1, $2, $3, $4)', [username, securePassword, code, avatar || ""]);
        authorizeSocket(ws, username, code);
    } catch (e) {
        console.error('[РЕГИСТРАЦИЯ] Ошибка:', e.message);
        ws.send(JSON.stringify({ type: 'ERROR', data: { message: 'Ошибка базы данных при регистрации' } }));
    }
}

async function handleAuth(ws, data) {
    const { username, password } = data;
    try {
        const res = await pool.query('SELECT password, user_code FROM users WHERE username = $1', [username]);

        if (res.rows.length === 0) {
            ws.send(JSON.stringify({ type: 'ERROR', data: { message: 'Пользователь не найден' } }));
            return;
        }

        const storedPassword = res.rows[0].password;

        if (!passwordMatches(password, storedPassword)) {
            ws.send(JSON.stringify({ type: 'ERROR', data: { message: 'Неверные данные входа' } }));
            return;
        }

        authorizeSocket(ws, username, res.rows[0].user_code);
    } catch (e) {
        console.error('[АВТОРИЗАЦИЯ] Ошибка:', e.message);
        ws.send(JSON.stringify({ type: 'ERROR', data: { message: 'Ошибка базы данных при авторизации' } }));
    }
}

async function authorizeSocket(ws, username, code) {
    activeConnections.set(ws, username);
    ws.send(JSON.stringify({ type: 'AUTH_OK', data: { username, code } }));

    await sendRoomHistory(ws, 'GLOBAL');
    await sendFriendsList(ws, username);
    broadcastStatusUpdate(username);
}

async function handleUpdateProfile(ws, username, data) {
    const { password, avatar } = data;
    try {
        if (password && password.trim() !== "") {
            await pool.query('UPDATE users SET password = $1 WHERE username = $2', [hashPasswordForStorage(password), username]);
        }
        if (avatar) {
            await pool.query('UPDATE users SET avatar_base64 = $1 WHERE username = $2', [avatar, username]);
        }
        await sendFriendsList(ws, username);
        await broadcastStatusUpdate(username);
    } catch (e) { console.error('[ПРОФИЛЬ] Ошибка обновления:', e.message); }
}

async function handleAddFriend(ws, data) {
    const myUsername = activeConnections.get(ws);
    if (!myUsername) return;
    const { code } = data;

    try {
        const res = await pool.query('SELECT username FROM users WHERE user_code = $1', [code]);
        if (res.rows.length === 0) {
            ws.send(JSON.stringify({ type: 'ERROR', data: { message: 'Пользователь не найден' } }));
            return;
        }
        const friendUsername = res.rows[0].username;

        if (myUsername === friendUsername) {
            ws.send(JSON.stringify({ type: 'ERROR', data: { message: 'Нельзя добавить самого себя' } }));
            return;
        }

        await pool.query('INSERT INTO friends (username, friend_name) VALUES ($1, $2) ON CONFLICT DO NOTHING', [myUsername, friendUsername]);
        await pool.query('INSERT INTO friends (username, friend_name) VALUES ($1, $2) ON CONFLICT DO NOTHING', [friendUsername, myUsername]);

        await sendFriendsList(ws, myUsername);
        await sendRoomHistory(ws, code);

        for (let [socket, user] of activeConnections.entries()) {
            if (user === friendUsername) {
                await sendFriendsList(socket, friendUsername);
                const myRes = await pool.query('SELECT user_code FROM users WHERE username = $1', [myUsername]);
                await sendRoomHistory(socket, myRes.rows[0].user_code);
                break;
            }
        }
    } catch (e) { console.error(e); }
}

async function handleMessage(ws, data) {
    const sender = activeConnections.get(ws);
    if (!sender) return;
    const { to, text, fromCode } = data;

    let room = to;
    try {
        await pool.query('INSERT INTO messages (room, sender, text) VALUES ($1, $2, $3)', [room, sender, text]);

        const messagePacket = {
            type: 'MSG',
            data: { senderName: sender, text, from: to === 'GLOBAL' ? 'GLOBAL' : fromCode }
        };
        const raw = JSON.stringify(messagePacket);

        if (to === 'GLOBAL') {
            for (let client of activeConnections.keys()) {
                if (client.readyState === WebSocket.OPEN) client.send(raw);
            }
        } else {
            ws.send(raw);
            const targetRes = await pool.query('SELECT username FROM users WHERE user_code = $1', [to]);
            if (targetRes.rows.length === 0) return;
            const targetUser = targetRes.rows[0].username;

            for (let [socket, user] of activeConnections.entries()) {
                if (user === targetUser && socket.readyState === WebSocket.OPEN) {
                    const receiverPacket = {
                        type: 'MSG',
                        data: { senderName: sender, text, from: fromCode }
                    };
                    socket.send(JSON.stringify(receiverPacket));
                    break;
                }
            }
        }
    } catch (e) { console.error(e); }
}

async function sendRoomHistory(ws, room) {
    try {
        const res = await pool.query(`
        SELECT sender, text, to_char(timestamp, 'HH24:MI') as time
        FROM messages
        WHERE room = $1
        ORDER BY timestamp ASC`, [room]);

        ws.send(JSON.stringify({
            type: 'MSG_HISTORY',
            data: { room, history: res.rows }
        }));
    } catch (e) { console.error(e); }
}

async function sendFriendsList(ws, username) {
    try {
        const res = await pool.query(`
        SELECT u.username, u.user_code, u.avatar_base64
        FROM friends f
        JOIN users u ON f.friend_name = u.username
        WHERE f.username = $1`, [username]);

        const list = res.rows.map(row => {
            const isOnline = Array.from(activeConnections.values()).includes(row.username);
            return { username: row.username, code: row.user_code, avatar: row.avatar_base64 || "", online: isOnline };
        });

        ws.send(JSON.stringify({ type: 'FRIENDS_LIST', data: { list } }));
    } catch (e) { console.error(e); }
}

async function broadcastStatusUpdate(username) {
    try {
        const res = await pool.query('SELECT friend_name FROM friends WHERE username = $1', [username]);
        const friends = res.rows.map(r => r.friend_name);

        for (let [socket, user] of activeConnections.entries()) {
            if (friends.includes(user) && socket.readyState === WebSocket.OPEN) {
                await sendFriendsList(socket, user);
            }
        }
    } catch (e) { console.error(e); }
}

console.log(`[СИСТЕМА] Node.js WebSocket-сервер запущен на порту ${PORT}`);
