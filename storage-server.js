const { WebSocketServer, WebSocket } = require('ws');
const { Pool } = require('pg');
const crypto = require('crypto');

// Port definition
const PORT = process.env.PORT || 8080;

// Verify and retrieve DATABASE_URL from Render Environment
const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) {
    console.warn("[WARNING] DATABASE_URL environment variable is missing! The server will not connect to PostgreSQL until config is provided.");
}

// Create connection pool
const pool = new Pool({
    connectionString: databaseUrl,
    ssl: databaseUrl && (
        databaseUrl.includes('render.com') ||
        databaseUrl.includes('aws') ||
        databaseUrl.includes('elephantsql') ||
        databaseUrl.includes('neon.tech')
    ) ? { rejectUnauthorized: false } : false
});

// Crypto helper block for secure AES-256-CBC with Master Setup Key
function encrypt(text, key) {
    try {
        const hashedKey = crypto.createHash('sha256').update(key).digest();
        const iv = crypto.randomBytes(16);
        const cipher = crypto.createCipheriv('aes-256-cbc', hashedKey, iv);
        let encrypted = cipher.update(text, 'utf8', 'hex');
        encrypted += cipher.final('hex');
        return iv.toString('hex') + ':' + encrypted;
    } catch (e) {
        return null;
    }
}

function decrypt(encryptedText, key) {
    try {
        const hashedKey = crypto.createHash('sha256').update(key).digest();
        const parts = encryptedText.split(':');
        const iv = Buffer.from(parts[0], 'hex');
        const encrypted = parts[1];
        const decipher = crypto.createDecipheriv('aes-256-cbc', hashedKey, iv);
        let decrypted = decipher.update(encrypted, 'hex', 'utf8');
        decrypted += decipher.final('utf8');
        return decrypted;
    } catch (e) {
        return null;
    }
}

// Initialize DB schema
async function initDatabase() {
    if (!databaseUrl) return;
    try {
        await pool.query(`
        CREATE TABLE IF NOT EXISTS users (
            username VARCHAR(255) PRIMARY KEY,
                                          password VARCHAR(255) NOT NULL,
                                          code VARCHAR(4) UNIQUE NOT NULL,
                                          avatar TEXT,
                                          nickname VARCHAR(255),
                                          email VARCHAR(255),
                                          registration_ip VARCHAR(100),
                                          last_ip VARCHAR(100),
                                          wakeup_enabled BOOLEAN DEFAULT FALSE,
                                          online BOOLEAN DEFAULT FALSE,
                                          last_seen BIGINT DEFAULT 0
        );
        `);
        console.log('[DB] Users table verified.');

        // Live migrations for Users table
        const userCols = {
            nickname: 'VARCHAR(255)',
            email: 'VARCHAR(255)',
            registration_ip: 'VARCHAR(100)',
            last_ip: 'VARCHAR(100)',
            wakeup_enabled: 'BOOLEAN DEFAULT FALSE'
        };
        for (const [colName, colType] of Object.entries(userCols)) {
            try {
                await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS ${colName} ${colType};`);
            } catch (err) {
                console.log(`[DB Migration ${colName} warning]:`, err.message);
            }
        }

        try {
            await pool.query('ALTER TABLE users DROP COLUMN IF EXISTS user_code CASCADE;');
        } catch (err) {}

        await pool.query(`
        CREATE TABLE IF NOT EXISTS server_config (
            key VARCHAR(255) PRIMARY KEY,
                                                  value TEXT
        );
        `);
        console.log('[DB] server_config table verified.');

        // Generate master decryption key for Moderator if not exists
        const checkKey = await pool.query("SELECT value FROM server_config WHERE key = 'moderator_key' LIMIT 1");
        if (checkKey.rowCount === 0) {
            const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
            let generatedKey = '';
            for (let i = 0; i < 16; i++) {
                generatedKey += chars.charAt(Math.floor(Math.random() * chars.length));
            }
            await pool.query("INSERT INTO server_config (key, value) VALUES ('moderator_key', $1)", [generatedKey]);
            console.log(`[DB] Created Master Decryption Setup Key for Mod: ${generatedKey}`);
        }

        await pool.query(`
        CREATE TABLE IF NOT EXISTS reset_requests (
            id SERIAL PRIMARY KEY,
            username VARCHAR(255) NOT NULL,
                                                   code VARCHAR(4),
                                                   email VARCHAR(255) NOT NULL,
                                                   reg_ip VARCHAR(100),
                                                   reset_ip VARCHAR(100),
                                                   encrypted_data TEXT,
                                                   status VARCHAR(20) DEFAULT 'PENDING',
                                                   request_time BIGINT
        );
        `);
        console.log('[DB] reset_requests table verified.');

        await pool.query(`
        CREATE TABLE IF NOT EXISTS groups (
            id VARCHAR(50) PRIMARY KEY,
                                           name VARCHAR(255) NOT NULL,
                                           avatar TEXT,
                                           creator_code VARCHAR(4) NOT NULL,
                                           members TEXT NOT NULL
        );
        `);
        console.log('[DB] groups table verified.');

        await pool.query(`
        CREATE TABLE IF NOT EXISTS friendships (
            user_code VARCHAR(4) NOT NULL,
                                                friend_code VARCHAR(4) NOT NULL,
                                                PRIMARY KEY (user_code, friend_code)
        );
        `);
        console.log('[DB] Friendships table verified.');

        await pool.query(`
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            room VARCHAR(255) NOT NULL,
                                             sender VARCHAR(255) NOT NULL,
                                             sender_code VARCHAR(4) NOT NULL,
                                             text TEXT NOT NULL,
                                             time VARCHAR(30) NOT NULL,
                                             timestamp BIGINT NOT NULL
        );
        `);
        console.log('[DB] Messages table verified.');

        // Live migrations for Messages table
        try {
            await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS room VARCHAR(255);');
        } catch (err) {}
        try {
            await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS sender VARCHAR(255);');
        } catch (err) {}
        try {
            await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS sender_code VARCHAR(4);');
        } catch (err) {}
        try {
            await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS text TEXT;');
        } catch (err) {}
        try {
            await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS time VARCHAR(30);');
        } catch (err) {}
        try {
            await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS timestamp BIGINT;');
            const checkTypeRes = await pool.query(`
            SELECT data_type FROM information_schema.columns
            WHERE table_name = 'messages' AND column_name = 'timestamp';
            `);
            if (checkTypeRes.rows.length > 0) {
                const dataType = checkTypeRes.rows[0].data_type;
                if (dataType && !dataType.toLowerCase().includes('bigint') && !dataType.toLowerCase().includes('numeric')) {
                    await pool.query('ALTER TABLE messages DROP COLUMN timestamp CASCADE;');
                    await pool.query('ALTER TABLE messages ADD COLUMN timestamp BIGINT DEFAULT 0;');
                }
            }
        } catch (err) {}

        try {
            await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS client_msg_id VARCHAR(255);');
        } catch (err) {}
        try {
            await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS reaction VARCHAR(255) DEFAULT \'\';');
        } catch (err) {}
        try {
            await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS is_edited BOOLEAN DEFAULT FALSE;');
        } catch (err) {}
        try {
            await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE;');
        } catch (err) {}

        // Mark everyone offline initially on server restart
        await pool.query('UPDATE users SET online = false;');
    } catch (err) {
        console.error('[DB] Critical error during table initialization:', err.message);
    }
}

// Map userCodes to active WebSocket connections
const activeConnections = new Map(); // userCode -> WebSocket

let wss = null;

// Helper to extract client IP safely
function getClientIp(ws, req) {
    if (req) {
        const forwarded = req.headers['x-forwarded-for'];
        if (forwarded) return forwarded.split(',')[0].trim();
        if (req.socket) return req.socket.remoteAddress || '127.0.0.1';
    }
    if (ws && ws._socket) {
        return ws._socket.remoteAddress || '127.0.0.1';
    }
    return '127.0.0.1';
}

// Initialize database then launch the secure server engine
initDatabase().then(() => {
    wss = new WebSocketServer({ port: PORT });
    console.log(`[RenaBile Server] WebSocket Listening on port ${PORT}`);

    // Automatic clean up: Delete message attachments older than 14 days
    setInterval(async () => {
        try {
            const twoWeeksAgo = Date.now() - (14 * 24 * 60 * 60 * 1000);
            const deleteRes = await pool.query(
                "DELETE FROM messages WHERE text LIKE '[IMAGE]:%' AND timestamp < $1",
                [twoWeeksAgo]
            );
            if (deleteRes.rowCount > 0) {
                console.log(`[DB Clean] Auto-deleted ${deleteRes.rowCount} photo attachments older than 14 days.`);
            }
        } catch (err) {
            console.error('[DB Clean Error] Failed to run automatic retention cleanup:', err.message);
        }
    }, 6 * 60 * 60 * 1000); // Executed every 6 hours

    wss.on('connection', (ws, req) => {
        let authenticatedUserCode = null;
        const clientIp = getClientIp(ws, req);
        console.log(`[Connection] User connected from IP ${clientIp} over WebSockets.`);

        ws.on('message', async (message) => {
            const payloadStr = message.toString();
            // Server packets are split on '\n'
            const lines = payloadStr.split('\n');

            for (const line of lines) {
                const trimmed = line.trim();
                if (!trimmed) continue;

                try {
                    const packet = JSON.parse(trimmed);
                    await handlePacket(ws, packet);
                } catch (err) {
                    console.error('[Connection] Error processing packet:', err.message, 'Raw line:', trimmed);
                    sendPacket(ws, 'ERROR', { message: "Неправильный формат запроса" });
                }
            }
        });

        ws.on('close', async () => {
            console.log('[Connection] User disconnected.');
            if (authenticatedUserCode) {
                try {
                    await pool.query('UPDATE users SET online = false, last_seen = $1 WHERE code = $2', [Date.now(), authenticatedUserCode]);
                    console.log(`[Status] Offline: #${authenticatedUserCode}`);
                    activeConnections.delete(authenticatedUserCode);
                    await broadcastToFriends(authenticatedUserCode);
                } catch (err) {
                    console.error('[Connection] Error taking user offline in DB:', err.message);
                }
            }
        });

        ws.on('error', (err) => {
            console.error('[Connection] Socket error occurred:', err.message);
        });

        // Handle incoming client packet types
        async function handlePacket(ws, packet) {
            const { type, data } = packet;
            if (!type || !data) {
                sendPacket(ws, 'ERROR', { message: "Метаданные пакета повреждены" });
                return;
            }

            console.log(`[Request] Handling packet type: ${type}`);

            switch (type) {
                case 'REG':
                    await handleRegister(ws, data);
                    break;
                case 'AUTH':
                    await handleAuth(ws, data);
                    break;
                case 'ADD_FRIEND':
                    await handleAddFriend(ws, data);
                    break;
                case 'GET_HISTORY':
                    await handleGetHistory(ws, data);
                    break;
                case 'MSG':
                    await handleSendMessage(ws, data);
                    break;
                case 'UPDATE_PROFILE':
                    await handleUpdateProfile(ws, data);
                    break;
                case 'MSG_UPDATE':
                    await handleMsgUpdate(ws, data);
                    break;
                case 'CLEAR_HISTORY':
                    await handleClearHistory(ws, data);
                    break;
                case 'RESET_REQ':
                    await handleResetRequest(ws, data, clientIp);
                    break;
                case 'CHECK_RESET_STATUS':
                    await handleCheckResetStatus(ws, data);
                    break;
                case 'CREATE_GROUP':
                    await handleCreateGroup(ws, data);
                    break;
                case 'WAKE_UP_ALERT':
                    await handleWakeUpAlert(ws, data);
                    break;
                case 'MOD_GET_REQUESTS':
                    await handleModGetRequests(ws, data);
                    break;
                case 'MOD_APPROVE':
                    await handleModApprove(ws, data);
                    break;
                case 'MOD_DECLINE':
                    await handleModDecline(ws, data);
                    break;
                case 'MOD_ROTATE_KEY':
                    await handleModRotateKey(ws, data);
                    break;
                case 'MOD_EMERGENCY':
                    await handleModEmergency(ws, data);
                    break;
                default:
                    sendPacket(ws, 'ERROR', { message: `Неизвестный тип пакета: ${type}` });
            }
        }

        async function handleRegister(ws, data) {
            let { username, password, avatar, email, nickname } = data;
            if (!username || !password) {
                sendPacket(ws, 'ERROR', { message: "Заполните все обязательные поля" });
                return;
            }

            username = username.trim();
            const usernameLower = username.toLowerCase();
            const activeNickname = nickname ? nickname.trim() : username;
            const activeEmail = email ? email.trim() : "";

            try {
                // Check existence
                const checkRes = await pool.query('SELECT 1 FROM users WHERE LOWER(username) = $1 LIMIT 1', [usernameLower]);
                if (checkRes.rowCount > 0) {
                    sendPacket(ws, 'ERROR', { message: "Код ошибки 409: Пользователь с таким логином уже существует" });
                    return;
                }

                const code = await generateUniqueCode();

                // Insert user
                await pool.query(
                    'INSERT INTO users (username, password, code, avatar, online, nickname, email, registration_ip, last_ip) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)',
                                 [username, password.toLowerCase(), code, avatar || "", true, activeNickname, activeEmail, clientIp, clientIp]
                );

                authenticatedUserCode = code;
                activeConnections.set(code, ws);

                console.log(`[REG] Registered: ${username} (Code #${code}, Email: ${activeEmail}, IP: ${clientIp})`);

                sendPacket(ws, 'AUTH_OK', { username: activeNickname, loginId: username, code, avatar: avatar || "", email: activeEmail, wakeupEnabled: false });
                await sendFriendsList(ws, code);
            } catch (err) {
                console.error('[REG] DB failure during registration:', err.message);
                sendPacket(ws, 'ERROR', { message: "Внутренняя ошибка сервера при генерации кода" });
            }
        }

        async function handleAuth(ws, data) {
            let { username, password } = data;
            if (!username || !password) {
                sendPacket(ws, 'ERROR', { message: "Укажите имя пользователя и пароль" });
                return;
            }

            username = username.trim();
            const usernameLower = username.toLowerCase();

            try {
                // Match either login, nickname, or short code
                const userRes = await pool.query(
                    'SELECT username, password, code, avatar, nickname, email, wakeup_enabled FROM users WHERE LOWER(username) = $1 OR code = $1 OR LOWER(nickname) = $1 LIMIT 1',
                                                 [usernameLower]
                );
                if (userRes.rowCount === 0) {
                    sendPacket(ws, 'ERROR', { message: "Ошибка авторизации: Пользователь не существует" });
                    return;
                }

                const user = userRes.rows[0];

                // Verify password hashes
                if (user.password.toLowerCase() !== password.toLowerCase()) {
                    sendPacket(ws, 'ERROR', { message: "Ошибка авторизации: Неверный логин или пароль" });
                    return;
                }

                // Mark online and update last IP
                await pool.query('UPDATE users SET online = true, last_ip = $1 WHERE code = $2', [clientIp, user.code]);

                authenticatedUserCode = user.code;
                activeConnections.set(user.code, ws);

                console.log(`[AUTH] Authenticated: ${user.username} (#${user.code}) from IP: ${clientIp}`);

                const activeNickname = user.nickname || user.username;
                sendPacket(ws, 'AUTH_OK', { username: activeNickname, loginId: user.username, code: user.code, avatar: user.avatar || "", email: user.email || "", wakeupEnabled: !!user.wakeup_enabled });
                await sendFriendsList(ws, user.code);
                await broadcastToFriends(user.code);
            } catch (err) {
                console.error('[AUTH] DB error during auth:', err.message);
                sendPacket(ws, 'ERROR', { message: "Ошибка сервера при авторизации" });
            }
        }

        async function handleAddFriend(ws, data) {
            if (!authenticatedUserCode) {
                sendPacket(ws, 'ERROR', { message: "Сессия больше не валидна, требуется войти снова." });
                return;
            }

            const { code } = data;
            if (!code) {
                sendPacket(ws, 'ERROR', { message: "Введите корректный код потенциального собеседника." });
                return;
            }

            if (code === authenticatedUserCode) {
                sendPacket(ws, 'ERROR', { message: "Вы не можете добавить свой собственный код." });
                return;
            }

            try {
                // Find target
                const targetRes = await pool.query('SELECT username FROM users WHERE code = $1 LIMIT 1', [code]);
                if (targetRes.rowCount === 0) {
                    sendPacket(ws, 'ERROR', { message: "Пользователь со специальным кодом #" + code + " не найден в базе." });
                    return;
                }

                // Check if already friends
                const existsRes = await pool.query(
                    'SELECT 1 FROM friendships WHERE user_code = $1 AND friend_code = $2 LIMIT 1',
                    [authenticatedUserCode, code]
                );
                if (existsRes.rowCount > 0) {
                    sendPacket(ws, 'ERROR', { message: "Пользователь уже находится в вашем списке чатов." });
                    return;
                }

                // Commit friendship relation mutually
                await pool.query('INSERT INTO friendships (user_code, friend_code) VALUES ($1, $2)', [authenticatedUserCode, code]);
                await pool.query('INSERT INTO friendships (user_code, friend_code) VALUES ($1, $2)', [code, authenticatedUserCode]);

                console.log(`[Friendship] Added link between #${authenticatedUserCode} and #${code}`);

                // Direct instant update
                await sendFriendsList(ws, authenticatedUserCode);

                const friendWs = activeConnections.get(code);
                if (friendWs && friendWs.readyState === WebSocket.OPEN) {
                    await sendFriendsList(friendWs, code);
                }
            } catch (err) {
                console.error('[AddFriend] DB transaction failure:', err.message);
                sendPacket(ws, 'ERROR', { message: "Произошла ошибка при установлении контакта." });
            }
        }

        async function handleGetHistory(ws, data) {
            if (!authenticatedUserCode) {
                sendPacket(ws, 'ERROR', { message: "Требуется авторизация" });
                return;
            }

            const { room } = data;
            if (!room) {
                sendPacket(ws, 'ERROR', { message: "Комната не указана" });
                return;
            }

            console.log(`[History] Fetching history for ${room}`);

            try {
                let historyRes;
                if (room === 'GLOBAL') {
                    historyRes = await pool.query(
                        'SELECT sender, text, time, client_msg_id, reaction, is_edited, is_deleted FROM messages WHERE room = $1 ORDER BY timestamp ASC',
                        ['GLOBAL']
                    );
                } else if (room.startsWith('group_')) {
                    historyRes = await pool.query(
                        `SELECT sender, sender_code, text, time, client_msg_id, reaction, is_edited, is_deleted FROM messages
                        WHERE room = $1
                        ORDER BY timestamp ASC`,
                        [room]
                    );
                } else {
                    // Return messages between matching room code & authenticated sender code
                    historyRes = await pool.query(
                        `SELECT sender, sender_code, text, time, client_msg_id, reaction, is_edited, is_deleted FROM messages
                        WHERE (room = $1 AND sender_code = $2)
                        OR (room = $2 AND sender_code = $1)
                        ORDER BY timestamp ASC`,
                        [room, authenticatedUserCode]
                    );
                }

                const historyList = historyRes.rows.map(row => ({
                    sender: row.sender,
                    text: row.text,
                    time: row.time,
                    client_msg_id: row.client_msg_id || "",
                    reaction: row.reaction || "",
                    is_edited: !!row.is_edited,
                    is_deleted: !!row.is_deleted
                }));

                sendPacket(ws, 'MSG_HISTORY', { room, history: historyList });
            } catch (err) {
                console.error('[History] DB select failure:', err.message);
                sendPacket(ws, 'ERROR', { message: "Не удалось загрузить историю чата" });
            }
        }

        async function handleClearHistory(ws, data) {
            if (!authenticatedUserCode) {
                sendPacket(ws, 'ERROR', { message: "Требуется авторизация" });
                return;
            }

            const { room } = data;
            if (!room) {
                sendPacket(ws, 'ERROR', { message: "Комната не указана" });
                return;
            }

            console.log(`[Clear] Clearing history for room ${room} requested by #${authenticatedUserCode}`);
            try {
                if (room === 'GLOBAL') {
                    await pool.query('DELETE FROM messages WHERE room = $1', ['GLOBAL']);
                } else {
                    await pool.query(
                        `DELETE FROM messages
                        WHERE (room = $1 AND sender_code = $2)
                        OR (room = $2 AND sender_code = $1)`,
                                     [room, authenticatedUserCode]
                    );
                }

                sendPacket(ws, 'MSG_HISTORY', { room, history: [] });

                const otherWs = activeConnections.get(room);
                if (otherWs && otherWs.readyState === WebSocket.OPEN) {
                    sendPacket(otherWs, 'MSG_HISTORY', { room: authenticatedUserCode, history: [] });
                }

                console.log(`[Clear] History successfully cleared for room ${room}`);
            } catch (err) {
                console.error('[Clear] Database deletion failure:', err.message);
                sendPacket(ws, 'ERROR', { message: "Не удалось очистить историю" });
            }
        }

        async function handleSendMessage(ws, data) {
            if (!authenticatedUserCode) {
                sendPacket(ws, 'ERROR', { message: "Ошибка отправки: Сессия не авторизована." });
                return;
            }

            const { to, text, clientMsgId } = data;
            if (!to || !text || !text.trim()) {
                return;
            }

            try {
                const senderUsername = await getUsernameByCode(authenticatedUserCode);
                const now = new Date();
                const timeStr = now.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });

                // 1. BROADCAST IMMEDIATELY (sub-millisecond WebSocket delivery)
                if (to === 'GLOBAL') {
                    wss.clients.forEach(client => {
                        if (client.readyState === WebSocket.OPEN) {
                            sendPacket(client, 'MSG', {
                                from: 'GLOBAL',
                                senderName: senderUsername,
                                text: text,
                                clientMsgId: clientMsgId || ""
                            });
                        }
                    });
                } else if (to.startsWith('group_')) {
                    // Group message broadcast
                    const gRes = await pool.query('SELECT members FROM groups WHERE id = $1 LIMIT 1', [to]);
                    if (gRes.rowCount > 0) {
                        const members = typeof gRes.rows[0].members === 'string'
                        ? JSON.parse(gRes.rows[0].members)
                        : gRes.rows[0].members;
                        if (Array.isArray(members)) {
                            members.forEach(memberCode => {
                                if (memberCode !== authenticatedUserCode) {
                                    const memberWs = activeConnections.get(memberCode);
                                    if (memberWs && memberWs.readyState === WebSocket.OPEN) {
                                        sendPacket(memberWs, 'MSG', {
                                            from: to,
                                            senderName: senderUsername,
                                            text: text,
                                            clientMsgId: clientMsgId || ""
                                        });
                                    }
                                }
                            });
                        }
                    }
                    // Send back to current client so it displays immediately
                    sendPacket(ws, 'MSG', {
                        from: to,
                        senderName: senderUsername,
                        text: text,
                        clientMsgId: clientMsgId || ""
                    });
                } else {
                    // Private message
                    const targetCompanionWs = activeConnections.get(to);
                    if (targetCompanionWs && targetCompanionWs.readyState === WebSocket.OPEN) {
                        sendPacket(targetCompanionWs, 'MSG', {
                            from: authenticatedUserCode,
                            senderName: senderUsername,
                            text: text,
                            clientMsgId: clientMsgId || ""
                        });
                    }

                    // Send back to current client so it displays immediately
                    sendPacket(ws, 'MSG', {
                        from: to,
                        senderName: senderUsername,
                        text: text,
                        clientMsgId: clientMsgId || ""
                    });
                }

                // 2. PERSIST TO POSTGRES ASYNC (Decoupled from realtime WebSocket delivery)
                pool.query(
                    'INSERT INTO messages (room, sender, sender_code, text, time, timestamp, client_msg_id) VALUES ($1, $2, $3, $4, $5, $6, $7)',
                           [to, senderUsername, authenticatedUserCode, text, timeStr, now.getTime(), clientMsgId || ""]
                ).catch(err => {
                    console.error('[MSG] Async DB write failure:', err.message);
                });

            } catch (err) {
                console.error('[MSG] Send error:', err.message);
            }
        }

        async function handleUpdateProfile(ws, data) {
            if (!authenticatedUserCode) {
                sendPacket(ws, 'ERROR', { message: "Сессия не авторизована." });
                return;
            }

            const { username, password, avatar, wakeupEnabled } = data;
            if (!username || !username.trim()) {
                sendPacket(ws, 'ERROR', { message: "Имя не может быть пустым." });
                return;
            }

            const parsedWakeup = wakeupEnabled !== undefined ? !!wakeupEnabled : false;

            try {
                if (password && password.trim()) {
                    await pool.query(
                        'UPDATE users SET username = $1, password = $2, avatar = $3, wakeup_enabled = $4 WHERE code = $5',
                        [username.trim(), password.trim().toLowerCase(), avatar || "", parsedWakeup, authenticatedUserCode]
                    );
                } else {
                    await pool.query(
                        'UPDATE users SET username = $1, avatar = $2, wakeup_enabled = $3 WHERE code = $4',
                        [username.trim(), avatar || "", parsedWakeup, authenticatedUserCode]
                    );
                }

                console.log(`[Profile Update] Code #${authenticatedUserCode} updated to ${username} (Wakeup: ${parsedWakeup})`);

                // Send success confirmation packet
                sendPacket(ws, 'AUTH_OK', { username: username.trim(), code: authenticatedUserCode, avatar: avatar || "", wakeupEnabled: parsedWakeup });

                // Refresh lists
                await sendFriendsList(ws, authenticatedUserCode);
                await broadcastToFriends(authenticatedUserCode);
            } catch (err) {
                console.error('[UPDATE_PROFILE] Failed:', err.message);
                sendPacket(ws, 'ERROR', { message: "Ошибка бэкенда при обновлении профиля." });
            }
        }

        async function handleMsgUpdate(ws, data) {
            if (!authenticatedUserCode) {
                sendPacket(ws, 'ERROR', { message: "Сессия не авторизована." });
                return;
            }

            const { clientMsgId, room, text, reaction, isEdited, isDeleted } = data;
            if (!clientMsgId) return;

            try {
                // Async database update
                pool.query(
                    'UPDATE messages SET text = $1, reaction = $2, is_edited = $3, is_deleted = $4 WHERE client_msg_id = $5',
                    [text || "", reaction || "", !!isEdited, !!isDeleted, clientMsgId]
                ).catch(err => {
                    console.error('[MSG_UPDATE] DB write error:', err.message);
                });

                // Prepare update frame
                const updateFrame = {
                    clientMsgId,
                    room,
                    text: text || "",
                    reaction: reaction || "",
                    isEdited: !!isEdited,
                    isDeleted: !!isDeleted
                };

                // Broadcast
                if (room === 'GLOBAL') {
                    wss.clients.forEach(client => {
                        if (client.readyState === WebSocket.OPEN) {
                            sendPacket(client, 'MSG_UPDATE', updateFrame);
                        }
                    });
                } else {
                    const companionWs = activeConnections.get(room);
                    if (companionWs && companionWs.readyState === WebSocket.OPEN) {
                        sendPacket(companionWs, 'MSG_UPDATE', updateFrame);
                    }
                    sendPacket(ws, 'MSG_UPDATE', updateFrame);
                }
            } catch (err) {
                console.error('[MSG_UPDATE] broadcast error:', err.message);
            }
        }
    });
}).catch(err => {
    console.error("[CRITICAL] Could not initialize database schema:", err.message);
    process.exit(1);
});

// Helper: send serial message
function sendPacket(ws, type, data) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    const packet = JSON.stringify({ type, data }) + "\n";
    ws.send(packet);
}

async function sendGroupsList(ws, userCode) {
    try {
        const res = await pool.query('SELECT id, name, avatar, creator_code, members FROM groups');
        const list = [];
        res.rows.forEach(row => {
            let membersArray = [];
            try {
                membersArray = JSON.parse(row.members);
            } catch (e) {}
            if (Array.isArray(membersArray) && membersArray.includes(userCode)) {
                list.push({
                    id: row.id,
                    name: row.name,
                    avatar: row.avatar || "",
                    creatorCode: row.creator_code,
                    members: membersArray
                });
            }
        });
        sendPacket(ws, 'GROUPS_LIST', { list });
    } catch (err) {
        console.error('[Groups] Get list error:', err.message);
    }
}

// Helper: Send updated lists to user
async function sendFriendsList(ws, userCode) {
    try {
        const query = `
        SELECT u.username, u.code, u.online, u.avatar, u.last_seen
        FROM friendships f
        JOIN users u ON f.friend_code = u.code
        WHERE f.user_code = $1
        `;
        const res = await pool.query(query, [userCode]);

        const list = res.rows.map(row => ({
            username: row.username,
            code: row.code,
            online: row.online,
            avatar: row.avatar || "",
            last_seen: row.last_seen ? Number(row.last_seen) : 0
        }));

        sendPacket(ws, 'FRIENDS_LIST', { list });
        await sendGroupsList(ws, userCode);
    } catch (err) {
        console.error('[Friends] Get list error:', err.message);
    }
}

// Helper: Broadcast offline status to buddies
async function broadcastToFriends(userCode) {
    try {
        const res = await pool.query('SELECT friend_code FROM friendships WHERE user_code = $1', [userCode]);
        for (const row of res.rows) {
            const fCode = row.friend_code;
            const friendWs = activeConnections.get(fCode);
            if (friendWs && friendWs.readyState === WebSocket.OPEN) {
                await sendFriendsList(friendWs, fCode);
            }
        }
    } catch (err) {
        console.error('[Broadcast] Failure notifying list to companions:', err.message);
    }
}

// Helper: Generate a random unique 4-digit code (e.g. "4921")
async function generateUniqueCode() {
    let attempt = 0;
    while (attempt < 10000) {
        const num = Math.floor(Math.random() * 10000);
        const codeStr = String(num).padStart(4, '0');

        // Check uniqueness in DB
        const res = await pool.query('SELECT 1 FROM users WHERE code = $1 LIMIT 1', [codeStr]);
        if (res.rowCount === 0) {
            return codeStr;
        }
        attempt++;
    }
    throw new Error("Unable to generate unique 4-digit code. Range exceeded.");
}

// Helper: Get username safely by code
async function getUsernameByCode(code) {
    try {
        const res = await pool.query('SELECT username FROM users WHERE code = $1 LIMIT 1', [code]);
        return res.rows[0] ? res.rows[0].username : "Пользователь";
    } catch (err) {
        return "Пользователь";
    }
}

async function handleResetRequest(ws, data, clientIp) {
    const { username, email } = data;
    if (!username || !email) {
        sendPacket(ws, 'ERROR', { message: 'Заполните логин и e-mail для сброса пароля' });
        return;
    }
    try {
        const userRes = await pool.query(
            'SELECT username, code, email, registration_ip, last_ip, nickname FROM users WHERE LOWER(username) = $1 OR code = $1 OR LOWER(nickname) = $1 LIMIT 1',
                                         [username.trim().toLowerCase()]
        );
        if (userRes.rowCount === 0) {
            sendPacket(ws, 'ERROR', { message: 'Пользователь не найден' });
            return;
        }
        const user = userRes.rows[0];

        if (user.email && user.email.toLowerCase().trim() !== email.toLowerCase().trim()) {
            sendPacket(ws, 'ERROR', { message: 'Введенный e-mail не совпадает с привязанным' });
            return;
        }

        const tempPassword = String(Math.floor(Math.random() * 900000 + 100000));

        const keyRes = await pool.query("SELECT value FROM server_config WHERE key = 'moderator_key' LIMIT 1");
        const modKey = keyRes.rows[0].value;

        const dataPayload = JSON.stringify({
            username: user.username,
            code: user.code,
            registration_ip: user.registration_ip || 'unknown',
            last_ip: user.last_ip || 'unknown',
            email: email,
            temp_password: tempPassword,
            reset_ip: clientIp,
            request_time: Date.now()
        });

        const encryptedData = encrypt(dataPayload, modKey);

        await pool.query(
            'INSERT INTO reset_requests (username, code, email, reg_ip, reset_ip, encrypted_data, status, request_time) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)',
                         [user.username, user.code, email, user.registration_ip || 'unknown', clientIp, encryptedData, 'PENDING', Date.now()]
        );

        console.log(`[PASSWORD RESET REQ] Created for: ${user.username} (#${user.code}). Temp password generated, encrypted for Mod.`);

        sendPacket(ws, 'RESET_REQ_CONFIRMED', {
            message: 'Заявка на сброс пароля отправлена модераторам. Ожидайте подтверждения.',
            code: user.code
        });
    } catch (err) {
        console.error('[RESET_REQ] Error:', err.message);
        sendPacket(ws, 'ERROR', { message: 'Внутренняя ошибка сервера при обработке сброса' });
    }
}

async function handleCheckResetStatus(ws, data) {
    const { code } = data;
    if (!code) {
        sendPacket(ws, 'ERROR', { message: 'Укажите код #' });
        return;
    }
    try {
        const res = await pool.query(
            'SELECT id, status, encrypted_data FROM reset_requests WHERE code = $1 ORDER BY id DESC LIMIT 1',
            [code.trim()]
        );
        if (res.rowCount === 0) {
            sendPacket(ws, 'RESET_STATUS', { status: 'NONE', message: 'Заявок на восстановление не найдено' });
            return;
        }
        const req = res.rows[0];

        if (req.status === 'APPROVED') {
            const keyRes = await pool.query("SELECT value FROM server_config WHERE key = 'moderator_key' LIMIT 1");
            const modKey = keyRes.rows[0].value;
            const decrypted = decrypt(req.encrypted_data, modKey);
            if (decrypted) {
                const payload = JSON.parse(decrypted);
                sendPacket(ws, 'RESET_STATUS', {
                    status: 'APPROVED',
                    tempPassword: payload.temp_password,
                    message: `Восстановление подтверждено! Ваш новый временный пароль: ${payload.temp_password}`
                });
                return;
            }
        }

        sendPacket(ws, 'RESET_STATUS', {
            status: req.status,
            message: req.status === 'DECLINED' ? 'Заявка на сброс пароля была отклонена модератором.' : 'Ваша заявка ожидает проверки модератором.'
        });
    } catch (err) {
        console.error('[CHECK_RESET_STATUS] Error:', err.message);
        sendPacket(ws, 'ERROR', { message: 'Ошибка проверки статуса восстановления' });
    }
}

async function handleCreateGroup(ws, data) {
    const { name, avatar, members } = data;
    if (!name) {
        sendPacket(ws, 'ERROR', { message: 'Укажите название группы' });
        return;
    }
    try {
        const creatorCode = authenticatedUserCode;
        if (!creatorCode) {
            sendPacket(ws, 'ERROR', { message: 'Сессия не авторизована' });
            return;
        }

        let memberList = members || [];
        if (!memberList.includes(creatorCode)) {
            memberList.push(creatorCode);
        }

        const groupId = 'group_' + Math.floor(Math.random() * 1000000 + 100000);
        await pool.query(
            'INSERT INTO groups (id, name, avatar, creator_code, members) VALUES ($1, $2, $3, $4, $5)',
                         [groupId, name, avatar || '', creatorCode, JSON.stringify(memberList)]
        );

        console.log(`[GROUPS] Group "${name}" created with ID: ${groupId} by creator #${creatorCode}`);

        const groupPayload = { id: groupId, name, avatar: avatar || '', creatorCode, members: memberList };
        for (const code of memberList) {
            const memberWs = activeConnections.get(code);
            if (memberWs && memberWs.readyState === WebSocket.OPEN) {
                sendPacket(memberWs, 'GROUP_CREATED', groupPayload);
                await sendGroupsList(memberWs, code);
            }
        }
    } catch (err) {
        console.error('[CREATE_GROUP] Error:', err.message);
        sendPacket(ws, 'ERROR', { message: 'Ошибка создания группы на сервере' });
    }
}

async function handleWakeUpAlert(ws, data) {
    const { targetCode } = data;
    if (!targetCode) return;
    try {
        const senderCode = authenticatedUserCode;
        if (!senderCode) return;

        const userRes = await pool.query('SELECT username, nickname, wakeup_enabled FROM users WHERE code = $1 LIMIT 1', [targetCode]);
        if (userRes.rowCount === 0) return;
        const receiver = userRes.rows[0];

        if (!receiver.wakeup_enabled) {
            sendPacket(ws, 'ERROR', { message: 'У пользователя отключена функция пробуждения в настройках' });
            return;
        }

        const targetWs = activeConnections.get(targetCode);
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
            const sName = receiver.nickname || receiver.username;
            sendPacket(targetWs, 'WAKE_UP_TRIGGER', {
                senderNickname: sName,
                senderCode: senderCode
            });
            sendPacket(ws, 'WAKE_UP_SENT', { message: 'Пробуждающий сигнал доставлен!' });
            console.log(`[WAKE_UP] Alert sent from #${senderCode} to #${targetCode}`);
        } else {
            sendPacket(ws, 'ERROR', { message: 'Пользователь сейчас не в сети' });
        }
    } catch (e) {
        console.error('[WAKE_UP] Error:', e.message);
    }
}

async function handleModGetRequests(ws, data) {
    const { master_key } = data;
    if (!master_key) {
        sendPacket(ws, 'ERROR', { message: 'Укажите секретный мастер-ключ' });
        return;
    }
    try {
        const keyRes = await pool.query("SELECT value FROM server_config WHERE key = 'moderator_key' LIMIT 1");
        const realKey = keyRes.rows[0].value;
        if (realKey !== master_key.trim()) {
            sendPacket(ws, 'ERROR', { message: 'Неверный мастер-ключ модератора!' });
            return;
        }

        const reqsRes = await pool.query('SELECT id, username, code, email, reg_ip, reset_ip, encrypted_data, status, request_time FROM reset_requests ORDER BY id DESC');
        sendPacket(ws, 'MOD_REQUESTS_LIST', { requests: reqsRes.rows });
    } catch (err) {
        console.error('[MOD_GET] Failed:', err.message);
        sendPacket(ws, 'ERROR', { message: 'Ошибка получения заявок' });
    }
}

async function handleModApprove(ws, data) {
    const { request_id, master_key } = data;
    if (!request_id || !master_key) {
        sendPacket(ws, 'ERROR', { message: 'Укажите ID заявки и мастер-ключ' });
        return;
    }
    try {
        const keyRes = await pool.query("SELECT value FROM server_config WHERE key = 'moderator_key' LIMIT 1");
        const realKey = keyRes.rows[0].value;
        if (realKey !== master_key.trim()) {
            sendPacket(ws, 'ERROR', { message: 'Неверный мастер-ключ модератора!' });
            return;
        }

        const reqRes = await pool.query('SELECT username, code, encrypted_data FROM reset_requests WHERE id = $1 LIMIT 1', [request_id]);
        if (reqRes.rowCount === 0) {
            sendPacket(ws, 'ERROR', { message: 'Заявка не найдена' });
            return;
        }
        const reqRow = reqRes.rows[0];

        const decrypted = decrypt(reqRow.encrypted_data, realKey);
        if (!decrypted) {
            sendPacket(ws, 'ERROR', { message: 'Ошибка дешифрования данных. Возможно, ключ утерян.' });
            return;
        }

        const payload = JSON.parse(decrypted);
        const tempPassword = payload.temp_password;

        await pool.query('UPDATE users SET password = $1 WHERE code = $2', [tempPassword.toLowerCase(), reqRow.code]);
        await pool.query("UPDATE reset_requests SET status = 'APPROVED' WHERE id = $1", [request_id]);

        console.log(`[MOD APPROVED] Request #${request_id} for user ${reqRow.username}. Password is reset to temporary.`);

        const userWs = activeConnections.get(reqRow.code);
        if (userWs && userWs.readyState === WebSocket.OPEN) {
            sendPacket(userWs, 'RESET_STATUS', {
                status: 'APPROVED',
                tempPassword: tempPassword,
                message: `Восстановление одобрено! Новый временный пароль: ${tempPassword}`
            });
        }

        sendPacket(ws, 'MOD_ACTION_OK', { message: `Заявка #${request_id} успешно одобрена!` });
    } catch (err) {
        console.error('[MOD_APPROVE] Failed:', err.message);
        sendPacket(ws, 'ERROR', { message: 'Внутренняя ошибка одобрения' });
    }
}

async function handleModDecline(ws, data) {
    const { request_id, master_key } = data;
    if (!request_id || !master_key) {
        sendPacket(ws, 'ERROR', { message: 'Укажите ID заявки и мастер-ключ' });
        return;
    }
    try {
        const keyRes = await pool.query("SELECT value FROM server_config WHERE key = 'moderator_key' LIMIT 1");
        const realKey = keyRes.rows[0].value;
        if (realKey !== master_key.trim()) {
            sendPacket(ws, 'ERROR', { message: 'Неверный мастер-ключ' });
            return;
        }

        await pool.query("UPDATE reset_requests SET status = 'DECLINED' WHERE id = $1", [request_id]);
        sendPacket(ws, 'MOD_ACTION_OK', { message: `Заявка #${request_id} отклонена.` });
    } catch (err) {
        console.error('[MOD_DECLINE] Error:', err.message);
        sendPacket(ws, 'ERROR', { message: 'Ошибка отклонения заявки' });
    }
}

async function handleModRotateKey(ws, data) {
    const { master_key, new_key } = data;
    if (!master_key || !new_key) {
        sendPacket(ws, 'ERROR', { message: 'Заполните все поля для смены ключа' });
        return;
    }
    if (new_key.length !== 16) {
        sendPacket(ws, 'ERROR', { message: 'Новый ключ обязан содержать ровно 16 символов!' });
        return;
    }
    try {
        const keyRes = await pool.query("SELECT value FROM server_config WHERE key = 'moderator_key' LIMIT 1");
        const realKey = keyRes.rows[0].value;
        if (realKey !== master_key.trim()) {
            sendPacket(ws, 'ERROR', { message: 'Неверный текущий мастер-ключ' });
            return;
        }

        await pool.query("UPDATE server_config SET value = $1 WHERE key = 'moderator_key'", [new_key.trim()]);
        console.log(`[MASTER KEY ROTATED] Moderator key successfully rotated to: ${new_key.trim()}`);
        sendPacket(ws, 'MOD_ACTION_OK', { message: 'Ключ успешно изменен!' });
    } catch (err) {
        console.error('[ROTATE] Error:', err.message);
        sendPacket(ws, 'ERROR', { message: 'Ошибка ротации ключа' });
    }
}

async function handleModEmergency(ws, data) {
    const { master_key } = data;
    if (!master_key) {
        sendPacket(ws, 'ERROR', { message: 'Введите мастер-ключ' });
        return;
    }
    try {
        const keyRes = await pool.query("SELECT value FROM server_config WHERE key = 'moderator_key' LIMIT 1");
        const realKey = keyRes.rows[0].value;
        if (realKey !== master_key.trim()) {
            sendPacket(ws, 'ERROR', { message: 'Неверный мастер-ключ!' });
            return;
        }

        await pool.query('TRUNCATE TABLE reset_requests CASCADE');
        await pool.query('DELETE FROM server_config WHERE key = \'moderator_key\'');
        await pool.query('UPDATE users SET password = \'destroyed\', avatar = \'\', email = \'\', registration_ip = \'\', last_ip = \'\'');
        await pool.query('TRUNCATE friendships CASCADE');
        await pool.query('TRUNCATE messages CASCADE');
        await pool.query('TRUNCATE groups CASCADE');

        console.warn('[EMERGENCY SELF-DESTRUCT] Self-destruct command triggered from mod interface! DB wiped clean!');
        sendPacket(ws, 'EMERGENCY_DESTROYED', { message: 'Самоликвидация сервера проведена успешно! Все ключи и данные полностью стерты.' });
    } catch (err) {
        console.error('[EMERGENCY DELETE] Critical crash during force self destruct:', err.message);
        sendPacket(ws, 'ERROR', { message: 'Ошибка стирания данных' });
    }
}
