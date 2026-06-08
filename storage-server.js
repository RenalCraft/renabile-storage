const { WebSocketServer, WebSocket } = require('ws');
const { Pool } = require('pg');

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
                                          online BOOLEAN DEFAULT FALSE
        );
        `);
        console.log('[DB] Users table verified.');

        // Live migrations for Users table (safe if columns already exist)
        try {
            await pool.query('ALTER TABLE users DROP COLUMN IF EXISTS user_code CASCADE;');
        } catch (err) {
            console.log('[DB Migration user_code warning] Ignored or already dropped:', err.message);
        }
        try {
            await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS password VARCHAR(255);');
        } catch (err) {
            console.log('[DB Migration password warning] Ignored or already applied:', err.message);
        }
        try {
            await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS code VARCHAR(4);');
        } catch (err) {
            console.log('[DB Migration code warning] Ignored or already applied:', err.message);
        }
        try {
            await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar TEXT;');
        } catch (err) {
            console.log('[DB Migration avatar warning] Ignored or already applied:', err.message);
        }
        try {
            await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS online BOOLEAN DEFAULT FALSE;');
        } catch (err) {
            console.log('[DB Migration online warning] Ignored or already applied:', err.message);
        }
        try {
            await pool.query('ALTER TABLE users ADD CONSTRAINT unique_code_node UNIQUE (code);');
        } catch (err) {}
        console.log('[DB] Users table live migrations validated.');

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
        } catch (err) {
            console.log('[DB Migration Messages room warning]:', err.message);
        }
        try {
            await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS sender VARCHAR(255);');
        } catch (err) {
            console.log('[DB Migration Messages sender warning]:', err.message);
        }
        try {
            await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS sender_code VARCHAR(4);');
        } catch (err) {
            console.log('[DB Migration Messages sender_code warning]:', err.message);
        }
        try {
            await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS text TEXT;');
        } catch (err) {
            console.log('[DB Migration Messages text warning]:', err.message);
        }
        try {
            await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS time VARCHAR(30);');
        } catch (err) {
            console.log('[DB Migration Messages time warning]:', err.message);
        }
        try {
            await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS timestamp BIGINT;');
            // Verify and migrate database schemas containing obsolete timestamp data types (e.g., TIMESTAMP)
            const checkTypeRes = await pool.query(`
            SELECT data_type FROM information_schema.columns
            WHERE table_name = 'messages' AND column_name = 'timestamp';
            `);
            if (checkTypeRes.rows.length > 0) {
                const dataType = checkTypeRes.rows[0].data_type;
                if (dataType && !dataType.toLowerCase().includes('bigint') && !dataType.toLowerCase().includes('numeric')) {
                    console.log('[DB Migration] Messages timestamp column is of invalid type:', dataType, '. Dropping and recreating as BIGINT...');
                    await pool.query('ALTER TABLE messages DROP COLUMN timestamp CASCADE;');
                    await pool.query('ALTER TABLE messages ADD COLUMN timestamp BIGINT DEFAULT 0;');
                    console.log('[DB Migration] Messages timestamp column successfully migrated to BIGINT.');
                }
            }
        } catch (err) {
            console.log('[DB Migration Messages timestamp warning]:', err.message);
        }

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

        console.log('[DB] Messages table live migrations validated.');

        // Mark everyone offline initially on server restart
        await pool.query('UPDATE users SET online = false;');
        console.log('[DB] All users marked offline in database.');
    } catch (err) {
        console.error('[DB] Critical error during table initialization:', err.message);
    }
}

// Map userCodes to active WebSocket connections
const activeConnections = new Map(); // userCode -> WebSocket

let wss = null;

// Initialize database then launch the secure server engine
initDatabase().then(() => {
    wss = new WebSocketServer({ port: PORT });
    console.log(`[RenaBile Server] WebSocket Listening on port ${PORT}`);

    wss.on('connection', (ws) => {
        let authenticatedUserCode = null;
        console.log('[Connection] User connected over WebSockets.');

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
                    await pool.query('UPDATE users SET online = false WHERE code = $1', [authenticatedUserCode]);
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
                default:
                    sendPacket(ws, 'ERROR', { message: `Неизвестный тип пакета: ${type}` });
            }
        }

        async function handleRegister(ws, data) {
            let { username, password, avatar } = data;
            if (!username || !password) {
                sendPacket(ws, 'ERROR', { message: "Заполните все обязательные поля" });
                return;
            }

            username = username.trim();
            const usernameLower = username.toLowerCase();

            try {
                // Check existence
                const checkRes = await pool.query('SELECT 1 FROM users WHERE LOWER(username) = $1 LIMIT 1', [usernameLower]);
                if (checkRes.rowCount > 0) {
                    sendPacket(ws, 'ERROR', { message: "Код ошибки 409: Пользователь с таким именем уже существует" });
                    return;
                }

                const code = await generateUniqueCode();

                // Insert user
                await pool.query(
                    'INSERT INTO users (username, password, code, avatar, online) VALUES ($1, $2, $3, $4, $5)',
                                 [username, password.toLowerCase(), code, avatar || "", true]
                );

                authenticatedUserCode = code;
                activeConnections.set(code, ws);

                console.log(`[REG] Registered: ${username} with code #${code}`);

                sendPacket(ws, 'AUTH_OK', { username, code, avatar: avatar || "" });
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
                const userRes = await pool.query('SELECT username, password, code, avatar FROM users WHERE LOWER(username) = $1 LIMIT 1', [usernameLower]);
                if (userRes.rowCount === 0) {
                    sendPacket(ws, 'ERROR', { message: "Ошибка авторизации: Пользователь не существует" });
                    return;
                }

                const user = userRes.rows[0];

                // Verify password hashes (compare in case-insensitive lowercase)
                if (user.password.toLowerCase() !== password.toLowerCase()) {
                    sendPacket(ws, 'ERROR', { message: "Ошибка авторизации: Неверный логин или пароль" });
                    return;
                }

                // Mark online
                await pool.query('UPDATE users SET online = true WHERE code = $1', [user.code]);

                authenticatedUserCode = user.code;
                activeConnections.set(user.code, ws);

                console.log(`[AUTH] Authenticated: ${user.username} (#${user.code})`);

                sendPacket(ws, 'AUTH_OK', { username: user.username, code: user.code, avatar: user.avatar || "" });
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
                } else {
                    // Return messages between matching room code & authenticated sender code
                    historyRes = await pool.query(
                        `SELECT sender, text, time, client_msg_id, reaction, is_edited, is_deleted FROM messages
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

            const { username, password, avatar } = data;
            if (!username || !username.trim()) {
                sendPacket(ws, 'ERROR', { message: "Имя не может быть пустым." });
                return;
            }

            try {
                if (password && password.trim()) {
                    await pool.query(
                        'UPDATE users SET username = $1, password = $2, avatar = $3 WHERE code = $4',
                        [username.trim(), password.trim().toLowerCase(), avatar || "", authenticatedUserCode]
                    );
                } else {
                    await pool.query(
                        'UPDATE users SET username = $1, avatar = $2 WHERE code = $3',
                        [username.trim(), avatar || "", authenticatedUserCode]
                    );
                }

                console.log(`[Profile Update] Code #${authenticatedUserCode} updated to ${username}`);

                // Send success confirmation packet
                sendPacket(ws, 'AUTH_OK', { username: username.trim(), code: authenticatedUserCode, avatar: avatar || "" });

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

// Helper: Send updated lists to user
async function sendFriendsList(ws, userCode) {
    try {
        const query = `
        SELECT u.username, u.code, u.online, u.avatar
        FROM friendships f
        JOIN users u ON f.friend_code = u.code
        WHERE f.user_code = $1
        `;
        const res = await pool.query(query, [userCode]);

        const list = res.rows.map(row => ({
            username: row.username,
            code: row.code,
            online: row.online,
            avatar: row.avatar || ""
        }));

        sendPacket(ws, 'FRIENDS_LIST', { list });
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
