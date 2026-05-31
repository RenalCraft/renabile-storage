const WebSocket = require('ws');
const crypto = require('crypto');

// Инициализируем сервер на порту 10000 (дефолтный порт для Render)
const PORT = process.env.PORT || 10000;
const wss = new WebSocket.Server({ port: PORT });

// Имитация базы данных (хранится в памяти сервера, пока он запущен)
const db = {
    users: {},       // Никнейм -> { password, code, avatar }
    codes: {},       // Код -> Никнейм
    friendships: {}  // Никнейм -> Set(Никнеймы друзей)
};

// Хранилище активных подключений: Сокет -> Никнейм
const activeConnections = new Map();

console.log(`[ЯДРО] Сервер RenaBile запущен на порту ${PORT}`);

wss.on('connection', (ws) => {
    console.log('[СЕТЬ] Новое входящее соединение.');

    ws.on('message', (rawMessage) => {
        try {
            const packet = JSON.parse(rawMessage.toString().trim());
            const { type, data } = packet;

            if (!type || !data) return;

            switch (type) {
                case 'REG':
                    handleRegister(ws, data);
                    break;
                case 'AUTH':
                    handleAuth(ws, data);
                    break;
                case 'UPDATE_PROFILE':
                    handleUpdateProfile(ws, data);
                    break;
                case 'MSG':
                    handleMessage(ws, data);
                    break;
                case 'ADD_FRIEND':
                    handleAddFriend(ws, data);
                    break;
                default:
                    console.log(`[СЕТЬ] Неизвестный тип пакета: ${type}`);
            }
        } catch (err) {
            console.error('[ОШИБКА] Ошибка парсинга пакета:', err.message);
        }
    });

    ws.on('close', () => {
        const username = activeConnections.get(ws);
        if (username) {
            console.log(`[СЕТЬ] Пользователь ${username} отключился.`);
            activeConnections.delete(ws);
            // Оповещаем друзей, что пользователь теперь оффлайн
            broadcastStatusUpdate(username);
        } else {
            console.log('[СЕТЬ] Неавторизованный клиент отключился.');
        }
    });
});

// --- ПОДПРОГРАММЫ ОБРАБОТКИ ПАКЕТОВ ---

// 1. Регистрация нового аккаунта
function handleRegister(ws, data) {
    const { username, password, avatar } = data;

    if (!username || !password) {
        return sendError(ws, 'REG_OK', 'Имя и пароль не могут быть пустыми');
    }

    if (db.users[username]) {
        return sendError(ws, 'REG_OK', 'Это имя уже занято');
    }

    // Генерируем уникальный 4-значный код, которого еще нет в базе
    let code;
    do {
        code = Math.floor(1000 + Math.random() * 9000).toString();
    } while (db.codes[code]);

    // Сохраняем в "БД"
    db.users[username] = { password, code, avatar: avatar || "" };
    db.codes[code] = username;
    db.friendships[username] = new Set();

    console.log(`[БАЗА] Создан аккаунт: ${username} | Код: #${code}`);

    // Автоматически авторизуем после успешной регистрации
    authorizeSocket(ws, username, code);
}

// 2. Авторизация (Вход)
function handleAuth(ws, data) {
    const { username, password } = data;

    if (!username || !password) {
        return sendError(ws, 'AUTH_OK', 'Заполните все поля');
    }

    const user = db.users[username];

    // Простая проверка логина и пароля
    if (!user || user.password !== password) {
        console.log(`[БАЗА] Отказ во входе: ${username}`);
        return ws.send(JSON.stringify({ type: 'auth_fail', data: { message: 'Неверное имя или пароль' } }));
    }

    console.log(`[БАЗА] Успешный вход: ${username} | Код: #${user.code}`);

    // ПРИВЯЗЫВАЕМ СОКЕТ И НЕ ЗАКРЫВАЕМ ЕГО!
    authorizeSocket(ws, username, user.code);
}

// Вспомогательный метод привязки сокета к сессии
function authorizeSocket(ws, username, code) {
    activeConnections.set(ws, username);

    // Отправляем клиенту подтверждение успеха
    ws.send(JSON.stringify({
        type: 'AUTH_OK',
        data: { username, code }
    }));

    // Сразу же высылаем обновленный список чатов и друзей
    sendFriendsList(ws, username);

    // Оповещаем друзей, что мы зашли (стали онлайн)
    broadcastStatusUpdate(username);
}

// 3. Обновление профиля (Смена авы / пароля)
function handleUpdateProfile(ws, data) {
    const username = activeConnections.get(ws);
    if (!username) return;

    const { password, avatar } = data;
    const user = db.users[username];

    if (user) {
        if (password && password.trim() !== "") user.password = password;
        if (avatar) user.avatar = avatar;

        console.log(`[БАЗА] Профиль ${username} обновлен.`);

        // Переотправляем список друзей всем, чтобы обновились аватарки
        sendFriendsList(ws, username);
        broadcastStatusUpdate(username);
    }
}

// 4. Добавление в друзья по 4-значному коду
function handleAddFriend(ws, data) {
    const myUsername = activeConnections.get(ws);
    if (!myUsername) return;

    const { code } = data;
    const friendUsername = db.codes[code];

    if (!friendUsername) {
        console.log(`[ДРУЗЬЯ] Код #${code} не найден`);
        return;
    }

    if (friendUsername === myUsername) {
        return; // Нельзя добавить самого себя
    }

    // Добавляем обоюдно в списки друзей
    db.friendships[myUsername].add(friendUsername);
    db.friendships[friendUsername].add(myUsername);

    console.log(`[ДРУЗЬЯ] ${myUsername} и ${friendUsername} теперь друзья!`);

    // Обновляем списки чатов у обоих пользователей, если они онлайн
    sendFriendsList(ws, myUsername);

    // Ищем сокет друга, чтобы обновить и ему экран в реальном времени
    for (let [socket, user] of activeConnections.entries()) {
        if (user === friendUsername) {
            sendFriendsList(socket, friendUsername);
            break;
        }
    }
}

// 5. Обработка обмена сообщениями (Глобальный или ЛС)
function handleMessage(ws, data) {
    const senderName = activeConnections.get(ws);
    if (!senderName) return;

    const { to, text, fromCode } = data;

    const messagePacket = {
        type: 'MSG',
        data: {
            senderName,
            text,
            from: to === 'GLOBAL' ? 'GLOBAL' : fromCode
        }
    };

    if (to === 'GLOBAL') {
        // Рассылаем всем подключенным пользователям в общий чат
        const raw = JSON.stringify(messagePacket);
        for (let client of activeConnections.keys()) {
            if (client.readyState === WebSocket.OPEN) {
                client.send(raw);
            }
        }
    } else {
        // Личные сообщения (ЛС). Ищем получателя по его коду
        const targetUsername = db.codes[to];
        if (!targetUsername) return;

        const raw = JSON.stringify(messagePacket);

        // Отправляем фрейм получателю, если он в сети
        for (let [socket, user] of activeConnections.entries()) {
            if (user === targetUsername && socket.readyState === WebSocket.OPEN) {
                socket.send(raw);
                break;
            }
        }
    }
}

// --- СИСТЕМНЫЕ ФУНКЦИИ РАССЫЛКИ ---

// Сборка и отправка списка друзей для конкретного юзера
function sendFriendsList(ws, username) {
    const friendsSet = db.friendships[username] || new Set();
    const list = [];

    friendsSet.forEach(fName => {
        const fUser = db.users[fName];
        if (fUser) {
            // Проверяем, онлайн ли друг прямо сейчас
            const isOnline = Array.from(activeConnections.values()).includes(fName);
            list.push({
                username: fName,
                code: fUser.code,
                avatar: fUser.avatar, // Отдаем Base64 строку аватарки на клиент
                online: isOnline
            });
        }
    });

    ws.send(JSON.stringify({
        type: 'FRIENDS_LIST',
        data: { list }
    }));
}

// Оповещение всех друзей пользователя о смене его статуса (онлайн/оффлайн)
function broadcastStatusUpdate(username) {
    const friendsSet = db.friendships[username] || new Set();

    for (let [socket, user] of activeConnections.entries()) {
        if (friendsSet.has(user) && socket.readyState === WebSocket.OPEN) {
            sendFriendsList(socket, user);
        }
    }
}

function sendError(ws, actionType, message) {
    ws.send(JSON.stringify({
        type: 'ERROR',
        message: message
    }));
}
