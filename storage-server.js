const express = require('express');
const { Pool } = require('pg');
const http = require('http');

const app = express();
app.use(express.json());

// Жестко прописываем конфигурацию, чтобы pg-pool вообще не мог выдать ошибку 28000
const poolConfig = {
    user: 'renabile_db_user',
    host: 'dpg-d8drpdmk1jcs739b1t60-a.frankfurt-postgres.render.com',
    database: 'renabile_db',
    password: 'Z6A4Hq5tNq639FAyWbJFaQjeUFQVYa78',
    port: 5432,
    ssl: {
        rejectUnauthorized: false // Позволяет успешно пройти Handshake на Render
    }
};

// Если в системе есть DATABASE_URL, принудительно чистим её, чтобы pg её не подхватил скрытно
if (process.env.DATABASE_URL) {
    delete process.env.DATABASE_URL;
}

const pool = new Pool(poolConfig);

// Проверка подключения и создание таблицы истории чатов
async function initDatabase() {
    try {
        const client = await pool.connect();
        console.log("[DB] Успешное подключение к PostgreSQL установлено!");

        // Создаем таблицу, если её нет
        await client.query(`
        CREATE TABLE IF NOT EXISTS chat_history (
            id SERIAL PRIMARY KEY,
            sender VARCHAR(50) NOT NULL,
                                                 receiver VARCHAR(50) NOT NULL,
                                                 message TEXT NOT NULL,
                                                 timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        `);
        console.log("[DB] Таблица истории чатов успешно проверена/создана.");
        client.release();
    } catch (err) {
        console.error("[DB] Критическая ошибка при работе с базой данных:");
        console.error(err.message);
    }
}

// Базовый роут для проверки работоспособности
app.get('/', (req, res) => {
    res.send({ status: "online", service: "Renabile Storage Engine" });
});

// Пример роута для сохранения сообщения (подставь свои пути, если они отличались)
app.post('/api/messages', async (req, res) => {
    const { sender, receiver, message } = req.body;
    try {
        await pool.query(
            'INSERT INTO chat_history (sender, receiver, message) VALUES ($1, $2, $3)',
                         [sender, receiver, message]
        );
        res.status(201).send({ success: true });
    } catch (err) {
        res.status(500).send({ error: err.message });
    }
});

// Запуск сервера на порту, который выдает Render
const PORT = process.env.PORT || 10000;
app.listen(PORT, async () => {
    console.log(`[Node.js] Storage Engine успешно запущен на порту ${PORT}`);
    await initDatabase();
});
