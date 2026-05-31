const express = require('express');
const { Pool } = require('pg');

const app = express();
app.use(express.json());

// Изолированная конфигурация пула — никакой магии, только прямые данные
const poolConfig = {
    user: 'renabile_db_user',
    host: 'dpg-d8drpdmk1jcs739b1t60-a.frankfurt-postgres.render.com',
    database: 'renabile_db',
    password: 'Z6A4Hq5tNq639FAyWbJFaQjeUFQVYa78',
    port: 5432,
    ssl: {
        rejectUnauthorized: false // Гарантирует успешный обход проверок сертификатов Render
    },
    connectionTimeoutMillis: 10000
};

// Принудительно удаляем системную переменную, чтобы Node.js не пытался парсить её скрытно
if (process.env.DATABASE_URL) {
    delete process.env.DATABASE_URL;
}

const pool = new Pool(poolConfig);

// Безопасный запуск БД с отловом ошибок коннекта
async function initDatabase() {
    try {
        const client = await pool.connect();
        console.log("[DB] Успешное подключение к PostgreSQL установлено!");

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
        console.error("[DB] Критическая ошибка пула базы данных:");
        console.error(err.message);
    }
}

// Проверка статуса сервиса
app.get('/', (req, res) => {
    res.send({ status: "online", service: "Renabile Storage Engine" });
});

// API для добавления сообщений в историю
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

const PORT = process.env.PORT || 10000;
app.listen(PORT, async () => {
    console.log(`[Node.js] Storage Engine успешно запущен на порту ${PORT}`);
    await initDatabase();
});
