const express = require('express');
const { Pool } = require('pg');
const app = express();

// Увеличиваем лимит, чтобы большие картинки аватарок пролетали без проблем
app.use(express.json({ limit: '10mb' }));

// Безопасно подтягиваем DATABASE_URL из Environment Variables Рендера
const pool = new Pool({
    connectionString: process.env.DATABASE_URL ? process.env.DATABASE_URL.replace("jdbc:postgresql://", "postgres://") : "",
                      ssl: { rejectUnauthorized: false }
});

// Инициализация таблицы сообщений для сохранения истории чатов
pool.query(`
CREATE TABLE IF NOT EXISTS chat_history (
    id SERIAL PRIMARY KEY,
    sender VARCHAR(50),
                                         recipient VARCHAR(50),
                                         message TEXT,
                                         ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
`).then(() => console.log("[DB] Таблица истории чатов проверена/создана."));

// 1. ПОЛУЧЕНИЕ АВАТАРКИ
app.get('/api/avatar/:username', async (req, res) => {
    try {
        const result = await pool.query('SELECT avatar_base64 FROM users WHERE username = $1', [req.params.username]);
        if (result.rows.length > 0 && result.rows[0].avatar_base64) {
            // Декодируем base64 обратно в бинарный формат изображения для JavaFX Image
            const imgBuffer = Buffer.from(result.rows[0].avatar_base64, 'base64');
            res.writeHead(200, {
                'Content-Type': 'image/png',
                'Content-Length': imgBuffer.length
            });
            res.end(imgBuffer);
        } else {
            // Если аватарки нет — возвращаем прозрачный пиксель 1x1, чтобы клиент не падал
            const dummy = Buffer.from("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=", 'base64');
            res.writeHead(200, { 'Content-Type': 'image/png' });
            res.end(dummy);
        }
    } catch (err) {
        res.status(500).send(err.message);
    }
});

// 2. ЗАГРУЗКА/ОБНОВЛЕНИЕ АВАТАРКИ (HTTP POST вместо перегрузки веб-сокета)
app.post('/api/avatar/upload', async (req, res) => {
    const { username, avatar } = req.body;
    try {
        await pool.query('UPDATE users SET avatar_base64 = $1 WHERE username = $2', [avatar, username]);
        res.json({ status: "OK", message: "Аватарка успешно обновлена!" });
    } catch (err) {
        res.status(500).json({ status: "ERROR", error: err.message });
    }
});

// 3. ПОЛУЧЕНИЕ ИСТОРИИ ПЕРЕПИСКИ
app.get('/api/history', async (req, res) => {
    const { u1, u2 } = req.query; // u2 может быть 'GLOBAL' или кодом друга
    try {
        let result;
        if (u2 === 'GLOBAL') {
            result = await pool.query(
                'SELECT sender, message as text FROM chat_history WHERE recipient = \'GLOBAL\' ORDER BY ts ASC LIMIT 50'
            );
        } else {
            result = await pool.query(
                'SELECT sender, message as text FROM chat_history WHERE (sender = $1 AND recipient = $2) OR (sender = $2 AND recipient = $1) ORDER BY ts ASC LIMIT 50',
                                      [u1, u2]
            );
        }
        res.json(result.rows);
    } catch (err) {
        res.status(500).send(err.message);
    }
});

// Запуск сервера на порту, который выделит Render
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`[Node.js] Storage Engine успешно запущен на порту ${PORT}`));
