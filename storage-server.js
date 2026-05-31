const express = require('express');
const { Pool } = require('pg');
const app = express();

app.use(express.json({ limit: '10mb' }));

const pool = new Pool({
    connectionString: "jdbc:postgresql://dpg-d8drpdmk1jcs739b1t60-a.frankfurt-postgres.render.com:5432/renabile_db".replace("jdbc:postgresql://", "postgres://"),
                      ssl: { rejectUnauthorized: false }
});

// Инициализация таблицы сообщений для сохранения истории
pool.query(`
CREATE TABLE IF NOT EXISTS chat_history (
    id SERIAL PRIMARY KEY,
    sender VARCHAR(50),
                                         recipient VARCHAR(50),
                                         message TEXT,
                                         ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
`);

// Загрузка аватарки (Быстро и без падения WebSocket)
app.get('/api/avatar/:username', async (req, res) => {
    try {
        const result = await pool.query('SELECT avatar_base64 FROM users WHERE username = $1', [req.params.username]);
        res.json({ avatar: result.rows[0]?.avatar_base64 || "" });
    } catch (err) { res.status(500).send(err.message); }
});

// Загрузка истории чата
app.get('/api/history', async (req, res) => {
    const { u1, u2 } = req.query; // u2 может быть 'GLOBAL'
    try {
        let result;
        if (u2 === 'GLOBAL') {
            result = await pool.query('SELECT sender, message as text FROM chat_history WHERE recipient = \'GLOBAL\' ORDER BY ts ASC LIMIT 50');
        } else {
            result = await pool.query(
                'SELECT sender, message as text FROM chat_history WHERE (sender = $1 AND recipient = $2) OR (sender = $2 AND recipient = $1) ORDER BY ts ASC LIMIT 50',
                                      [u1, u2]
            );
        }
        res.json(result.rows);
    } catch (err) { res.status(500).send(err.message); }
});

app.listen(process.env.PORT || 3000, () => console.log('Node.js Storage Engine запущен!'));
