"""
Единая база данных: кэш, история, результаты, избранное.
"""

import aiosqlite
import json
import time
import logging

DB_NAME = "bot_database.db"
CACHE_TTL = 3600

class Database:
    def __init__(self, db_path=DB_NAME):
        self.db_path = db_path

    async def init_db(self):
        """Создаёт все таблицы."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS search_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    query TEXT,
                    timestamp REAL
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    query TEXT PRIMARY KEY,
                    data TEXT,
                    created_at REAL
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    search_id INTEGER,
                    source TEXT,
                    title TEXT,
                    price_int INTEGER,
                    link TEXT,
                    image_url TEXT,
                    created_at REAL
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS favorites (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    source TEXT,
                    title TEXT,
                    price_int INTEGER,
                    link TEXT,
                    image_url TEXT,
                    created_at REAL
                )
            """)
            await db.commit()
            logging.info("БД инициализирована.")

    # --- История ---
    async def add_history(self, user_id: int, query: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO search_history (user_id, query, timestamp) VALUES (?, ?, ?)",
                (user_id, query, time.time())
            )
            await db.commit()

    async def get_history(self, user_id: int, limit: int = 20):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT id, query, timestamp FROM search_history WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?",
                (user_id, limit)
            ) as cursor:
                return [dict(row) async for row in cursor]

    # --- Кэш ---
    async def get_cache(self, query: str):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT data, created_at FROM cache WHERE query = ?", (query,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    data_json, created_at = row
                    if time.time() - created_at < CACHE_TTL:
                        return json.loads(data_json)
                    await db.execute("DELETE FROM cache WHERE query = ?", (query,))
                    await db.commit()
        return None

    async def save_cache(self, query: str, results: list):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO cache (query, data, created_at) VALUES (?, ?, ?)",
                (query, json.dumps(results, ensure_ascii=False), time.time())
            )
            await db.commit()

    # --- Результаты ---
    async def save_results(self, search_id: int, results: list):
        async with aiosqlite.connect(self.db_path) as db:
            for r in results:
                await db.execute(
                    "INSERT INTO results (search_id, source, title, price_int, link, image_url, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (search_id, r.get('source', ''), r.get('title', ''), r.get('price_int', 0),
                     r.get('link', ''), r.get('image_url', ''), time.time())
                )
            await db.commit()

    async def get_results(self, search_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM results WHERE search_id = ? ORDER BY price_int ASC", (search_id,)
            ) as cursor:
                return [dict(row) async for row in cursor]

    # --- Избранное ---
    async def add_favorite(self, user_id: int, source: str, title: str, price_int: int, link: str, image_url: str = ''):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO favorites (user_id, source, title, price_int, link, image_url, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (user_id, source, title, price_int, link, image_url, time.time())
            )
            await db.commit()

    async def remove_favorite(self, fav_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM favorites WHERE id = ?", (fav_id,))
            await db.commit()

    async def get_favorites(self, user_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM favorites WHERE user_id = ? ORDER BY created_at DESC", (user_id,)
            ) as cursor:
                return [dict(row) async for row in cursor]


db = Database()
