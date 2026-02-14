#!/bin/bash
# Запуск FastAPI (API для Mini App) в фоне
cd /app
python -m uvicorn miniapp.api:app --host 0.0.0.0 --port 8000 &

# Ждём пока API поднимется
sleep 3

# Запуск Telegram бота
python main.py
