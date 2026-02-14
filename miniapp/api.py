"""
FastAPI Backend для Telegram Mini App.
Использует parsers.search_all_sites() и database.db из корня проекта.
"""

import asyncio
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Корень проекта для импортов
sys.path.insert(0, str(Path(__file__).parent.parent))

# Загружаем переменные окружения из корня проекта
env_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(env_path)

from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from pydantic import BaseModel
import re

from database import db
from logic import filter_results
from parsers import search_all_sites

app = FastAPI(title="Parts Search Mini App")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

class NgrokMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["ngrok-skip-browser-warning"] = "true"
        return response

app.add_middleware(NgrokMiddleware)

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


class SearchRequest(BaseModel):
    query: str
    user_id: int = 0
    sort_by: str = "price_asc"
    force_refresh: bool = False

class FavoriteRequest(BaseModel):
    user_id: int
    source: str
    title: str
    price_int: int = 0
    link: str = ""
    image_url: str = ""


@app.on_event("startup")
async def startup():
    await db.init_db()


@app.get("/", response_class=HTMLResponse)
async def root():
    html_path = TEMPLATES_DIR / "index.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.post("/api/search")
async def api_search(request: SearchRequest):
    query = request.query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Пустой запрос")

    await db.add_history(request.user_id, query)

    # Кэш (пропускаем при force_refresh)
    # Извлекаем артикул для альтернативной оценки релевантности
    from parsers import _extract_article_query
    article_q = _extract_article_query(query) or ''

    if not request.force_refresh:
        cached = await db.get_cache(query)
        if cached:
            filtered = filter_results(cached, query, request.sort_by, article_query=article_q)
            return {"query": query, "count": len(filtered), "results": filtered, "cached": True}

    # Парсинг
    results = await search_all_sites(query)
    if results:
        await db.save_cache(query, results)

    filtered = filter_results(results, query, request.sort_by, article_query=article_q)
    return {"query": query, "count": len(filtered), "results": filtered, "cached": False}


@app.post("/api/search/photo")
async def search_by_photo(file: UploadFile = File(...), user_id: int = Form(0)):
    contents = await file.read()
    article = None
    try:
        import pytesseract
        from PIL import Image
        import io
        image = Image.open(io.BytesIO(contents))
        text = pytesseract.image_to_string(image, lang='eng+rus')
        patterns = re.findall(r'[A-Za-z0-9]{5,15}', text)
        if patterns:
            article = patterns[0]
    except ImportError:
        pass

    if not article:
        return JSONResponse(status_code=400, content={"error": "Не удалось распознать артикул. Введите вручную."})

    return await api_search(SearchRequest(query=article, user_id=user_id))


@app.get("/api/history/{user_id}")
async def api_history(user_id: int):
    return {"history": await db.get_history(user_id)}


@app.post("/api/favorites")
async def api_add_favorite(request: FavoriteRequest):
    await db.add_favorite(request.user_id, request.source, request.title, request.price_int, request.link, request.image_url)
    return {"status": "added"}


@app.get("/api/favorites/{user_id}")
async def api_get_favorites(user_id: int):
    return {"favorites": await db.get_favorites(user_id)}


@app.delete("/api/favorites/{fav_id}")
async def api_remove_favorite(fav_id: int):
    await db.remove_favorite(fav_id)
    return {"status": "removed"}


@app.get("/health")
async def health():
    return {"status": "ok", "parsers": 10}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
