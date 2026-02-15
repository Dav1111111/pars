"""
Единый модуль парсеров автозапчастей.
Все парсеры здесь. Один формат результата. Один агрегатор.
"""

import asyncio
import aiohttp
import json
import logging
import os
import random
import re
import sys
from bs4 import BeautifulSoup

from logic import clean_price

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# User-Agent ротация
# =============================================================================

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
]

def _random_ua():
    return random.choice(USER_AGENTS)

def _random_headers():
    return {'User-Agent': _random_ua(), 'Accept-Language': 'ru-RU,ru;q=0.9'}

TIMEOUT = aiohttp.ClientTimeout(total=25)


# =============================================================================
# Прокси-ротация
# =============================================================================

class ProxyRotator:
    """Ротация прокси. Поддерживает HTTP и SOCKS5."""
    def __init__(self):
        self._proxies = []
        self._index = 0
        self._local_forwarder_port = None
        self._forwarder_proc = None
        self._load_proxies()

    def _load_proxies(self):
        # Из переменной окружения PROXIES (через запятую)
        raw = os.getenv('PROXIES', '')
        if raw:
            self._proxies = [p.strip() for p in raw.split(',') if p.strip()]
            logger.info(f"[Proxy] Загружено {len(self._proxies)} прокси")
        else:
            logger.info("[Proxy] Прокси не заданы, работаем напрямую")

    def get(self):
        """Возвращает следующий прокси или None."""
        if not self._proxies:
            return None
        proxy = self._proxies[self._index % len(self._proxies)]
        self._index += 1
        return proxy

    def _needs_forwarder(self, proxy_url):
        """Chromium не поддерживает SOCKS5 с авторизацией — нужен форвардер."""
        from urllib.parse import urlparse
        parsed = urlparse(proxy_url)
        return parsed.scheme in ('socks5', 'socks4') and parsed.username

    def _ensure_forwarder(self, proxy_url):
        """Запускает локальный HTTP CONNECT форвардер для Playwright."""
        if self._local_forwarder_port:
            return self._local_forwarder_port
        import subprocess, socket
        # Находим свободный порт
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]
        try:
            forwarder_script = os.path.join(os.path.dirname(__file__), 'proxy_forwarder.py')
            self._forwarder_proc = subprocess.Popen(
                [sys.executable, forwarder_script, str(port), proxy_url],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            )
            # Ждём сигнал готовности
            import time
            for _ in range(10):
                line = self._forwarder_proc.stdout.readline().decode().strip()
                if f'FORWARDER_READY:{port}' in line:
                    break
                time.sleep(0.3)
            self._local_forwarder_port = port
            logger.info(f"[Proxy] Форвардер запущен на 127.0.0.1:{port}")
            return port
        except Exception as e:
            logger.error(f"[Proxy] Не удалось запустить форвардер: {e}")
            return None

    def get_playwright(self):
        """Формат прокси для Playwright. SOCKS5+auth → локальный HTTP форвардер."""
        proxy = self.get()
        if not proxy:
            return None
        if self._needs_forwarder(proxy):
            port = self._ensure_forwarder(proxy)
            if port:
                return {'server': f'http://127.0.0.1:{port}'}
            return None
        from urllib.parse import urlparse
        parsed = urlparse(proxy)
        result = {'server': f'{parsed.scheme}://{parsed.hostname}:{parsed.port}'}
        if parsed.username:
            result['username'] = parsed.username
        if parsed.password:
            result['password'] = parsed.password
        return result

    @property
    def available(self):
        return len(self._proxies) > 0

proxy_rotator = ProxyRotator()


async def _human_page(playwright, warmup_url, target_url, scroll=True, browser=None):
    """
    Имитация человека + прогрев. Если browser передан — использует его (shared).
    Иначе создаёт новый.
    """
    own_browser = browser is None
    if own_browser:
        launch_args = {
            'headless': True,
            'args': ['--disable-blink-features=AutomationControlled', '--no-sandbox'],
        }
        pw_proxy = proxy_rotator.get_playwright()
        if pw_proxy:
            launch_args['proxy'] = pw_proxy
        browser = await playwright.chromium.launch(**launch_args)

    ua = _random_ua()
    ctx = await browser.new_context(
        user_agent=ua,
        viewport={'width': random.choice([1280, 1366, 1440, 1536]),
                  'height': random.choice([720, 768, 900])},
        locale='ru-RU',
    )
    page = await ctx.new_page()
    try:
        from playwright_stealth import stealth_async
        await stealth_async(page)
    except ImportError:
        await ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    # Прогрев
    try:
        await page.goto(warmup_url, timeout=20000, wait_until='domcontentloaded')
        await page.wait_for_timeout(random.randint(1500, 3000))
        await page.mouse.move(random.randint(200, 600), random.randint(200, 400))
        await page.mouse.wheel(0, random.randint(300, 700))
        await page.wait_for_timeout(random.randint(1000, 2000))
    except Exception:
        pass

    # Целевая страница
    await page.goto(target_url, timeout=30000, wait_until='domcontentloaded')
    await page.wait_for_timeout(random.randint(3000, 5000))

    if scroll:
        await page.mouse.wheel(0, random.randint(500, 1500))
        await page.wait_for_timeout(random.randint(1500, 3000))

    html = await page.content()
    await ctx.close()
    if own_browser:
        await browser.close()
    return html


def _result(source, title, price_int, link, image_url=''):
    """Единый формат результата."""
    return {
        'source': source,
        'title': str(title)[:100],
        'price': f"{price_int} ₽" if price_int else "По ссылке",
        'price_int': price_int,
        'link': link,
        'image_url': image_url,
    }


# =============================================================================
# HTTP парсеры (aiohttp, быстрые)
# =============================================================================

async def _fetch(session, url, headers=None):
    """GET запрос, возвращает HTML или None. Прокси — на уровне сессии (ProxyConnector)."""
    try:
        async with session.get(url, headers=headers or _random_headers(), timeout=TIMEOUT, ssl=False) as r:
            if r.status == 200:
                return await r.text()
            logger.warning(f"HTTP {r.status}: {url[:50]}")
    except Exception as e:
        logger.error(f"Fetch error: {e}")
    return None


async def parse_partkom(session, query):
    html = await _fetch(session, f'https://part-kom.ru/search?query={query}')
    if not html:
        return []
    soup = BeautifulSoup(html, 'html.parser')
    results = []
    seen_links = set()
    for a in soup.find_all('a', href=lambda x: x and '/products/' in str(x))[:40]:
        raw_title = a.get_text(strip=True)
        # Пропуск навигационных ссылок
        if raw_title in ('Перейти', 'Подробнее', 'Купить') or len(raw_title) < 8:
            continue
        link = a.get('href', '')
        if not link.startswith('http'):
            link = 'https://part-kom.ru' + link
        # Дедупликация по ссылке (каждый товар имеет 2 <a>: название и "Перейти")
        if link in seen_links:
            continue
        seen_links.add(link)

        # Очистка title: убираем "BRAND · ARTICLE" префикс из <span> и "Под заказ"
        title = raw_title
        # Находим <span> с "BRAND · ARTICLE" внутри <a> и удаляем его текст из title
        for sp in a.find_all('span'):
            sp_text = sp.get_text(strip=True)
            if '·' in sp_text and len(sp_text) < 40:
                title = title.replace(sp_text, '', 1).strip()
        # Убираем "Под заказ" и trailing артикул (7+ цифр в конце строки)
        title = re.sub(r'\s*\d{7,}\s*$', '', title).strip()
        title = re.sub(r'\s*Под заказ\s*$', '', title).strip()
        title = re.sub(r'\s*\d{7,}\s*$', '', title).strip()
        if len(title) < 6:
            title = raw_title  # fallback если очистка убрала всё

        # Ищем цену: поднимаемся по дереву до контейнера с ₽
        price = 0
        img = ''
        el = a
        for _ in range(5):
            el = el.parent
            if not el:
                break
            text = el.get_text()
            if '₽' in text:
                ptag = el.find(string=lambda t: t and '₽' in t)
                if ptag:
                    price = clean_price(ptag)
                break
            # Картинка
            if not img:
                iel = el.find('img')
                if iel:
                    isrc = iel.get('src', '') or iel.get('data-src', '')
                    if isrc and 'no-image' not in isrc:
                        img = isrc if isrc.startswith('http') else 'https://part-kom.ru' + isrc
        results.append(_result('Part-Kom', title, price, link, img))
        if len(results) >= 20:
            break
    return results


async def parse_parterra(session, query):
    """Parterra — SPA, перехватываем XHR с данными."""
    if not PLAYWRIGHT_AVAILABLE:
        return []
    from urllib.parse import quote
    results = []
    api_data = []
    try:
        async with async_playwright() as p:
            launch_args = {
                'headless': True,
                'args': ['--disable-blink-features=AutomationControlled', '--no-sandbox'],
            }
            pw_proxy = proxy_rotator.get_playwright()
            if pw_proxy:
                launch_args['proxy'] = pw_proxy
            browser = await p.chromium.launch(**launch_args)
            ctx = await browser.new_context(
                user_agent=_random_ua(),
                viewport={'width': 1440, 'height': 900},
                locale='ru-RU',
            )
            page = await ctx.new_page()
            try:
                from playwright_stealth import stealth_async
                await stealth_async(page)
            except ImportError:
                await ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            # Перехватываем API-ответы
            async def handle_response(response):
                try:
                    url = response.url
                    if '/api/' in url and ('search' in url or 'catalog' in url or 'product' in url):
                        if response.status == 200:
                            body = await response.json()
                            if isinstance(body, dict):
                                items = body.get('items', []) or body.get('products', []) or body.get('data', [])
                                if items:
                                    api_data.extend(items)
                            elif isinstance(body, list):
                                api_data.extend(body)
                except Exception:
                    pass

            page.on('response', handle_response)

            await page.goto('https://parterra.ru/', timeout=20000, wait_until='domcontentloaded')
            await page.wait_for_timeout(random.randint(1500, 2500))
            await page.goto(
                f'https://parterra.ru/search/?query={quote(query)}',
                timeout=30000, wait_until='domcontentloaded',
            )
            await page.wait_for_timeout(random.randint(5000, 8000))
            await page.mouse.wheel(0, random.randint(300, 700))
            await page.wait_for_timeout(random.randint(2000, 3000))

            # Пробуем парсить из перехваченных API-данных
            for item in api_data[:20]:
                title = item.get('name', '') or item.get('title', '') or item.get('description', '')
                if not title or len(str(title)) < 5:
                    continue
                price = item.get('price', 0) or item.get('cost', 0)
                if isinstance(price, str):
                    price = clean_price(price)
                link = item.get('url', '') or item.get('link', '')
                if link and not link.startswith('http'):
                    link = 'https://parterra.ru' + link
                img = item.get('image', '') or item.get('img', '') or item.get('photo', '')
                if img and not img.startswith('http'):
                    img = 'https://parterra.ru' + img
                results.append(_result('Parterra', str(title), int(price) if price else 0, link, img))

            # Fallback: парсим HTML если API не вернул данных
            if not results:
                soup = BeautifulSoup(await page.content(), 'html.parser')
                for a in soup.select('a[href*="/product/"], a[href*="/catalog/"]')[:20]:
                    title = a.get_text(strip=True)
                    if len(title) < 5:
                        continue
                    link = a.get('href', '')
                    if link and not link.startswith('http'):
                        link = 'https://parterra.ru' + link
                    parent = a.find_parent('div', class_=True)
                    price = 0
                    if parent:
                        pm = re.search(r'(\d[\d\s]*)\s*₽', parent.get_text())
                        if pm:
                            price = clean_price(pm.group(1))
                    results.append(_result('Parterra', title, price, link, ''))

            await browser.close()
    except Exception as e:
        logger.error(f"[Parterra] {e}")
    return results


async def parse_koleso(session, query):
    """
    Koleso.ru: Next.js SSR — данные товаров в __NEXT_DATA__ JSON.
    Универсальный: автоматически определяет каталог (шины, масла, диски, АКБ).
    """
    from urllib.parse import quote
    results = []

    # Автоопределение категории по запросу
    q_lower = query.lower()
    # Маппинг: ключевые слова → URL каталога + ключ JSON + URL-путь товара
    _categories = [
        (['шин', 'tire', 'tyre', 'pirelli', 'michelin', 'continental', 'bridgestone',
          'nokian', 'hankook', 'goodyear', 'dunlop', 'yokohama', 'toyo', 'kumho',
          'r13', 'r14', 'r15', 'r16', 'r17', 'r18', 'r19', 'r20', 'r21', 'r22',
          'ice', 'winter', 'summer', 'hakkapeliitta'],
         '/catalog/tyres/', 'tyres'),
        (['масл', 'oil', '5w', '10w', '15w', '20w', '0w', 'синтетик', 'полусинтет',
          'motul', 'castrol', 'mobil', 'shell', 'zic', 'lukoil', 'лукойл'],
         '/catalog/oils/', 'oils'),
        (['диск', 'disk', 'wheel', 'литой', 'штамп'],
         '/catalog/disks/', 'disks'),
        (['аккумулятор', 'акб', 'battery'],
         '/catalog/akb/', 'akb'),
    ]

    # Определяем подходящие каталоги
    catalog_urls = []
    for keywords, cat_path, cat_key in _categories:
        if any(kw in q_lower for kw in keywords):
            catalog_urls.append((f'https://koleso.ru{cat_path}', cat_key))

    # Всегда пробуем поиск первым
    urls_to_try = [(f'https://koleso.ru/search/?query={quote(query)}', None)]
    urls_to_try.extend(catalog_urls)

    for url, expected_key in urls_to_try:
        html = await _fetch(session, url)
        if not html:
            continue
        soup = BeautifulSoup(html, 'html.parser')
        script = soup.select_one('script#__NEXT_DATA__')
        if not script or not script.string:
            continue
        try:
            data = json.loads(script.string)
            state = data.get('props', {}).get('pageProps', {}).get('initialState', {})
            home = state.get('home', {})

            # Собираем товары из всех доступных категорий или конкретной
            all_items = []
            if expected_key:
                all_items = home.get(expected_key, [])
            else:
                # Поиск: пробуем все ключи
                for key in ['tyres', 'oils', 'disks', 'akb', 'goods', 'products']:
                    all_items = home.get(key, [])
                    if all_items:
                        break
                if not all_items:
                    search_data = state.get('search', {})
                    all_items = search_data.get('items', []) or search_data.get('products', [])

            for item in all_items[:20]:
                # Универсальное извлечение: поддерживаем оба формата (lowercase и UPPERCASE)
                name = item.get('name', '') or item.get('NAME', '')
                brand = item.get('BRAND_NAME', '') or item.get('brand', '')
                if brand and name and brand.lower() not in name.lower():
                    name = f'{brand} {name}'
                if not name or len(name) < 3:
                    continue

                price = item.get('price', 0) or item.get('MIN_PRICE', 0) or 0
                if isinstance(price, str):
                    price = clean_price(price)

                slug = item.get('slug', '') or item.get('MODEL_URL', '')
                cat_path = '/catalog/tyres/' if item.get('TYPE') or expected_key == 'tyres' else '/catalog/oils/'
                link = f'https://koleso.ru{cat_path}{slug}/' if slug else ''

                img_file = item.get('image', '') or item.get('FileImg', '')
                img = f'https://koleso.ru/catalog-images/sources/{img_file}' if img_file else ''

                results.append(_result('Koleso', name, int(price), link, img))
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(f"[Koleso] JSON parse error: {e}")
        if results:
            break
    return results


async def parse_ruli(session, query):
    html = await _fetch(session, f'https://www.ruli.ru/search/?query={query}')
    if not html:
        return []
    soup = BeautifulSoup(html, 'html.parser')
    results = []
    for item in soup.select('.js-product-list-item')[:20]:
        t = item.select_one('a.prod-name')
        if not t:
            continue
        link = t.get('href', '')
        if not link.startswith('http'):
            link = 'https://www.ruli.ru' + link
        m = re.search(r'([\d\s]+)₽', item.get_text())
        price = clean_price(m.group(1)) if m else 0
        iel = item.select_one('img')
        img = ''
        if iel:
            img = iel.get('data-src', '') or iel.get('data-original', '') or iel.get('src', '')
            if img and img.startswith('data:'):
                img = ''
            if img and 'loading' in img:
                img = ''
            if img and not img.startswith('http'):
                img = 'https://www.ruli.ru' + img
        results.append(_result('Ruli', t.get_text(strip=True), price, link, img))
    return results


async def parse_autopiter(session, query):
    if not PLAYWRIGHT_AVAILABLE:
        return []
    from urllib.parse import quote
    results = []
    try:
        async with async_playwright() as p:
            html = await _human_page(
                p,
                warmup_url='https://autopiter.ru/',
                target_url=f'https://autopiter.ru/goods?search={quote(query)}',
            )
            soup = BeautifulSoup(html, 'html.parser')

            for a in soup.select('a[href*="/goods/"]')[:20]:
                title = a.get_text(strip=True)
                if len(title) < 5:
                    continue
                link = a.get('href', '')
                if not link.startswith('http'):
                    link = 'https://autopiter.ru' + link
                parent = a.find_parent('div') or a.parent
                m = re.search(r'([\d\s]+)₽', parent.get_text()) if parent else None
                price = clean_price(m.group(1)) if m else 0
                img = ''
                if parent:
                    iel = parent.find('img')
                    if iel:
                        img = iel.get('src', '') or iel.get('data-src', '')
                        if img and not img.startswith('http'):
                            img = 'https://autopiter.ru' + img
                results.append(_result('Autopiter', title, price, link, img))
    except Exception as e:
        logger.error(f"[Autopiter] {e}")
    return results


async def parse_bibinet(session, query):
    """Bibinet — SPA, используем поиск через ввод в поле + перехват XHR."""
    if not PLAYWRIGHT_AVAILABLE:
        return []
    from urllib.parse import quote
    results = []
    api_data = []
    try:
        async with async_playwright() as p:
            launch_args = {
                'headless': True,
                'args': ['--disable-blink-features=AutomationControlled', '--no-sandbox'],
            }
            pw_proxy = proxy_rotator.get_playwright()
            if pw_proxy:
                launch_args['proxy'] = pw_proxy
            browser = await p.chromium.launch(**launch_args)
            ctx = await browser.new_context(
                user_agent=_random_ua(),
                viewport={'width': 1440, 'height': 900},
                locale='ru-RU',
            )
            page = await ctx.new_page()
            try:
                from playwright_stealth import stealth_async
                await stealth_async(page)
            except ImportError:
                await ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            # Перехватываем API-ответы с данными товаров
            async def handle_response(response):
                try:
                    url = response.url
                    if response.status == 200 and ('api' in url or 'search' in url) and 'text/html' not in (response.headers.get('content-type', '')):
                        body = await response.json()
                        if isinstance(body, dict):
                            items = body.get('items', []) or body.get('products', []) or body.get('results', []) or body.get('data', [])
                            if items and isinstance(items, list):
                                api_data.extend(items)
                        elif isinstance(body, list) and len(body) > 0:
                            api_data.extend(body)
                except Exception:
                    pass

            page.on('response', handle_response)

            await page.goto('https://bibinet.ru/', timeout=20000, wait_until='domcontentloaded')
            await page.wait_for_timeout(random.randint(2000, 3000))

            # Ищем поле поиска и вводим запрос
            inp = await page.query_selector('input[type="search"], input[type="text"], input[placeholder*="поиск" i], input[placeholder*="артикул" i], input[name*="search" i]')
            if inp:
                await inp.click()
                await page.wait_for_timeout(500)
                await inp.type(query, delay=random.randint(50, 100))
                await page.wait_for_timeout(random.randint(1000, 2000))
                await page.keyboard.press('Enter')
                await page.wait_for_timeout(random.randint(5000, 8000))
            else:
                await page.goto(f'https://bibinet.ru/search?query={quote(query)}', timeout=30000, wait_until='domcontentloaded')
                await page.wait_for_timeout(random.randint(5000, 8000))

            await page.mouse.wheel(0, random.randint(300, 700))
            await page.wait_for_timeout(random.randint(1500, 2500))

            # Парсим из перехваченных API-данных
            for item in api_data[:20]:
                title = item.get('name', '') or item.get('title', '') or item.get('description', '')
                if not title or len(str(title)) < 5:
                    continue
                price = item.get('price', 0) or item.get('cost', 0)
                if isinstance(price, str):
                    price = clean_price(price)
                link = item.get('url', '') or item.get('link', '')
                if link and not link.startswith('http'):
                    link = 'https://bibinet.ru' + link
                img = item.get('image', '') or item.get('img', '') or item.get('photo', '')
                if img and not img.startswith('http'):
                    img = 'https://bibinet.ru' + img
                results.append(_result('Bibinet', str(title), int(price) if price else 0, link, img))

            # Fallback: парсим HTML
            if not results:
                soup = BeautifulSoup(await page.content(), 'html.parser')
                seen = set()
                for a in soup.select('a[href*="/part/"], a[href*="/product/"], a[href*="/detail/"]')[:20]:
                    title = a.get_text(strip=True)
                    if len(title) < 8 or title in seen:
                        continue
                    seen.add(title)
                    link = a.get('href', '')
                    if not link.startswith('http'):
                        link = 'https://bibinet.ru' + link
                    # Пропускаем навигационные ссылки
                    if link.rstrip('/') in ('https://bibinet.ru/part', 'https://bibinet.ru/product'):
                        continue
                    parent = a.find_parent('div', class_=True)
                    price = 0
                    if parent:
                        pm = re.search(r'(\d[\d\s]*)\s*₽', parent.get_text())
                        if pm:
                            price = clean_price(pm.group(1))
                    results.append(_result('Bibinet', title, price, link, ''))

            await browser.close()
    except Exception as e:
        logger.error(f"[Bibinet] {e}")
    return results


# =============================================================================
# Wildberries (JSON API, без Playwright)
# =============================================================================

def _wb_image(pid):
    vol = pid // 100000
    part = pid // 1000
    baskets = [
        (144, '01'), (288, '02'), (432, '03'), (720, '04'), (1008, '05'),
        (1296, '06'), (1584, '07'), (1872, '08'), (2160, '09'), (2448, '10'),
        (2736, '11'), (3024, '12'), (3312, '13'), (3600, '14'), (3888, '15'),
        (4176, '16'),
    ]
    basket = '17'
    for threshold, b in baskets:
        if vol < threshold:
            basket = b
            break
    return f'https://basket-{basket}.wbbasket.ru/vol{vol}/part{part}/{pid}/images/c516x688/1.webp'


async def parse_wildberries(_session, query):
    import random as _rnd
    from urllib.parse import quote
    encoded = quote(query)
    # Случайная задержка 1-3с чтобы не триггерить rate-limit
    await asyncio.sleep(_rnd.uniform(1.0, 3.0))

    ua = _random_ua()
    dest = _rnd.choice(['-1257786', '-1221148', '-364763', '-446085'])
    url = (
        'https://search.wb.ru/exactmatch/ru/common/v9/search'
        f'?ab_testing=false&appType=1&curr=rub&dest={dest}&query={encoded}'
        '&resultset=catalog&sort=popular&spp=30&suppressSpellcheck=false'
    )
    headers = {
        'User-Agent': ua,
        'Accept': '*/*',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Origin': 'https://www.wildberries.ru',
        'Referer': f'https://www.wildberries.ru/catalog/0/search.aspx?search={encoded}',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'x-queryid': 'qid' + str(_rnd.randint(100000000, 999999999)),
    }

    # Стратегия: прокси → напрямую. Макс 2 попытки чтоб не тормозить.
    _proxy_url = proxy_rotator.get()
    strategies = []
    if _proxy_url:
        strategies.append(_proxy_url)  # через прокси
    strategies.append(None)            # напрямую

    for i, proxy in enumerate(strategies):
        try:
            session_kwargs = {}
            request_kwargs = {'headers': headers, 'ssl': False, 'timeout': TIMEOUT}
            if proxy and 'socks' in proxy:
                from aiohttp_socks import ProxyConnector
                session_kwargs['connector'] = ProxyConnector.from_url(proxy)
            elif proxy:
                request_kwargs['proxy'] = proxy

            via = 'proxy' if proxy else 'direct'
            async with aiohttp.ClientSession(**session_kwargs) as wb_session:
                async with wb_session.get(url, **request_kwargs) as resp:
                    if resp.status == 429:
                        logger.warning(f"[WB] 429 ({via}), пробуем следующий...")
                        await asyncio.sleep(_rnd.uniform(1, 3))
                        headers['User-Agent'] = _random_ua()
                        headers['x-queryid'] = 'qid' + str(_rnd.randint(100000000, 999999999))
                        continue
                    if resp.status != 200:
                        logger.warning(f"[WB] HTTP {resp.status} ({via})")
                        return []
                    raw = await resp.read()

            data = json.loads(raw.decode('utf-8'))
            products = data.get('data', {}).get('products', []) or data.get('products', [])
            results = []
            for p in products[:20]:
                pid = p.get('id', 0)
                price = 0
                sizes = p.get('sizes', [])
                if sizes:
                    pi = sizes[0].get('price', {})
                    price = pi.get('product', 0) // 100
                if not price:
                    price = p.get('salePriceU', 0) // 100
                results.append(_result(
                    'Wildberries',
                    p.get('name', ''),
                    price,
                    f'https://www.wildberries.ru/catalog/{pid}/detail.aspx',
                    _wb_image(pid),
                ))
            return results
        except Exception as e:
            logger.error(f"[WB] {e}")
    logger.warning("[WB] Все попытки 429 — нужен резидентный прокси")
    return []


# =============================================================================
# Ozon (Playwright + прокси + прогрев)
# =============================================================================

async def parse_ozon(session, query):
    if not PLAYWRIGHT_AVAILABLE:
        logger.warning("[Ozon] Playwright не установлен")
        return []
    if not proxy_rotator.available:
        logger.warning("[Ozon] Пропуск — нет прокси, будет IP-бан")
        return []
    from urllib.parse import quote
    results = []
    try:
        async with async_playwright() as p:
            html = await _human_page(
                p,
                warmup_url='https://www.ozon.ru/',
                target_url=f'https://www.ozon.ru/search/?text={quote(query)}&from_global=true',
            )

            if 'Доступ ограничен' in html or 'captcha' in html.lower():
                logger.warning("[Ozon] IP заблокирован / капча")
                return []

            soup = BeautifulSoup(html, 'html.parser')
            seen = set()
            for link in soup.select('a[href*="/product/"]'):
                href = link.get('href', '')
                text = link.get_text(strip=True)
                if not text or len(text) < 6 or text in seen:
                    continue
                seen.add(text)
                parent = link.find_parent('div', class_=True)
                price = 0
                img = ''
                if parent:
                    pm = re.search(r'([\d\s]+)\s*₽', parent.get_text())
                    if pm:
                        price = clean_price(pm.group(1))
                    iel = parent.select_one('img')
                    if iel:
                        img = iel.get('src', '')
                full = 'https://www.ozon.ru' + href if href.startswith('/') else href
                results.append(_result('Ozon', text, price, full, img))
                if len(results) >= 20:
                    break
    except Exception as e:
        logger.error(f"[Ozon] {e}")
    return results


# =============================================================================
# Playwright парсеры (JS-рендеринг)
# =============================================================================

async def parse_kolesa_darom(session, query):
    if not PLAYWRIGHT_AVAILABLE:
        return []
    results = []
    try:
        async with async_playwright() as p:
            launch_args = {
                'headless': True,
                'args': ['--disable-blink-features=AutomationControlled', '--no-sandbox'],
            }
            pw_proxy = proxy_rotator.get_playwright()
            if pw_proxy:
                launch_args['proxy'] = pw_proxy
            browser = await p.chromium.launch(**launch_args)
            ua = _random_ua()
            ctx = await browser.new_context(
                user_agent=ua,
                viewport={'width': 1440, 'height': 900},
                locale='ru-RU',
            )
            page = await ctx.new_page()
            try:
                from playwright_stealth import stealth_async
                await stealth_async(page)
            except ImportError:
                await ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            await page.goto('https://www.kolesa-darom.ru/', timeout=20000, wait_until='domcontentloaded')
            await page.wait_for_timeout(random.randint(1500, 2500))
            await page.goto(
                f'https://www.kolesa-darom.ru/search/?q={query}',
                timeout=30000, wait_until='domcontentloaded',
            )
            await page.wait_for_timeout(random.randint(6000, 8000))
            await page.mouse.wheel(0, random.randint(500, 1000))
            await page.wait_for_timeout(random.randint(2000, 3000))

            soup = BeautifulSoup(await page.content(), 'html.parser')
            await ctx.close()
            await browser.close()

            # 1) digi-product карточки
            for card in soup.select('.digi-product')[:20]:
                a = card.select_one('a[href]')
                if not a:
                    continue
                href = a.get('href', '')
                if not href:
                    continue
                if not href.startswith('http'):
                    href = 'https://www.kolesa-darom.ru' + href
                # Цена: пробуем несколько селекторов
                price = 0
                for price_sel in ['[class*="price-variant_actual"]', '[class*="price_actual"]', '[class*="Price"]', '[class*="price"]']:
                    price_el = card.select_one(price_sel)
                    if price_el:
                        price = clean_price(price_el.get_text())
                        if price > 0:
                            break
                # Если не нашли через селекторы — ищем ₽ в тексте карточки
                if not price:
                    pm = re.search(r'(\d[\d\s]*)\s*₽', card.get_text())
                    if pm:
                        price = clean_price(pm.group(1))
                img_el = card.select_one('img')
                img = (img_el.get('src', '') or img_el.get('data-src', '')) if img_el else ''
                if img and not img.startswith('http'):
                    img = 'https://www.kolesa-darom.ru' + img
                title = img_el.get('alt', '') if img_el else a.get_text(strip=True)
                if not title or len(title) < 3:
                    title = a.get_text(strip=True)
                if title:
                    results.append(_result('Колёса Даром', title, price, href, img))

            # 2) product-card карточки (fallback)
            if len(results) < 5:
                for item in soup.find_all('div', class_='product-card')[:20]:
                    img_el = item.find('img')
                    title = img_el.get('alt', '') if img_el else ''
                    if not title:
                        continue
                    a = item.find('a')
                    link = a.get('href', '') if a else ''
                    if link and not link.startswith('http'):
                        link = 'https://www.kolesa-darom.ru' + link
                    price = 0
                    pm = re.search(r'(\d[\d\s]*)\s*₽', item.get_text())
                    if pm:
                        price = clean_price(pm.group(1))
                    if not price:
                        ptag = item.find(class_=lambda x: x and 'price' in str(x).lower())
                        price = clean_price(ptag.get_text() if ptag and hasattr(ptag, 'get_text') else '0')
                    img = img_el.get('src', '') or img_el.get('data-src', '') if img_el else ''
                    if img and not img.startswith('http'):
                        img = 'https://www.kolesa-darom.ru' + img
                    results.append(_result('Колёса Даром', title, price, link, img))
    except Exception as e:
        logger.error(f"[Колёса Даром] {e}")
    return results


async def parse_armtek(session, query):
    if not PLAYWRIGHT_AVAILABLE:
        return []
    results = []
    try:
        async with async_playwright() as p:
            html = await _human_page(
                p,
                warmup_url='https://armtek.ru/',
                target_url=f'https://armtek.ru/search?q={query}',
            )
            soup = BeautifulSoup(html, 'html.parser')

            cards = soup.find_all(class_=lambda x: x and 'carousel__list_container_item' in ' '.join(x) if x else False)
            if not cards:
                pels = soup.find_all(class_=lambda x: x and 'price' in str(x).lower() if x else False)
                cards = [el.find_parent('div') for el in pels if el.find_parent('div')]

            seen_titles = set()
            query_words = [w.lower() for w in query.split() if len(w) >= 3]

            for card in cards[:20]:
                text = card.get_text(strip=True)
                if '₽' not in text:
                    continue
                pm = re.search(r'(\d[\d\s]*)\s*₽', text)
                if not pm:
                    continue
                price = clean_price(pm.group(1))
                # Извлекаем title: после артикула (разделитель ·)
                parts = text.split('·')
                if len(parts) >= 2:
                    # Убираем артикул из начала, берём описание
                    raw_title = parts[1].strip()
                    # Убираем цену и мусор из конца
                    raw_title = re.sub(r'\d[\d\s]*₽.*$', '', raw_title).strip()
                    raw_title = re.sub(r'В корзину.*$', '', raw_title).strip()
                    raw_title = re.sub(r'Купить.*$', '', raw_title).strip()
                    title = raw_title[:100]
                else:
                    title = text[:100]
                if not title or len(title) < 5:
                    continue
                # Дедупликация
                if title in seen_titles:
                    continue
                seen_titles.add(title)
                # Базовая проверка релевантности — хотя бы одно слово запроса в title
                title_lower = title.lower()
                if query_words and not any(w in title_lower for w in query_words):
                    continue
                lel = card.find('a', href=True)
                link = lel.get('href', '') if lel else ''
                if link and not link.startswith('http'):
                    link = 'https://armtek.ru' + link
                if title and price:
                    iel = card.find('img')
                    img = ''
                    if iel:
                        img = iel.get('src', '') or iel.get('data-src', '')
                        if img and not img.startswith('http'):
                            img = 'https://armtek.ru' + img
                    results.append(_result('Armtek', title, price, link, img))
                    if len(results) >= 20:
                        break
    except Exception as e:
        logger.error(f"[Armtek] {e}")
    return results


# =============================================================================
# Exist.ru (Playwright + прогрев)
# =============================================================================

def _is_article_query(query: str) -> bool:
    """Определяет, является ли запрос артикулом (а не текстовым поиском)."""
    q = query.strip()
    # Артикул: содержит цифры и буквы, без пробелов или с дефисами
    if re.match(r'^[A-Za-z0-9\-./]{4,20}$', q):
        return True
    # Артикул вида "5Q0 615 301" или "96352591"
    no_spaces = q.replace(' ', '')
    if re.match(r'^[A-Za-z0-9]{5,20}$', no_spaces) and any(c.isdigit() for c in no_spaces):
        return True
    # "Бренд артикул" вида "FEBI 08730", "Mann W914/2" (макс 2 слова, латиница+цифры)
    words = q.split()
    if len(words) == 2 and not re.search(r'[а-яА-ЯёЁ]', q):
        if any(c.isdigit() for c in q):
            return True
    return False


async def parse_exist(session, query):
    if not PLAYWRIGHT_AVAILABLE:
        return []
    # Exist работает только по артикулам
    if not _is_article_query(query):
        logger.info(f"[Exist] Пропуск: '{query}' — не артикул")
        return []
    from urllib.parse import quote
    results = []
    try:
        async with async_playwright() as p:
            html = await _human_page(
                p,
                warmup_url='https://exist.ru/',
                target_url=f'https://exist.ru/Price/?pcode={quote(query)}',
            )
            soup = BeautifulSoup(html, 'html.parser')

            # Exist: таблица результатов с классами .row, .art, .partno, a.descr
            for row in soup.select('.row')[:20]:
                brand_el = row.select_one('.art')
                partno_el = row.select_one('.partno')
                descr_el = row.select_one('a.descr')
                if not descr_el:
                    continue

                brand = brand_el.get_text(strip=True) if brand_el else ''
                partno = partno_el.get_text(strip=True) if partno_el else ''
                descr = descr_el.get_text(strip=True)
                title = f"{brand} {partno} — {descr}".strip(' —')
                if len(title) < 5:
                    continue

                link = descr_el.get('href', '')
                if link and not link.startswith('http'):
                    link = 'https://exist.ru' + link

                price = 0
                price_el = row.select_one('[class*="price"]')
                if price_el:
                    pm = re.search(r'(\d[\d\s]*)', price_el.get_text())
                    if pm:
                        price = clean_price(pm.group(1))

                results.append(_result('Exist', title, price, link, ''))

            # Fallback: ссылки на товары если .row не сработал
            if not results:
                for a in soup.select('a[href*="/Parts/"]')[:20]:
                    title = a.get_text(strip=True)
                    if len(title) < 5:
                        continue
                    link = a.get('href', '')
                    if link and not link.startswith('http'):
                        link = 'https://exist.ru' + link
                    parent = a.find_parent('tr') or a.find_parent('div')
                    price = 0
                    if parent:
                        pm = re.search(r'(\d[\d\s]*)\s*₽', parent.get_text())
                        if pm:
                            price = clean_price(pm.group(1))
                    results.append(_result('Exist', title, price, link, ''))
    except Exception as e:
        logger.error(f"[Exist] {e}")
    return results


# =============================================================================
# Autodoc.ru (через внутренний REST API, без Playwright)
# =============================================================================

_AUTODOC_HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'ru-RU,ru;q=0.9',
    'Content-Type': 'application/json',
    'Origin': 'https://www.autodoc.ru',
    'Referer': 'https://www.autodoc.ru/',
}


async def parse_autodoc(session, query):
    """
    Autodoc: 2 шага через внутренний API (без Playwright).
    1) POST search → получаем categoryId по текстовому запросу
    2) POST find-goods → получаем товары с ценами из категории
    """
    from urllib.parse import quote
    results = []
    headers = {**_AUTODOC_HEADERS, 'User-Agent': _random_ua()}
    try:
        # Шаг 1: поиск категории по запросу
        search_url = (
            'https://web.autodoc.ru/api/catalog-universal-service/'
            f'catalog-universal-categories/search?SearchText={quote(query)}'
        )
        async with session.post(search_url, headers=headers, ssl=False, timeout=TIMEOUT) as r:
            if r.status != 200:
                logger.warning(f"[Autodoc] search HTTP {r.status}")
                return []
            data = await r.json()

        categories = data.get('items', [])
        if not categories:
            logger.info(f"[Autodoc] Категории не найдены для '{query}'")
            return []

        # Ищем категорию каталога (routeUrl содержит 'catalogs')
        cat = None
        cat_id = None
        for item in categories:
            url = item.get('routeUrl', '')
            if 'catalogs' in url:
                m = re.search(r'-(\d+)$', url)
                if m:
                    cat = item
                    cat_id = int(m.group(1))
                    break

        # Если найден только производитель (/man/), повторяем поиск без бренда
        if not cat_id:
            first_url = categories[0].get('routeUrl', '') if categories else ''
            if '/man/' in first_url:
                # Универсальный fallback: пробуем без первого слова (бренда),
                # потом без последнего слова, потом каждое слово отдельно
                words = query.split()
                fallback_queries = []
                if len(words) > 1:
                    fallback_queries.append(' '.join(words[1:]))   # без первого
                    fallback_queries.append(' '.join(words[:-1]))  # без последнего
                for fallback_q in fallback_queries:
                    if cat_id:
                        break
                    logger.info(f"[Autodoc] Повтор: '{fallback_q}'")
                    fb_url = (
                        'https://web.autodoc.ru/api/catalog-universal-service/'
                        f'catalog-universal-categories/search?SearchText={quote(fallback_q)}'
                    )
                    async with session.post(fb_url, headers=headers, ssl=False, timeout=TIMEOUT) as r2:
                        if r2.status == 200:
                            data2 = await r2.json()
                            for item in data2.get('items', []):
                                url = item.get('routeUrl', '')
                                if 'catalogs' in url:
                                    m = re.search(r'-(\d+)$', url)
                                    if m:
                                        cat = item
                                        cat_id = int(m.group(1))
                                        break

        if not cat_id:
            route_url = categories[0].get('routeUrl', '') if categories else '?'
            logger.warning(f"[Autodoc] Не удалось найти категорию (результат: {route_url})")
            return []

        # Шаг 2: получаем товары из категории
        goods_url = (
            'https://web.autodoc.ru/api/catalog-universal-service/'
            f'catalog-universal-goods/find-goods?CategoryId={cat_id}'
            '&PageNumber=0&IsCatalogsCar=false&MaxResultCount=20'
        )
        async with session.post(goods_url, headers=headers, ssl=False, timeout=TIMEOUT) as r:
            if r.status != 200:
                logger.warning(f"[Autodoc] find-goods HTTP {r.status}")
                return []
            data = await r.json()

        items = data.get('items', [])
        for item in items[:20]:
            name = item.get('name', '')
            if not name or len(name) < 5:
                continue
            article = item.get('article', '')
            manufacturer = item.get('manufacturer', {})
            brand = manufacturer.get('name', '')
            title = f"{brand} {name}".strip() if brand else name
            price = item.get('price', 0)
            img = item.get('imageUrl', '')
            link = f"https://www.autodoc.ru/catalogs/universal/goods/{article}" if article else ''
            results.append(_result('Autodoc', title, int(price), link, img))

    except Exception as e:
        logger.error(f"[Autodoc] {e}")
    return results


# =============================================================================
# Emex.ru (через внутренний REST API, требует прокси)
# =============================================================================

_EMEX_HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'ru-RU,ru;q=0.9',
    'Origin': 'https://emex.ru',
    'Referer': 'https://emex.ru/',
}


async def parse_emex(session, query):
    """
    Emex: поиск через внутренний API search2.
    Emex блокирует прямой IP — нужен прокси.
    Работает и по артикулам, и по текстовым запросам (артикулы дают больше результатов).
    """
    from urllib.parse import quote
    results = []
    headers = {**_EMEX_HEADERS, 'User-Agent': _random_ua()}
    proxy_url = proxy_rotator.get()
    if not proxy_url:
        logger.warning("[Emex] Пропуск: нет прокси (emex.ru блокирует прямой IP)")
        return []
    try:
        # Шаг 1: поиск по запросу
        search_url = (
            'https://emex.ru/api/search/search2'
            f'?detailNum={quote(query)}&isHeaderSearch=true'
            '&showAll=true&searchSource=direct'
            f'&searchString={quote(query)}'
        )
        connector = None
        if 'socks' in proxy_url:
            from aiohttp_socks import ProxyConnector
            connector = ProxyConnector.from_url(proxy_url)

        proxy_session_kwargs = {}
        if connector:
            proxy_session_kwargs['connector'] = connector
        else:
            proxy_session_kwargs['trust_env'] = True

        async with aiohttp.ClientSession(**proxy_session_kwargs) as proxy_session:
            request_kwargs = {'headers': headers, 'ssl': False, 'timeout': TIMEOUT}
            if not connector and proxy_url:
                request_kwargs['proxy'] = proxy_url

            async with proxy_session.get(search_url, **request_kwargs) as r:
                if r.status != 200:
                    logger.warning(f"[Emex] search2 HTTP {r.status}")
                    return []
                data = await r.json()

        sr = data.get('searchResult', {})

        # Проверяем makes (список производителей с bestPrice)
        makes = sr.get('makes', {})
        makes_list = makes.get('list', []) if isinstance(makes, dict) else []

        if makes_list:
            for item in makes_list[:20]:
                make = item.get('make', '')
                name = item.get('name', '')
                num = item.get('num', '')
                best_price = item.get('bestPrice', {})
                price_val = best_price.get('value', 0) if best_price else 0
                url_path = item.get('url', '')
                link = f"https://emex.ru{url_path}" if url_path else ''
                title = f"{make} {name}".strip() if make else name
                if num and num not in title:
                    title = f"{title} ({num})"
                if not title or len(title) < 3:
                    continue
                results.append(_result('Emex', title, int(price_val), link, ''))
        else:
            # Проверяем suggestions
            suggestions = sr.get('suggestions', [])
            for item in suggestions[:20]:
                make = item.get('make', '')
                name = item.get('name', '')
                num = item.get('detailNum', '')
                url_path = item.get('url', '')
                link = f"https://emex.ru{url_path}" if url_path else ''
                title = f"{make} {name}".strip() if make else name
                if num and num not in title:
                    title = f"{title} ({num})"
                if not title or len(title) < 3:
                    continue
                results.append(_result('Emex', title, 0, link, ''))

    except Exception as e:
        logger.error(f"[Emex] {e}")
    return results


# =============================================================================
# Dvizhcom.ru (aiohttp + BS4, Next.js SSR)
# =============================================================================

async def parse_dvizhcom(session, query):
    """
    Dvizhcom: поиск через SSR HTML (Next.js).
    URL: /auto/search/?q=...&type=n
    Данные рендерятся сервером — парсим BeautifulSoup.
    """
    from urllib.parse import quote
    results = []
    try:
        url = f'https://dvizhcom.ru/auto/search/?q={quote(query)}&type=n'
        html = await _fetch(session, url)
        if not html:
            return []
        soup = BeautifulSoup(html, 'html.parser')
        cards = soup.select('[class*="ProductCard_mainDesktop"], [class*="ProductCard_main__"]')
        if not cards:
            cards = soup.select('[class*="ProductCard"]')

        seen = set()
        for card in cards[:30]:
            text = card.get_text(strip=True)
            if not text or len(text) < 10:
                continue

            # Ссылка на товар
            a = card.select_one('a[href*="/catalogs/"]')
            if not a:
                continue
            title_text = a.get_text(strip=True)
            if not title_text or len(title_text) < 5 or title_text in seen:
                continue
            seen.add(title_text)

            link = a.get('href', '')
            if link and not link.startswith('http'):
                link = 'https://dvizhcom.ru' + link

            # Цена: ищем "X XXX ₽" в тексте (после "Код XXXXXX")
            pm = re.search(r'Код\s*\d+\s*([\d\s]+)\s*₽', text)
            if not pm:
                pm = re.search(r'([\d\s]+)\s*₽', text)
            price = 0
            if pm:
                price_str = pm.group(1).replace(' ', '').strip()
                if price_str.isdigit():
                    price = int(price_str)

            # Изображение: может быть в родителе (card не содержит img напрямую)
            img_el = card.select_one('img')
            if not img_el and card.parent:
                img_el = card.parent.select_one('img')
            img = ''
            if img_el:
                img = img_el.get('src', '') or img_el.get('data-src', '')
                if img and (img.startswith('data:') or 'loading' in img):
                    img = ''
                if img and not img.startswith('http'):
                    img = 'https://dvizhcom.ru' + img

            results.append(_result('Dvizhcom', title_text, price, link, img))
            if len(results) >= 20:
                break

    except Exception as e:
        logger.error(f"[Dvizhcom] {e}")
    return results


# =============================================================================
# Megazip.ru (Playwright, SPA)
# =============================================================================

async def parse_megazip(session, query):
    """
    Megazip: чистый SPA, данные грузятся через JS.
    Используем Playwright для рендеринга.
    """
    if not PLAYWRIGHT_AVAILABLE:
        return []
    results = []
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled', '--no-sandbox'],
            )
            ctx = await browser.new_context(
                user_agent=_random_ua(),
                viewport={'width': 1366, 'height': 768},
                locale='ru-RU',
            )
            page = await ctx.new_page()
            try:
                from playwright_stealth import stealth_async
                await stealth_async(page)
            except ImportError:
                await ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            api_data = []

            async def on_response(response):
                url = response.url
                ct = response.headers.get('content-type', '')
                if response.status == 200 and 'json' in ct and ('search' in url or 'product' in url or 'catalog' in url):
                    try:
                        body = await response.text()
                        if len(body) > 50:
                            api_data.append({'url': url, 'body': body})
                    except:
                        pass

            page.on('response', on_response)

            await page.goto('https://megazip.ru/', timeout=20000, wait_until='domcontentloaded')
            await page.wait_for_timeout(3000)

            # Ищем поле поиска
            inp = None
            inputs = await page.query_selector_all('input')
            for el in inputs:
                visible = await el.is_visible()
                if visible:
                    ph = await el.get_attribute('placeholder') or ''
                    t = await el.get_attribute('type') or 'text'
                    if t in ('text', 'search') or 'поиск' in ph.lower() or 'search' in ph.lower():
                        inp = el
                        break

            if inp:
                await inp.click()
                await page.wait_for_timeout(500)
                await inp.fill(query)
                await page.wait_for_timeout(2000)
                await page.keyboard.press('Enter')
                await page.wait_for_timeout(8000)

                soup = BeautifulSoup(await page.content(), 'html.parser')

                seen = set()
                for card in soup.select('[class*="product"], [class*="card"], [class*="item"], [class*="catalog"]')[:20]:
                    a = card.select_one('a[href]')
                    if not a:
                        continue
                    title = a.get_text(strip=True)
                    if not title or len(title) < 5 or title in seen:
                        continue
                    seen.add(title)
                    link = a.get('href', '')
                    if link and not link.startswith('http'):
                        link = 'https://megazip.ru' + link
                    pm = re.search(r'(\d[\d\s]*)\s*₽', card.get_text())
                    price = clean_price(pm.group(1)) if pm else 0
                    img_el = card.select_one('img')
                    img = ''
                    if img_el:
                        img = img_el.get('src', '') or img_el.get('data-src', '')
                    results.append(_result('Megazip', title, price, link, img))
                    if len(results) >= 20:
                        break

            await browser.close()
    except Exception as e:
        logger.error(f"[Megazip] {e}")
    return results


# =============================================================================
# АГРЕГАТОР
# =============================================================================

# Семафор: макс 3 Playwright-браузера одновременно (чтобы не перегрузить систему)
_pw_semaphore = asyncio.Semaphore(3)


async def _pw_limited(name, fn, session, query):
    """Обёртка: ограничивает параллельность Playwright-парсеров через семафор."""
    async with _pw_semaphore:
        return await fn(session, query)


_NOISE_WORDS = {
    'купить', 'заказать', 'найти', 'искать', 'продажа', 'цена', 'стоимость',
    'недорого', 'дешево', 'дёшево', 'оригинал', 'аналог', 'для', 'авто',
}


def _simplify_query(query: str) -> str:
    """
    Универсальное упрощение запроса: убираем только мусорные слова.
    Кириллица и латиница сохраняются — обе важны для поиска.

    Примеры:
      'Купить фильтр Mann W914/2'   → 'фильтр Mann W914/2'
      'FEBI 08730 Гайка M26x1 5mm'  → 'FEBI 08730 Гайка M26x1 5mm' (без изменений)
      'масло ZIC 5W-30'             → 'масло ZIC 5W-30' (без изменений)
      'колодки тойота'              → 'колодки тойота' (без изменений)
      'фары вольсваген'             → 'фары вольсваген' (без изменений)
      'заказать бампер хонда'       → 'бампер хонда'
    """
    q = query.strip()
    words = q.split()
    if len(words) <= 1:
        return q

    cleaned = [w for w in words if w.lower() not in _NOISE_WORDS]

    if not cleaned:
        return q

    return ' '.join(cleaned)


def _extract_article_query(query: str) -> str | None:
    """
    Если запрос содержит артикул (бренд + числовой код), возвращает
    короткий вариант 'бренд артикул' для второго прохода поиска.
    Иначе None.

    Примеры:
      'FEBI 08730 Гайка M26x1 5mm' → 'FEBI 08730'
      'Mann W914/2 фильтр масляный' → 'Mann W914/2'
      'колодки тойота'              → None
      'Pirelli Ice Zero FR'         → None (нет числового артикула)
    """
    words = query.strip().split()
    if len(words) < 3:
        return None

    # Ищем бренд (латиница) + артикул (цифры, возможно с буквами)
    brand = None
    article = None
    for w in words:
        # Артикул: минимум 3 цифры, возможно с буквами/точками (08730, W914/2, 46617)
        if re.search(r'\d{3,}', w) and not re.fullmatch(r'\d+[wW]-?\d+', w):
            # Не спецификация вроде 5W-30, 10W40
            if not article:
                article = w
        # Бренд: латиница, 2+ символов (FEBI, Mann, Bosch)
        elif re.search(r'[a-zA-Z]{2,}', w) and not re.search(r'[а-яА-ЯёЁ]', w):
            if not brand:
                brand = w

    if brand and article:
        return f'{brand} {article}'
    if article and len(article) >= 4:
        return article

    return None


async def search_all_sites(query: str) -> list:
    """
    Запускает все парсеры параллельно.
    HTTP — без ограничений, Playwright — макс 3 одновременно.

    Если запрос содержит артикул (FEBI 08730 ...), запускает второй проход
    по HTTP-парсерам с коротким запросом 'бренд артикул' и объединяет результаты.
    """
    import time
    t0 = time.time()

    # Упрощаем запрос (убираем мусорные слова)
    clean_q = _simplify_query(query)
    if clean_q != query:
        logger.info(f"[Агрегатор] Запрос упрощён: '{query}' → '{clean_q}'")

    # Проверяем, есть ли артикул для второго прохода
    article_q = _extract_article_query(clean_q)
    if article_q and article_q != clean_q:
        logger.info(f"[Агрегатор] Артикул: '{article_q}' (доп. поиск)")

    async with aiohttp.ClientSession() as session:
        # --- Основной проход ---
        http_tasks = {
            'Part-Kom': parse_partkom(session, clean_q),
            'Koleso': parse_koleso(session, clean_q),
            'Ruli': parse_ruli(session, clean_q),
            'Wildberries': parse_wildberries(session, clean_q),
            'Autodoc': parse_autodoc(session, clean_q),
            'Emex': parse_emex(session, clean_q),
            'Dvizhcom': parse_dvizhcom(session, clean_q),
        }

        pw_tasks = {}
        if PLAYWRIGHT_AVAILABLE:
            pw_tasks = {
                'Колёса Даром': _pw_limited('КД', parse_kolesa_darom, session, clean_q),
                'Armtek': _pw_limited('Armtek', parse_armtek, session, clean_q),
                'Exist': _pw_limited('Exist', parse_exist, session, clean_q),
                'Parterra': _pw_limited('Parterra', parse_parterra, session, clean_q),
                'Autopiter': _pw_limited('Autopiter', parse_autopiter, session, clean_q),
                'Bibinet': _pw_limited('Bibinet', parse_bibinet, session, clean_q),
                'Ozon': _pw_limited('Ozon', parse_ozon, session, clean_q),
                'Megazip': _pw_limited('Megazip', parse_megazip, session, clean_q),
            }

        all_tasks = {**http_tasks, **pw_tasks}
        names = list(all_tasks.keys())
        coros = list(all_tasks.values())

        results_list = await asyncio.gather(*coros, return_exceptions=True)

        final = []
        seen_titles = set()
        for i, res in enumerate(results_list):
            if isinstance(res, list):
                for item in res:
                    key = (item.get('title', ''), item.get('source', ''))
                    if key not in seen_titles:
                        seen_titles.add(key)
                        final.append(item)
                logger.info(f"[{names[i]}] ✅ {len(res)}")
            else:
                logger.error(f"[{names[i]}] ❌ {res}")

        # --- Второй проход по артикулу (если есть) ---
        if article_q and article_q != clean_q:
            art_tasks = {
                'Part-Kom②': parse_partkom(session, article_q),
                'Emex②': parse_emex(session, article_q),
                'Autodoc②': parse_autodoc(session, article_q),
                'Ruli②': parse_ruli(session, article_q),
                'Dvizhcom②': parse_dvizhcom(session, article_q),
            }
            if PLAYWRIGHT_AVAILABLE:
                art_tasks['Exist②'] = _pw_limited('Exist②', parse_exist, session, article_q)
            art_names = list(art_tasks.keys())
            art_coros = list(art_tasks.values())
            art_results = await asyncio.gather(*art_coros, return_exceptions=True)

            added = 0
            for i, res in enumerate(art_results):
                if isinstance(res, list):
                    for item in res:
                        key = (item.get('title', ''), item.get('source', ''))
                        if key not in seen_titles:
                            seen_titles.add(key)
                            final.append(item)
                            added += 1
                    logger.info(f"[{art_names[i]}] ✅ {len(res)}")
                else:
                    logger.error(f"[{art_names[i]}] ❌ {res}")
            if added:
                logger.info(f"[Агрегатор] Артикул-поиск добавил {added} новых")

    elapsed = time.time() - t0
    logger.info(f"[Агрегатор] Всего {len(final)} товаров за {elapsed:.1f}с")
    return final
