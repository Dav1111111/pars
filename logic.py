import re
import logging

logger = logging.getLogger(__name__)


def clean_price(price_raw: str) -> int:
    """
    Превращает строку '1 250,00 ₽' в int 1250.
    Если цена не найдена или 'по запросу', возвращает 0.
    """
    if not price_raw:
        return 0
    s = str(price_raw).strip()
    s = re.sub(r'[.,]\d{1,2}\s*$', '', s)
    s = re.sub(r'[.,]\d{1,2}\s*₽', ' ₽', s)
    clean_str = re.sub(r'[^\d]', '', s)
    if not clean_str:
        return 0
    return int(clean_str)


# =============================================================================
# УНИВЕРСАЛЬНАЯ СИСТЕМА ФИЛЬТРАЦИИ
# Не требует ручного хардкода — работает автоматически для любых запросов.
# =============================================================================

# Транслитерация автомобильных брендов (кириллица → латиница)
_BRAND_ALIASES = {
    'вольсваген': 'volkswagen', 'фольксваген': 'volkswagen', 'фольцваген': 'volkswagen',
    'тойота': 'toyota', 'тоёта': 'toyota',
    'хонда': 'honda', 'хёнда': 'honda',
    'хендай': 'hyundai', 'хёндай': 'hyundai', 'хюндай': 'hyundai', 'хундай': 'hyundai',
    'киа': 'kia', 'кия': 'kia',
    'ниссан': 'nissan', 'нисан': 'nissan',
    'мазда': 'mazda',
    'субару': 'subaru',
    'мицубиси': 'mitsubishi', 'митсубиши': 'mitsubishi', 'митсубиси': 'mitsubishi',
    'сузуки': 'suzuki',
    'лексус': 'lexus',
    'инфинити': 'infiniti',
    'мерседес': 'mercedes', 'мерс': 'mercedes',
    'бмв': 'bmw', 'бэмвэ': 'bmw',
    'ауди': 'audi',
    'порше': 'porsche', 'порш': 'porsche',
    'опель': 'opel',
    'шкода': 'skoda',
    'сеат': 'seat',
    'вольво': 'volvo',
    'пежо': 'peugeot',
    'ситроен': 'citroen', 'ситроён': 'citroen',
    'рено': 'renault',
    'форд': 'ford',
    'шевроле': 'chevrolet', 'шевролет': 'chevrolet',
    'кадиллак': 'cadillac',
    'додж': 'dodge',
    'крайслер': 'chrysler',
    'джип': 'jeep',
    'лада': 'lada', 'ваз': 'vaz',
    'газ': 'gaz', 'газель': 'gazel',
    'уаз': 'uaz',
    'чери': 'chery', 'черри': 'chery',
    'хавейл': 'haval', 'хавал': 'haval',
    'джили': 'geely',
    'грейт вол': 'great wall', 'грейтвол': 'greatwall',
    'лифан': 'lifan',
    'бид': 'byd',
    'пирелли': 'pirelli',
    'мишлен': 'michelin',
    'бриджстоун': 'bridgestone', 'бриджстон': 'bridgestone',
    'нокиан': 'nokian',
    'континенталь': 'continental',
    'данлоп': 'dunlop',
    'гудиер': 'goodyear', 'гудьир': 'goodyear',
    'йокогама': 'yokohama', 'йокохама': 'yokohama',
}

def _tokenize(text: str) -> list:
    """Разбивает текст на нормализованные токены."""
    t = text.lower()
    t = re.sub(r'[-_/]', ' ', t)
    t = re.sub(r'(\d+w)\s+(\d+)', r'\1\2', t)  # 10W 40 → 10w40
    return [w for w in re.split(r'[\s,.;:()\[\]]+', t) if len(w) >= 2]


def _relevance_score(title: str, query: str) -> float:
    """
    Универсальная оценка релевантности 0.0 — 1.0.
    Считает долю слов запроса, найденных в названии товара.
    Работает автоматически для любых запросов без хардкода.
    Поддерживает транслитерацию брендов (вольсваген → volkswagen).
    """
    q_tokens = _tokenize(query)
    if not q_tokens:
        return 1.0

    t_lower = title.lower()
    t_text = re.sub(r'[-_/]', ' ', t_lower)
    t_text = re.sub(r'(\d+w)\s+(\d+)', r'\1\2', t_text)

    matches = 0
    for qt in q_tokens:
        # Точное вхождение токена в текст
        if qt in t_text:
            matches += 1
            continue
        # Совпадение по основе (первые 3-4 символа, с учётом окончаний)
        stem = qt[:4] if len(qt) > 4 else qt[:3]
        if len(qt) >= 3 and stem in t_text:
            matches += 1
            continue
        # Транслитерация бренда: вольсваген → volkswagen
        alias = _BRAND_ALIASES.get(qt)
        if alias and alias in t_text:
            matches += 1
            continue
        # Обратная транслитерация: volkswagen → вольсваген
        for cyr, lat in _BRAND_ALIASES.items():
            if qt == lat and cyr[:4] in t_text:
                matches += 1
                break

    score = matches / len(q_tokens)

    # Бонус: склеенный вариант (ATC-SPORT → atcsport)
    if score < 1.0:
        q_glued = re.sub(r'[\W_]+', '', query.lower())
        t_glued = re.sub(r'[\W_]+', '', t_lower)
        if len(q_glued) >= 3 and q_glued in t_glued:
            score = 1.0

    return score


# Исключения: товары точно НЕ для автомобилей (для маркетплейсов)
_EXCLUDE = [
    'кулинар', 'кухн', 'пищев', 'подсолнеч', 'оливков', 'рапсов', 'кокосов',
    'массаж', 'косметик', 'волос', 'для тела', 'кожа лица', 'эфирн',
    'швейн', 'вязан', 'рукодел', 'мебел', 'интерьер',
    'детск игрушк', 'канцеляр', 'для дома', 'стиральн', 'посудомоеч',
]


def _is_not_junk(title: str) -> bool:
    """Отсеивает явный мусор (не авто-товары) для маркетплейсов."""
    t = title.lower()
    return not any(ex in t for ex in _EXCLUDE)


def filter_results(items: list, query: str, sort_by: str = 'price_asc',
                   article_query: str = '') -> list:
    """
    Универсальная фильтрация и сортировка результатов.

    Логика:
    - Авто-магазины (Part-Kom, Autodoc, ...): доверяем их поиску, пропускаем всё
    - Маркетплейсы (WB, Ozon): проверяем что товар не мусор + релевантен запросу
    - Везде: отсеиваем price=0 и навигационные ссылки

    Адаптивный порог релевантности:
    - 1 слово → 100%
    - 2 слова → 50%
    - 3+ слов → минимум 40% (хотя бы 2 из 5 токенов)

    article_query: короткий вариант запроса (бренд+артикул), если обнаружен.
    Товар проходит, если он релевантен хотя бы одному из запросов.
    """
    query_lower = query.lower()
    q_tokens = _tokenize(query)
    # Адаптивный порог: минимум 40% совпадения
    threshold = max(1.0 / max(len(q_tokens), 1), 0.4) if q_tokens else 0

    # Артикул-запрос: если есть, используем его как альтернативу
    # Для артикулов требуем 100% совпадение (и бренд, и номер)
    art_tokens = _tokenize(article_query) if article_query else []
    art_threshold = 1.0 if art_tokens else 0

    # Авто-магазины: доверяем их поиску
    auto_sources = {
        'Part-Kom', 'Parterra', 'Koleso', 'Armtek', 'Колёса Даром', 'Ruli',
        'Autopiter', 'Bibinet', 'Autodoc', 'Emex', 'Dvizhcom', 'Exist', 'Megazip'
    }

    filtered = []
    for item in items:
        price = item.get('price_int', 0)
        source = item.get('source', '')
        # Маркетплейсы без цены — бесполезны; авто-магазины "Под заказ" — допускаем
        if price < 1 and source not in auto_sources:
            continue

        title = item.get('title', '')

        # Мусор: слишком короткие и навигационные ссылки
        if len(title) < 8:
            continue

        score = _relevance_score(title, query)
        # Альтернативная оценка по артикулу (если есть)
        art_score = _relevance_score(title, article_query) if article_query else 0
        best_score = max(score, art_score)

        if source in auto_sources:
            passed = score >= threshold or (art_score >= art_threshold and art_score >= 0.5)
            if passed:
                filtered.append(item)
        else:
            # Маркетплейсы: фильтруем мусор + проверяем релевантность
            if not _is_not_junk(title):
                continue
            passed = score >= threshold or (art_score >= art_threshold and art_score >= 0.5)
            if passed:
                filtered.append(item)

    # Сортировка (товары без цены — в конец)
    if sort_by == 'price_desc':
        filtered.sort(key=lambda x: (x['price_int'] == 0, -x['price_int']))
    elif sort_by == 'source':
        filtered.sort(key=lambda x: x.get('source', ''))
    else:
        filtered.sort(key=lambda x: (x['price_int'] == 0, x['price_int']))

    return filtered
