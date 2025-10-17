#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import re
import unicodedata
import hashlib
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from collections import Counter
from contextlib import asynccontextmanager

import requests
from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ─────────────────────────────────────────────────────────────
# Конфиг
# ─────────────────────────────────────────────────────────────
PRODUCTS_JSON_URL = os.getenv("PRODUCTS_JSON_URL", "https://upravdom.com/upload/catalog.json")
REFRESH_SECONDS   = int(os.getenv("REFRESH_SECONDS", "300"))   # авто-обновление каждые N сек
HTTP_TIMEOUT      = float(os.getenv("HTTP_TIMEOUT", "30.0"))   # таймаут HTTP, сек
DISK_CACHE_PATH   = Path(os.getenv("DISK_CACHE_PATH", "./catalog_cache.json"))

MAX_PER_PAGE      = 20  # жёсткий потолок
DEFAULT_PER_PAGE  = 20

# Очерёдность регионов для фоллбека, если в запрошенном регионе нет цены
FALLBACK_PRIORITY = [
    # new keys first
    "moscow", "sankt-peterburg", "ekaterinburg", "krasnodar", "sochi", "adler",
    "spb", "msk", "ekb", "nsk", "nn", "krd",
]

# ─────────────────────────────────────────────────────────────
# Утилиты
# ─────────────────────────────────────────────────────────────
WHITESPACE_RE = re.compile(r"\s+", re.UNICODE)

def normalize_text(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFKC", s).replace("Ё", "Е").replace("ё", "е")
    s = s.casefold()
    return WHITESPACE_RE.sub(" ", s).strip()

TRANS_MAP = str.maketrans({
    "a":"а","b":"б","v":"в","g":"г","d":"д","e":"е","z":"з","i":"и","y":"й",
    "k":"к","l":"л","m":"м","n":"н","o":"о","p":"п","r":"р","s":"с","t":"т",
    "u":"у","f":"ф","h":"х","c":"ц",
})
def naive_lat_to_ru(s: str) -> str:
    return "".join(ch.translate(TRANS_MAP) if ch.isascii() else ch for ch in s)

def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None

def coerce_prices_map(prices_field: Union[List[Dict[str, Any]], Dict[str, Any], None]) -> Dict[str, Dict[str, Optional[float]]]:
    """
    Унифицируем цены в вид:
      {"spb": {"price": 2500.0, "price_old": None}, ...}
    Поддержано:
      - [{"spb": {"price":..,"price_old":..}}, {"msk": {...}}]
      - {"spb": {"price":..}, "msk": {...}}
      - None
    """
    res: Dict[str, Dict[str, Optional[float]]] = {}
    if prices_field is None:
        return res
    if isinstance(prices_field, dict):
        for rk, payload in prices_field.items():
            if isinstance(payload, dict):
                res[str(rk).lower()] = {
                    "price": _to_float(payload.get("price")),
                    "price_old": _to_float(payload.get("price_old")),
                }
        return res
    if isinstance(prices_field, list):
        for chunk in prices_field:
            if not isinstance(chunk, dict):
                continue
            for rk, payload in chunk.items():
                if isinstance(payload, dict):
                    res[str(rk).lower()] = {
                        "price": _to_float(payload.get("price")),
                        "price_old": _to_float(payload.get("price_old")),
                    }
        return res
    return res

def extract_subdomain(host: str) -> Optional[str]:
    host = (host or "").split(",")[0].strip().lower().split(":")[0]
    parts = host.split(".")
    if len(parts) >= 3:
        return parts[0]
    return None

def hard_region_from_host(host: str, known_regions: set) -> str:
    """
    Жёстко:
      - нет субдомена → 'msk'
      - есть субдомен и он в known_regions → он
      - иначе → 'msk'
    """
    sub = extract_subdomain(host)
    if not sub:
        return "msk"
    sub = sub.lower()
    return sub if sub in known_regions else "msk"

def make_etag(*parts: str) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(p.encode("utf-8"))
    return '"' + h.hexdigest()[:32] + '"'

# ─────────────────────────────────────────────────────────────
# Модели
# ─────────────────────────────────────────────────────────────
class Product(BaseModel):
    name: str
    url: Optional[str] = None
    img: Optional[str] = None
    measure_code: Optional[str] = None
    sale: Optional[bool] = None
    is_hit: Optional[bool] = None
    discount_percent: Optional[float] = None
    prices: Dict[str, Dict[str, Optional[float]]] = {}

    # совместимость/доп
    image: Optional[str] = None
    id_code: Optional[str] = None
    currency: Optional[str] = "RUB"
    type: Optional[str] = None

    # категория/секция/хлебные крошки (если есть)
    category: Optional[str] = None
    section: Optional[str] = None
    product_section_name: Optional[str] = None
    breadcrumbs: Optional[Union[str, List[str]]] = None

    # внутреннее поле для быстрого поиска
    _search_blob: Optional[str] = None

# ─────────────────────────────────────────────────────────────
# Каталог (URL + ETag/LM + фоновое обновление + дисковый кэш)
# ─────────────────────────────────────────────────────────────
class Catalog:
    def __init__(self, source_url: str, disk_cache: Path):
        self.source_url = source_url
        self.disk_cache = disk_cache
        self.products: List[Product] = []
        self.known_regions: Counter[str] = Counter()
        self._etag: Optional[str] = None
        self._last_modified: Optional[str] = None
        self._mtime_marker: float = 0.0
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._bg_thread: Optional[threading.Thread] = None

    @property
    def mtime(self) -> float:
        return self._mtime_marker

    def _parse_items(self, text: str) -> List[Dict[str, Any]]:
        """
        Пытаемся вытащить список товаров из разных форматов:
          - JSON-массив: [ {...}, {...} ]
          - Объект-обёртка: {"items":[...]} / {"data":[...]} / {"products":[...]} / {"catalog":[...]} / {"result":[...]} / {"rows":[...]} / {"list":[...]} / {"goods":[...]}
          - Объект-словарь: {"123": {...}, "124": {...}}  → берём values()
          - NDJSON: по строке на объект
        """
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None

        if isinstance(parsed, list):
            return parsed

        if isinstance(parsed, dict):
            for k in ("items", "data", "products", "catalog", "result", "rows", "list", "goods"):
                v = parsed.get(k)
                if isinstance(v, list):
                    return v
            values = list(parsed.values())
            if values and all(isinstance(x, dict) for x in values):
                return values

        items: List[Dict[str, Any]] = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    items.append(obj)
            except Exception:
                continue
        return items

    def _apply_items(self, items: List[Dict[str, Any]]):
        products: List[Product] = []
        known = Counter()
        for it in items:
            prices_map = coerce_prices_map(it.get("prices"))
            if not prices_map and "price" in it:
                prices_map = {"msk": {"price": _to_float(it.get("price")), "price_old": None}}
            known.update(prices_map.keys())

            img = it.get("img") or it.get("image") or it.get("picture") or it.get("img_url") or it.get("image_url")

            p = Product(
                name = it.get("name") or it.get("title") or "",
                url  = it.get("url") or it.get("link"),
                img  = img,
                measure_code = it.get("measure_code") or it.get("unit"),
                sale  = it.get("sale") if it.get("sale") is not None else it.get("has_sale"),
                is_hit= it.get("is_hit"),
                discount_percent = it.get("discount_percent"),
                prices = prices_map,

                image = it.get("image") or it.get("picture"),
                id_code = it.get("id_code") or it.get("code") or it.get("sku"),
                currency = it.get("currency") or "RUB",
                type = it.get("type") or it.get("category"),

                category = it.get("product_section_name") or it.get("category") or it.get("section"),
                section = it.get("section") or it.get("product_section_url"),
                product_section_name = it.get("product_section_name"),
                breadcrumbs = it.get("breadcrumbs") or it.get("path") or it.get("trail"),
            )

            # поисковый blob из ключевых полей
            crumbs = p.breadcrumbs
            if isinstance(crumbs, list):
                crumbs = " / ".join(map(str, crumbs))
            blob_parts = [
                p.name or "",
                p.type or "",
                p.category or "",
                p.section or "",
                p.product_section_name or "",
                crumbs or "",
                p.url or "",
                p.id_code or "",
            ]
            p._search_blob = normalize_text(" ".join(part for part in blob_parts if part))

            products.append(p)

        with self._lock:
            self.products = products
            self.known_regions = known
            self._mtime_marker = time.time()

    def refresh_from_url(self):
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; UpravdomSearch/1.0)",
            "Accept": "application/json",
        }
        if self._etag:
            headers["If-None-Match"] = self._etag
        if self._last_modified:
            headers["If-Modified-Since"] = self._last_modified

        resp = requests.get(self.source_url, headers=headers, timeout=HTTP_TIMEOUT)
        print(f"[CATALOG] GET {self.source_url} -> {resp.status_code} ct={resp.headers.get('Content-Type')} len={len(resp.content)}")
        if resp.status_code == 304:
            return
        resp.raise_for_status()

        ct = (resp.headers.get("Content-Type") or "").lower()
        if "json" not in ct:
            raise ValueError(f"Неверный Content-Type: {ct}")

        text = resp.text
        items = self._parse_items(text)
        if not isinstance(items, list):
            raise ValueError("Ожидался JSON-массив или NDJSON.")

        print(f"[CATALOG] parsed items: {len(items)}")
        self._apply_items(items)

        self._etag = resp.headers.get("ETag") or self._etag
        self._last_modified = resp.headers.get("Last-Modified") or self._last_modified

        try:
            self.disk_cache.parent.mkdir(parents=True, exist_ok=True)
            with self.disk_cache.open("w", encoding="utf-8") as f:
                json.dump(items, f, ensure_ascii=False)
        except Exception:
            pass

    def load_from_disk_cache(self) -> bool:
        try:
            text = self.disk_cache.read_text(encoding="utf-8")
        except Exception:
            return False
        items = self._parse_items(text)
        if not isinstance(items, list):
            return False
        self._apply_items(items)
        return True

    def ensure_ready(self):
        with self._lock:
            if self.products:
                return
        try:
            self.refresh_from_url()
            return
        except Exception as e:
            print(f"[WARN] Refresh from URL failed: {e}")
            if self.load_from_disk_cache():
                print("[CATALOG] Loaded from disk cache")
                return
            raise

    def start_background_refresh(self):
        if self._bg_thread and self._bg_thread.is_alive():
            return
        self._stop_event.clear()
        t = threading.Thread(target=self._refresh_loop, name="catalog-refresh", daemon=True)
        t.start()  # ВАЖНО: запуск потока
        self._bg_thread = t

    def stop_background_refresh(self):
        self._stop_event.set()

    def _refresh_loop(self):
        while not self._stop_event.is_set():
            try:
                self.refresh_from_url()
            except Exception as e:
                print(f"[WARN] Periodic refresh failed: {e}")
            self._stop_event.wait(REFRESH_SECONDS)

    # Поиск без пагинации (лист результатов)
    def search_all(self, query: str, requested_region: str, strict_region: bool = False) -> List[Dict[str, Any]]:
        self.ensure_ready()
        q = normalize_text(query)
        q_ru = normalize_text(naive_lat_to_ru(query))
        tokens = [t for t in q.split(" ") if t]
        tokens_ru = [t for t in q_ru.split(" ") if t]

        # --- Category-first detection ---
        # Build a set of distinct normalized section/category names present in the catalog
        with self._lock:
            products_snapshot = list(self.products)
        distinct_cats: Dict[str, str] = {}
        for _p in products_snapshot:
            cat_raw = _p.category or _p.product_section_name or ""
            cat_norm = normalize_text(cat_raw)
            if cat_norm:
                distinct_cats[cat_norm] = cat_raw

        # Choose the longest category name that appears in the query (ru or translit)
        q_norm = normalize_text(query)
        q_norm_ru = normalize_text(naive_lat_to_ru(query))
        matched_cat_norm: Optional[str] = None
        if distinct_cats:
            candidates = [c for c in distinct_cats.keys() if c and (c in q_norm or c in q_norm_ru)]
            if candidates:
                # prefer the longest textual match to avoid substring collisions
                matched_cat_norm = sorted(candidates, key=len, reverse=True)[0]

        # If a category was detected, treat remaining tokens as brand/model filters (match in name only)
        brand_tokens = tokens[:]
        brand_tokens_ru = tokens_ru[:]
        if matched_cat_norm:
            cat_tokens = matched_cat_norm.split()
            brand_tokens     = [t for t in brand_tokens if t not in cat_tokens]
            brand_tokens_ru  = [t for t in brand_tokens_ru if t not in cat_tokens]

        # синонимы по «категорийным» ключам
        SYN: Dict[str, set] = {
            "ламинат": {"ламинат", "laminat", "лам."},
            "плитка": {"плитка", "plitka", "керамика", "керамогранит", "keram"},
            "пвх": {"пвх", "pvh", "винил", "vinyl", "spc", "lvt"},
        }
        syn_set = SYN.get(q, set())

        # эвристика по категориям, если явной категории нет в JSON
        # для «ламинат»: берём м², исключаем ПВХ/винил/SPC/LVT/керам.
        LAMINATE_NEG_MARKERS = {"пвх", "винил", "spc", "lvt", "керам", "плитк", "vinyl", "keram"}
        is_laminate_query = q in SYN and "ламинат" in SYN

        products = products_snapshot

        result: List[Dict[str, Any]] = []
        for p in products:
            crumbs = p.breadcrumbs
            if isinstance(crumbs, list):
                crumbs = " / ".join(map(str, crumbs))
            blob = p._search_blob or normalize_text(" ".join(filter(None, [
                p.name, p.type, p.category, p.section, crumbs or "", p.url, p.id_code
            ])))

            # строгий матч: все токены должны входить (рус+транслит)
            ok_all    = all(t in blob for t in tokens) if tokens else True
            ok_all_ru = all(t in blob for t in tokens_ru) if tokens_ru else True
            # либеральный матч: любой токен
            ok_any    = any(t in blob for t in tokens) if tokens else True
            ok_any_ru = any(t in blob for t in tokens_ru) if tokens_ru else True
            syn_hit   = any(w in blob for w in syn_set) if syn_set else False

            matched = False

            # Category-first logic: if the query contains a known category,
            # require the product to belong to that category. If there are extra terms,
            # match them in the NAME only (brand/model constraint).
            if matched_cat_norm:
                prod_cat_norm = normalize_text(p.category or p.product_section_name or "")
                if prod_cat_norm == matched_cat_norm:
                    if brand_tokens or brand_tokens_ru:
                        name_norm = normalize_text(p.name or "")
                        brand_all = all(t in name_norm for t in brand_tokens) if brand_tokens else True
                        brand_all_ru = all(t in name_norm for t in brand_tokens_ru) if brand_tokens_ru else True
                        matched = brand_all or brand_all_ru
                    else:
                        matched = True
            else:
                # default relevance (previous behavior)
                matched = (ok_all or ok_all_ru) or syn_hit or (ok_any or ok_any_ru)

            # Категорийная эвристика для «ламинат»
            if not matched and is_laminate_query:
                # единица измерения м² и нет негативных маркеров
                is_m2 = (p.measure_code or "").replace("²", "2").strip().lower() in {"м2", "м^2", "m2"}
                has_neg = any(m in blob for m in LAMINATE_NEG_MARKERS)
                if is_m2 and not has_neg:
                    matched = True

            if not matched:
                continue

            # цена: сначала запрошенный регион
            price_block = p.prices.get(requested_region)
            price = price_block["price"] if price_block else None
            price_old = price_block["price_old"] if price_block else None
            used_region = requested_region if price is not None else None

            # если нет цены в запрошенном регионе
            if price is None:
                if strict_region:
                    # пропускаем товар
                    continue
                # пробуем по приоритету
                for rk in FALLBACK_PRIORITY:
                    blk = p.prices.get(rk)
                    if blk and blk.get("price") is not None:
                        price = blk["price"]
                        price_old = blk.get("price_old")
                        used_region = rk
                        break
                # если по приоритету нет — берём первую доступную
                if price is None:
                    for rk in sorted(p.prices.keys()):
                        blk = p.prices[rk]
                        if blk and blk.get("price") is not None:
                            price = blk["price"]
                            price_old = blk.get("price_old")
                            used_region = rk
                            break

            # скоринг: покрытие токенов + бонусы
            score = 0
            # base coverage
            score += sum(1 for t in tokens if t and t in blob)
            score += sum(1 for t in tokens_ru if t and t in blob)
            if ok_all or ok_all_ru:
                score += 3
            if syn_hit:
                score += 1
            if matched_cat_norm:
                prod_cat_norm = normalize_text(p.category or p.product_section_name or "")
                if prod_cat_norm == matched_cat_norm:
                    score += 10  # strong boost for category match
                    # extra boost per matched brand token in NAME only
                    name_norm = normalize_text(p.name or "")
                    score += 2 * sum(1 for t in brand_tokens if t in name_norm)
                    score += 2 * sum(1 for t in brand_tokens_ru if t in name_norm)
            if is_laminate_query and matched and p.measure_code and "м" in p.measure_code:
                score += 1

            result.append({
                "name": p.name,
                "url": p.url,
                "img": p.img or p.image,
                "measure_code": p.measure_code,
                "sale": p.sale,
                "is_hit": p.is_hit,
                "discount_percent": p.discount_percent,
                "region": used_region,
                "price": price,
                "price_old": price_old,
                "currency": p.currency,
                "type": p.type,
                "_score": score,
            })

        # сортировка: релевантность ↓, цена ↑, имя
        def price_key(x):
            return (x["price"] is None, x["price"] if x["price"] is not None else float("inf"))
        result.sort(key=lambda x: (-x["_score"],) + price_key(x) + (x["name"] or "",))
        for r in result:
            r.pop("_score", None)
        return result

catalog = Catalog(PRODUCTS_JSON_URL, DISK_CACHE_PATH)

# ─────────────────────────────────────────────────────────────
# Lifespan (вместо deprecated on_event)
# ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        catalog.ensure_ready()
    except Exception as e:
        print(f"[WARN] Catalog initial load failed: {e}")
    catalog.start_background_refresh()
    yield
    catalog.stop_background_refresh()

# ─────────────────────────────────────────────────────────────
# FastAPI
# ─────────────────────────────────────────────────────────────
app = FastAPI(
    title="Upravdom Search API",
    version="1.9.0",
    lifespan=lifespan,
    docs_url="/swagger",
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Root redirect to Swagger UI
@app.get("/", include_in_schema=False)
def root_redirect():
    return RedirectResponse(url="/swagger")

class SearchResponse(BaseModel):
    query: str
    requested_region: str
    total: int
    page: int
    per_page: int
    pages: int
    has_prev: bool
    has_next: bool
    items: List[Dict[str, Any]]

# POST body model for search
class SearchBody(BaseModel):
    q: str
    page: int = 1
    per_page: int = DEFAULT_PER_PAGE
    strict_region: bool = False

def paginate_and_respond(request: Request, q: str, page: int, per_page: int, strict_region: bool) -> JSONResponse:
    catalog.ensure_ready()
    with catalog._lock:
        known = set(catalog.known_regions.keys())

    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or ""
    requested_region = hard_region_from_host(host, known)

    try:
        all_items = catalog.search_all(q, requested_region, strict_region=strict_region)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Catalog not available: {e}")

    total = len(all_items)
    pages = max(1, (total + per_page - 1) // per_page)
    if page > pages:
        page = pages

    start = (page - 1) * per_page
    end = start + per_page
    slice_items = all_items[start:end]

    payload = {
        "query": q,
        "requested_region": requested_region,
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": pages,
        "has_prev": page > 1,
        "has_next": page < pages,
        "items": slice_items,
    }

    etag = make_etag("v1", q, str(page), str(per_page), requested_region, str(catalog.mtime), "strict" if strict_region else "nostrict")
    if_none_match = request.headers.get("if-none-match")
    if if_none_match and if_none_match == etag:
        return JSONResponse(status_code=304, content=None, headers={"ETag": etag})
    return JSONResponse(content=payload, headers={"Cache-Control": "public, max-age=30", "ETag": etag})

# Вариант 1: query string (?q=...)
@app.get("/search", response_model=SearchResponse)
def search_q(request: Request,
             q: str = Query(..., min_length=1),
             page: int = Query(1, ge=1),
             per_page: int = Query(DEFAULT_PER_PAGE, ge=1, le=MAX_PER_PAGE),
             strict_region: bool = Query(False)):
    return paginate_and_respond(request, q, page, per_page, strict_region)

# POST /search endpoint
@app.post("/search", response_model=SearchResponse)
def search_post(request: Request, body: SearchBody):
    return paginate_and_respond(request, body.q, body.page, body.per_page, body.strict_region)

# Вариант 2: человекочитаемый путь /search/ламинат
@app.get("/search/{q:path}", response_model=SearchResponse)
def search_path(request: Request,
                q: str,
                page: int = Query(1, ge=1),
                per_page: int = Query(DEFAULT_PER_PAGE, ge=1, le=MAX_PER_PAGE),
                strict_region: bool = Query(False)):
    # Be tolerant: treat plus as space and collapse multiple spaces
    q = q.replace("+", " ")
    q = WHITESPACE_RE.sub(" ", q).strip()
    return paginate_and_respond(request, q, page, per_page, strict_region)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/debug")
def debug():
    try:
        catalog.ensure_ready()
        with catalog._lock:
            return {
                "source_url": catalog.source_url,
                "products_loaded": len(catalog.products),
                "known_regions": sorted(catalog.known_regions.keys()),
                "mtime": catalog.mtime,
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/reload")
def reload_now():
    try:
        catalog.refresh_from_url()
        return {"status": "ok", "refreshed_at": time.time()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("search_service:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)