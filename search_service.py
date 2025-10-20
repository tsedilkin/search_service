#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import re
import sqlite3
import unicodedata
import hashlib
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Tuple
from math import ceil
from collections import Counter
from contextlib import asynccontextmanager, closing
from urllib.parse import urlparse, urlunparse

import requests
from fastapi import FastAPI, Query, Request, HTTPException, Body, Depends
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ─────────────────────────────────────────────────────────────
# Конфиг
# ─────────────────────────────────────────────────────────────
PRODUCTS_JSON_URL = os.getenv("PRODUCTS_JSON_URL", "https://upravdom.com/upload/catalog.json")
REFRESH_SECONDS   = int(os.getenv("REFRESH_SECONDS", "300"))   # авто-обновление каждые N сек
HTTP_TIMEOUT      = float(os.getenv("HTTP_TIMEOUT", "30.0"))   # таймаут HTTP, сек
DISK_CACHE_PATH   = Path(os.getenv("DISK_CACHE_PATH", "./catalog_cache.json"))

MAX_PER_PAGE      = 20
DEFAULT_PER_PAGE  = 20

# Очерёдность регионов для фоллбека, если в запрошенном регионе нет цены
FALLBACK_PRIORITY = ["msk", "spb", "moscow", "ekb", "nsk", "nn", "krasnodar", "krd", "adler"]

# Аналитика
DB_PATH        = Path(os.getenv("ANALYTICS_DB", "./analytics.db"))
ADMIN_TOKEN    = os.getenv("ADMIN_TOKEN", "devtoken")   # доступ к /admin?token=...
KEEP_QUERY_IPS = os.getenv("KEEP_QUERY_IPS", "1") == "1"

# ─────────────────────────────────────────────────────────────
# Утилиты
# ─────────────────────────────────────────────────────────────
WHITESPACE_RE = re.compile(r"\s+", re.UNICODE)

def normalize_text(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFKC", s).replace("Ё", "Е").replace("ё", "е")
    s = s.casefold()
    return WHITESPACE_RE.sub(" ", s).strip()

def normalize_compact(s: str) -> str:
    """Компакт для артикулов/цифр: только буквы/цифры, без пробелов и знаков."""
    s = normalize_text(s)
    return re.sub(r"[^a-z0-9а-я]+", "", s)

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
    breadcrumbs: Optional[Union[str, List[str]]] = None

    # внутренние поля для быстрого поиска
    _search_blob: Optional[str] = None
    _search_blob_compact: Optional[str] = None

# ─────────────────────────────────────────────────────────────
# Каталог
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
                sale  = it.get("sale"),
                is_hit= it.get("is_hit"),
                discount_percent = it.get("discount_percent"),
                prices = prices_map,
                image = it.get("image") or it.get("picture"),
                id_code = it.get("id_code") or it.get("code") or it.get("sku"),
                currency = it.get("currency") or "RUB",
                type = it.get("type") or it.get("category"),
                category = it.get("category") or it.get("section"),
                section = it.get("section"),
                breadcrumbs = it.get("breadcrumbs") or it.get("path") or it.get("trail"),
            )

            crumbs = p.breadcrumbs
            if isinstance(crumbs, list):
                crumbs = " / ".join(map(str, crumbs))
            blob_parts = [
                p.name or "",
                p.type or "",
                p.category or "",
                p.section or "",
                crumbs or "",
                p.url or "",
                p.id_code or "",
            ]
            blob = normalize_text(" ".join(part for part in blob_parts if part))
            p._search_blob = blob
            p._search_blob_compact = normalize_compact(blob)

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
        t.start()
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

    # Поиск
    def search_all(self, query: str, requested_region: str, strict_region: bool = False) -> List[Dict[str, Any]]:
        self.ensure_ready()

        q_lat = normalize_text(query)                 # латиница как есть
        q_ru  = normalize_text(naive_lat_to_ru(query))# «псевдо-ру»
        tokens_lat = [t for t in q_lat.split(" ") if t]
        tokens_ru  = [t for t in q_ru.split(" ") if t]

        STOP = {"quick", "step", "quickstep", "qs"}   # брендовые стоп-слова

        # сильные (с цифрами)
        strong_lat = [t for t in tokens_lat if any(ch.isdigit() for ch in t)]
        strong_ru  = [t for t in tokens_ru  if any(ch.isdigit() for ch in t)]
        # слабые (без цифр, не стоп-слова)
        weak_lat = [t for t in tokens_lat if not any(ch.isdigit() for ch in t) and t not in STOP]
        weak_ru  = [t for t in tokens_ru  if not any(ch.isdigit() for ch in t) and t not in STOP]

        with self._lock:
            products = list(self.products)

        result: List[Dict[str, Any]] = []
        for p in products:
            blob = p._search_blob or ""
            blob_compact = p._search_blob_compact or normalize_compact(blob)

            # 1) сильные — «все обязательны» по схеме (LAT ИЛИ RU)
            def all_in_compact(need: List[str]) -> bool:
                return all(normalize_compact(t) in blob_compact for t in need) if need else True

            strong_ok = True
            if strong_lat or strong_ru:
                strong_ok = (all_in_compact(strong_lat) or all_in_compact(strong_ru))
            if not strong_ok:
                continue

            # 2) слабые — «все обязательны» по схеме (LAT ИЛИ RU)
            def all_in_blob(need: List[str]) -> bool:
                return all(t in blob for t in need) if need else True

            weak_ok = True
            if weak_lat or weak_ru:
                weak_ok = (all_in_blob(weak_lat) or all_in_blob(weak_ru))
            if not weak_ok:
                continue

            # 3) цены/регион
            price_block = p.prices.get(requested_region)
            price = price_block["price"] if price_block else None
            price_old = price_block["price_old"] if price_block else None
            used_region = requested_region if price is not None else None

            if price is None:
                if strict_region:
                    continue
                for rk in FALLBACK_PRIORITY:
                    blk = p.prices.get(rk)
                    if blk and blk.get("price") is not None:
                        price = blk["price"]
                        price_old = blk.get("price_old")
                        used_region = rk
                        break
                if price is None:
                    for rk in sorted(p.prices.keys()):
                        blk = p.prices[rk]
                        if blk and blk.get("price") is not None:
                            price = blk["price"]
                            price_old = blk.get("price_old")
                            used_region = rk
                            break

            # 4) скоринг
            score = 0
            if strong_lat or strong_ru:
                score += 5
            score += len(weak_lat) or len(weak_ru)

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

        def price_key(x):
            return (x["price"] is None, x["price"] if x["price"] is not None else float("inf"))
        result.sort(key=lambda x: (-x["_score"],) + price_key(x) + (x["name"] or "",))
        for r in result:
            r.pop("_score", None)
        return result

catalog = Catalog(PRODUCTS_JSON_URL, DISK_CACHE_PATH)

# ─────────────────────────────────────────────────────────────
# Аналитика (SQLite)
# ─────────────────────────────────────────────────────────────
def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS searches(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            ip TEXT,
            ua TEXT,
            host TEXT,
            region TEXT,
            q TEXT NOT NULL,
            page INTEGER,
            per_page INTEGER,
            total INTEGER,
            took_ms INTEGER
        )""")
        con.execute("""
        CREATE TABLE IF NOT EXISTS clicks(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            ip TEXT,
            ua TEXT,
            host TEXT,
            region TEXT,
            q TEXT,
            item_name TEXT,
            item_url TEXT,
            price REAL
        )""")
        con.commit()

def db_execute(sql: str, args: Tuple[Any, ...] = ()):
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.execute(sql, args)
        con.commit()

def db_query(sql: str, args: Tuple[Any, ...] = ()) -> List[sqlite3.Row]:
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        cur = con.execute(sql, args)
        return cur.fetchall()

def get_client_ip(request: Request) -> str:
    if not KEEP_QUERY_IPS:
        return ""
    xff = request.headers.get("x-forwarded-for", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else ""

# ─────────────────────────────────────────────────────────────
# Lifespan
# ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
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
app = FastAPI(title="Upravdom Search API", version="2.2.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

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

class SearchRequest(BaseModel):
    q: str
    page: int = 1
    per_page: int = DEFAULT_PER_PAGE
    strict_region: bool = False

def log_search(request: Request, region: str, q: str, page: int, per_page: int, total: int, started: float):
    took_ms = int((time.time() - started) * 1000)
    ip = get_client_ip(request)
    ua = request.headers.get("user-agent", "")[:400]
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or ""
    db_execute(
        "INSERT INTO searches(ts, ip, ua, host, region, q, page, per_page, total, took_ms) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (int(time.time()), ip, ua, host, region, q, page, per_page, total, took_ms)
    )

def paginate_and_respond(request: Request, q: str, page: int, per_page: int, strict_region: bool) -> JSONResponse:
    start = time.time()
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

    start_i = (page - 1) * per_page
    end_i = start_i + per_page
    slice_items = all_items[start_i:end_i]

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

    # лог
    try:
        log_search(request, requested_region, q, page, per_page, total, start)
    except Exception as e:
        print(f"[WARN] log_search failed: {e}")

    etag = make_etag("v1", q, str(page), str(per_page), requested_region, str(catalog.mtime), "strict" if strict_region else "nostrict")
    if_none_match = request.headers.get("if-none-match")
    if if_none_match and if_none_match == etag:
        return JSONResponse(status_code=304, content=None, headers={"ETag": etag})
    return JSONResponse(content=payload, headers={"Cache-Control": "public, max-age=30", "ETag": etag})

# GET: ?q=...
@app.get("/search", response_model=SearchResponse)
def search_q(request: Request,
             q: str = Query(..., min_length=1),
             page: int = Query(1, ge=1),
             per_page: int = Query(DEFAULT_PER_PAGE, ge=1, le=MAX_PER_PAGE),
             strict_region: bool = Query(False)):
    return paginate_and_respond(request, q, page, per_page, strict_region)

# GET: /search/{q}
@app.get("/search/{q}", response_model=SearchResponse)
def search_path(request: Request,
                q: str,
                page: int = Query(1, ge=1),
                per_page: int = Query(DEFAULT_PER_PAGE, ge=1, le=MAX_PER_PAGE),
                strict_region: bool = Query(False)):
    return paginate_and_respond(request, q, page, per_page, strict_region)

# POST: JSON {q, page, per_page, strict_region}
@app.post("/search", response_model=SearchResponse)
def search_post(request: Request, body: SearchRequest = Body(...)):
    per_page = min(MAX_PER_PAGE, max(1, body.per_page))
    page = max(1, body.page)
    return paginate_and_respond(request, body.q, page, per_page, body.strict_region)

# Клик-трекер: редирект на целевой URL с логированием
@app.get("/out")
def out(request: Request, u: str = Query(..., description="target URL"), name: str = Query("", description="item name"), price: float = Query(None)):
    try:
        parsed = urlparse(u)
        if parsed.scheme not in ("http", "https"):
            return PlainTextResponse("Bad redirect", status_code=400)
        safe_url = urlunparse(parsed)
    except Exception:
        return PlainTextResponse("Bad redirect", status_code=400)

    try:
        ip = get_client_ip(request)
        ua = request.headers.get("user-agent", "")[:400]
        host = request.headers.get("x-forwarded-host") or request.headers.get("host") or ""
        rows = db_query("SELECT q, region FROM searches WHERE ip=? ORDER BY id DESC LIMIT 1", (ip,))
        q_src, region_src = (rows[0]["q"], rows[0]["region"]) if rows else ("", "")
        db_execute(
            "INSERT INTO clicks(ts, ip, ua, host, region, q, item_name, item_url, price) VALUES(?,?,?,?,?,?,?,?,?)",
            (int(time.time()), ip, ua, host, region_src, q_src, name[:200] if name else "", safe_url[:2000], float(price) if price is not None else None)
        )
    except Exception as e:
        print(f"[WARN] log click failed: {e}")

    return RedirectResponse(safe_url, status_code=302)

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

# ─────────────────────────────────────────────────────────────
# Простейшая админ-панель /admin?token=...
# ─────────────────────────────────────────────────────────────
ADMIN_HTML = """
<!doctype html>
<html><head>
<meta charset="utf-8"/>
<title>Search Monitor</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
body{font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Helvetica,Arial; margin:24px;}
h1{font-size:20px;margin:0 0 12px;}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:12px;margin-bottom:16px;}
.card{border:1px solid #e5e7eb;border-radius:12px;padding:12px;background:#fff;box-shadow:0 1px 2px rgba(0,0,0,.05)}
table{border-collapse:collapse;width:100%}
th,td{border-bottom:1px solid #eee;padding:8px 6px;text-align:left;font-size:13px;vertical-align:top}
th{background:#fafafa}
code{background:#f3f4f6;padding:1px 4px;border-radius:4px}
small{color:#6b7280}
.badge{display:inline-block;padding:2px 6px;border-radius:999px;background:#eef2ff;color:#3730a3;font-size:12px}
</style>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head><body>
<h1>Search Monitor</h1>
<div class="cards">
  <div class="card"><div>Запросов за 24ч: <b id="m1">–</b></div><div>CTR (клики/запросы): <b id="ctr">–</b></div></div>
  <div class="card"><div>Уник. запросов за 24ч: <b id="m2">–</b></div><div>Средн. выдача (total): <b id="avg">–</b></div></div>
</div>

<canvas id="chart" height="120"></canvas>

<h2>Последние запросы</h2>
<table id="qtable"><thead><tr>
  <th>Время</th><th>q</th><th>total</th><th>регион</th><th>page/per</th><th>IP</th><th>UA</th>
</tr></thead><tbody></tbody></table>

<h2>Последние клики</h2>
<table id="ctable"><thead><tr>
  <th>Время</th><th>q</th><th>name</th><th>url</th><th>цена</th><th>регион</th><th>IP</th>
</tr></thead><tbody></tbody></table>

<script>
const token = new URLSearchParams(location.search).get('token');
async function api(path){
  const r = await fetch(path + (path.includes('?')?'&':'?') + 'token=' + encodeURIComponent(token));
  if(!r.ok){document.body.innerHTML='<p>Auth failed</p>';throw new Error('auth');}
  return r.json();
}
function t(ts){const d=new Date(ts*1000);return d.toLocaleString();}
function esc(s){return (s||'').toString().replace(/[&<>"]/g, m=>({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;' }[m]));}

(async ()=>{
  const stats = await api('/admin/api/stats24');
  document.getElementById('m1').textContent = stats.searches_24h;
  document.getElementById('m2').textContent = stats.unique_q_24h;
  document.getElementById('avg').textContent = stats.avg_total_24h.toFixed(1);
  document.getElementById('ctr').textContent = (stats.ctr_24h*100).toFixed(1)+'%';

  const ctx = document.getElementById('chart').getContext('2d');
  new Chart(ctx, {
    type:'line',
    data:{labels:stats.series.hours, datasets:[
      {label:'Запросы', data:stats.series.counts, tension:.2},
      {label:'Клики', data:stats.series.clicks, tension:.2}
    ]},
    options:{responsive:true, plugins:{legend:{position:'bottom'}}}
  });

  const qs = await api('/admin/api/last_searches?limit=50');
  const tb = document.querySelector('#qtable tbody');
  tb.innerHTML = qs.map(r=>`<tr>
    <td>${t(r.ts)}</td><td><code>${esc(r.q)}</code></td>
    <td>${r.total}</td><td>${esc(r.region||'')}</td>
    <td>${r.page}/${r.per_page}</td><td>${esc(r.ip||'')}</td>
    <td><small>${esc(r.ua||'')}</small></td>
  </tr>`).join('');

  const cs = await api('/admin/api/last_clicks?limit=50');
  const tb2 = document.querySelector('#ctable tbody');
  tb2.innerHTML = cs.map(r=>`<tr>
    <td>${t(r.ts)}</td><td><code>${esc(r.q||'')}</code></td>
    <td>${esc(r.item_name||'')}</td>
    <td><a href="${esc(r.item_url)}" target="_blank">${esc(r.item_url)}</a></td>
    <td>${r.price==null?'':r.price}</td><td>${esc(r.region||'')}</td><td>${esc(r.ip||'')}</td>
  </tr>`).join('');
})();
</script>
</body></html>
"""

def require_admin_token(request: Request):
    token = request.query_params.get("token")
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True

@app.get("/admin", response_class=HTMLResponse)
def admin_ui(ok: bool = Depends(require_admin_token)):
    return HTMLResponse(ADMIN_HTML)

@app.get("/admin/api/stats24")
def admin_stats24(ok: bool = Depends(require_admin_token)):
    now = int(time.time())
    day_ago = now - 24*3600
    rows_s = db_query("SELECT * FROM searches WHERE ts>=? ORDER BY ts ASC", (day_ago,))
    rows_c = db_query("SELECT * FROM clicks WHERE ts>=? ORDER BY ts ASC", (day_ago,))

    searches_24h = len(rows_s)
    unique_q_24h = len({r["q"] for r in rows_s})
    avg_total_24h = (sum(r["total"] for r in rows_s)/searches_24h) if searches_24h else 0.0
    ctr_24h = (len(rows_c)/searches_24h) if searches_24h else 0.0

    hours = []; counts = []; clicks = []
    bucket = {h:0 for h in range(24)}; bucket_c = {h:0 for h in range(24)}
    for r in rows_s: bucket[(r["ts"]//3600)%24]+=1
    for r in rows_c: bucket_c[(r["ts"]//3600)%24]+=1
    for h in range(24):
        hours.append(f"{h:02d}:00"); counts.append(bucket[h]); clicks.append(bucket_c[h])

    return {
        "searches_24h": searches_24h,
        "unique_q_24h": unique_q_24h,
        "avg_total_24h": avg_total_24h,
        "ctr_24h": ctr_24h,
        "series": {"hours": hours, "counts": counts, "clicks": clicks},
    }

@app.get("/admin/api/last_searches")
def admin_last_searches(limit: int = 50, ok: bool = Depends(require_admin_token)):
    limit = max(1, min(500, limit))
    rows = db_query("SELECT ts, ip, ua, host, region, q, page, per_page, total FROM searches ORDER BY id DESC LIMIT ?", (limit,))
    return [dict(r) for r in rows]

@app.get("/admin/api/last_clicks")
def admin_last_clicks(limit: int = 50, ok: bool = Depends(require_admin_token)):
    limit = max(1, min(500, limit))
    rows = db_query("SELECT ts, ip, ua, host, region, q, item_name, item_url, price FROM clicks ORDER BY id DESC LIMIT ?", (limit,))
    return [dict(r) for r in rows]

# ─────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("search_service:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)