#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import re
import sqlite3
import unicodedata
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Tuple
from collections import Counter
from contextlib import asynccontextmanager
from urllib.parse import urlparse, urlunparse

import requests
from fastapi import FastAPI, Query, Request, HTTPException, Body
from fastapi.responses import JSONResponse, RedirectResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ─────────────────────────────────────────────────────────────
# Конфиг
# ─────────────────────────────────────────────────────────────
PRODUCTS_JSON_URL = os.getenv("PRODUCTS_JSON_URL", "https://upravdom.com/upload/catalog.json")
REFRESH_SECONDS   = int(os.getenv("REFRESH_SECONDS", "300"))
HTTP_TIMEOUT      = float(os.getenv("HTTP_TIMEOUT", "30.0"))
DISK_CACHE_PATH   = Path(os.getenv("DISK_CACHE_PATH", "./catalog_cache.json"))

MAX_PER_PAGE      = 20
DEFAULT_PER_PAGE  = 20

# Базовый домен для редиректа на карточки (всегда префиксуем путь из JSON)
SITE_BASE = os.getenv("SITE_BASE", "https://upravdom.com")

# Ключи регионов для fallback (только канонические)
FALLBACK_PRIORITY = ["msk", "spb", "ekb", "nsk", "nn", "krd", "adler"]

# Синонимы регионов (домены/ключи из JSON -> канонический ключ)
REGION_ALIASES = {
    # Москва
    "msk": "msk", "moscow": "msk",
    # Санкт-Петербург
    "spb": "spb", "sankt-peterburg": "spb", "spb-city": "spb", "piter": "spb", "spbru": "spb",
    # Екатеринбург
    "ekb": "ekb", "yekaterinburg": "ekb", "ekaterinburg": "ekb",
    # Новосибирск
    "nsk": "nsk", "novosibirsk": "nsk",
    # Нижний Новгород
    "nn": "nn", "nizhny-novgorod": "nn", "nizhnij-novgorod": "nn", "nizhniy-novgorod": "nn",
    # Краснодар
    "krd": "krd", "krasnodar": "krd",
    # Прочие
    "adler": "adler",
}
def normalize_region_key(k: str) -> str:
    k = (k or "").strip().lower()
    return REGION_ALIASES.get(k, k)

# Аналитика
DB_PATH        = Path(os.getenv("ANALYTICS_DB", "./analytics.db"))
KEEP_QUERY_IPS = os.getenv("KEEP_QUERY_IPS", "1") == "1"

# API keys (optional). If at least one key is provided via env, /search endpoints will require it.
# Set env: API_KEYS="key1,key2,..."
API_KEYS = {
    k.strip() for k in (os.getenv("API_KEYS", "").split(",") if os.getenv("API_KEYS") else [])
    if k.strip()
}

def _get_api_key(request: Request) -> Optional[str]:
    # Header takes precedence
    h = request.headers.get("x-api-key")
    if h:
        return h.strip()
    # fallback to query param
    qp = request.query_params.get("api_key")
    if qp:
        return qp.strip()
    return None

def ensure_api_allowed(request: Request):
    """Require a valid API key when API_KEYS is configured. No-op if not configured."""
    if not API_KEYS:
        return
    key = _get_api_key(request)
    if not key or key not in API_KEYS:
        raise HTTPException(status_code=401, detail="Unauthorized: missing or invalid API key")

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
    Приводим prices к виду: {"spb": {"price": 2500.0, "price_old": None}, ...}
    Поддерживаются словарь и список словарей с регионами.
    """
    res: Dict[str, Dict[str, Optional[float]]] = {}
    if prices_field is None:
        return res
    if isinstance(prices_field, dict):
        for rk, payload in prices_field.items():
            if isinstance(payload, dict):
                key = normalize_region_key(rk)
                res[key] = {"price": _to_float(payload.get("price")),
                            "price_old": _to_float(payload.get("price_old"))}
        return res
    if isinstance(prices_field, list):
        for chunk in prices_field:
            if not isinstance(chunk, dict):
                continue
            for rk, payload in chunk.items():
                if isinstance(payload, dict):
                    key = normalize_region_key(rk)
                    res[key] = {"price": _to_float(payload.get("price")),
                                "price_old": _to_float(payload.get("price_old"))}
        return res
    return res

def extract_subdomain(host: str) -> Optional[str]:
    host = (host or "").split(",")[0].strip().lower().split(":")[0]
    parts = host.split(".")
    if len(parts) >= 3:
        return parts[0]
    return None

def hard_region_from_host(host: str) -> str:
    """
    Берём регион строго из субдомена и нормализуем через алиасы.
    Без субдомена — 'msk'.
    """
    sub = extract_subdomain(host)
    if not sub:
        return "msk"
    return normalize_region_key(sub)

def make_etag(*parts: str) -> str:
    import hashlib
    h = hashlib.sha256()
    for p in parts:
        h.update(p.encode("utf-8"))
    return '"' + h.hexdigest()[:32] + '"'

# ─────────────────────────────────────────────────────────────
# Модель
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

    image: Optional[str] = None
    id_code: Optional[str] = None
    currency: Optional[str] = "RUB"
    type: Optional[str] = None
    category: Optional[str] = None
    section: Optional[str] = None
    section_norm: Optional[str] = None
    breadcrumbs: Optional[Union[str, List[str]]] = None

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
                section = it.get("product_section_name") or it.get("section"),
                breadcrumbs = it.get("breadcrumbs") or it.get("path") or it.get("trail"),
            )
            p.section_norm = normalize_text(p.section) if p.section else None
            crumbs = p.breadcrumbs
            if isinstance(crumbs, list):
                crumbs = " / ".join(map(str, crumbs))
            blob_parts = [p.name or "", p.type or "", p.category or "", p.section or "", crumbs or "", p.url or "", p.id_code or ""]
            blob = normalize_text(" ".join(part for part in blob_parts if part))
            p._search_blob = blob
            p._search_blob_compact = normalize_compact(blob)
            products.append(p)

        with self._lock:
            self.products = products
            self.known_regions = known
            self._mtime_marker = time.time()

    def refresh_from_url(self):
        headers = {"User-Agent": "UpravdomSearch/1.0", "Accept": "application/json"}
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
            self.disk_cache.write_text(json.dumps(items, ensure_ascii=False), encoding="utf-8")
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

    def search_all(self, query: str, requested_region: str, strict_region: bool = False) -> List[Dict[str, Any]]:
        self.ensure_ready()

        q_lat = normalize_text(query)
        q_ru  = normalize_text(naive_lat_to_ru(query))
        tokens_lat = [t for t in q_lat.split(" ") if t]
        tokens_ru  = [t for t in q_ru.split(" ") if t]

        # разделяем токены на сильные (с цифрами/артикулы) и слабые (обычные слова)
        strong_lat = [t for t in tokens_lat if any(ch.isdigit() for ch in t)]
        strong_ru  = [t for t in tokens_ru  if any(ch.isdigit() for ch in t)]
        weak_lat   = [t for t in tokens_lat if not any(ch.isdigit() for ch in t)]
        weak_ru    = [t for t in tokens_ru  if not any(ch.isdigit() for ch in t)]
        # Сильные токены можно объединять (цифры/артикулы).
        strong_need = list(dict.fromkeys(strong_lat + strong_ru))
        # Слабые токены берём из одной «доминирующей» раскладки: если есть латиница — используем её,
        # иначе — русские токены.
        if weak_lat:
            weak_need = list(dict.fromkeys(weak_lat))
        else:
            weak_need = list(dict.fromkeys(weak_ru))

        # секционный фильтр: если в запросе присутствует слово, совпадающее с известным названием раздела,
        # фильтруем результаты по этому разделу. Например: "ламинат 1994" → показываем только раздел "ламинат".
        with self._lock:
            known_sections = {p.section_norm for p in self.products if getattr(p, 'section_norm', None)}
        sec_need = [t for t in tokens_ru if t in known_sections]

        with self._lock:
            products = list(self.products)

        result: List[Dict[str, Any]] = []
        for p in products:
            # если пользователь явно указал раздел (например, "ламинат"), оставляем только такие позиции
            if sec_need:
                if not p.section_norm or p.section_norm not in sec_need:
                    continue
            blob = p._search_blob or ""
            blob_compact = p._search_blob_compact or normalize_compact(blob)

            # утилиты поиска подстрок
            def any_in_compact(need: List[str]) -> bool:
                return any(normalize_compact(t) in blob_compact for t in need) if need else False
            def any_in_blob(need: List[str]) -> bool:
                return any(t in blob for t in need) if need else False
            def all_in_compact(need: List[str]) -> bool:
                return all(normalize_compact(t) in blob_compact for t in need) if need else True
            def all_in_blob(need: List[str]) -> bool:
                return all(t in blob for t in need) if need else True

            has_strong = any_in_compact(strong_need)
            weak_all   = all_in_blob(weak_need) or all_in_compact(weak_need)

            # Правила допуска:
            # 1) Если есть сильные токены — достаточно совпадения ЛЮБОГО из них (артикул/код выигрывает).
            #    Но если слабые тоже есть, то все они должны быть покрыты.
            # 2) Если сильных нет — требуем совпадения всех слабых.
            if strong_need:
                if not has_strong:
                    continue
                if weak_need and not weak_all:
                    continue
            else:
                if weak_need and not weak_all:
                    continue

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
                        price = blk["price"]; price_old = blk.get("price_old"); used_region = rk; break
                if price is None:
                    for rk in sorted(p.prices.keys()):
                        blk = p.prices[rk]
                        if blk and blk.get("price") is not None:
                            price = blk["price"]; price_old = blk.get("price_old"); used_region = rk; break

            score = 0
            # бонус за сильные совпадения
            if has_strong:
                score += 10
                # дополнительный бонус за число совпавших strong токенов
                score += sum(1 for t in strong_need if normalize_compact(t) in blob_compact)
            # бонусы за слабые совпадения
            weak_hits = sum(1 for t in weak_need if (t in blob) or (normalize_compact(t) in blob_compact))
            score += weak_hits

            # phrase bonus (e.g., "impressive ultra" occurring contiguously)
            if weak_need:
                phrase_compact = normalize_compact(" ".join(weak_need))
                if phrase_compact and phrase_compact in blob_compact:
                    score += 5
                # small bonus if all weak tokens are present
                if weak_all:
                    score += len(weak_need)

            result.append({
                "name": p.name, "url": p.url, "img": p.img or p.image, "measure_code": p.measure_code,
                "sale": p.sale, "is_hit": p.is_hit, "discount_percent": p.discount_percent,
                "region": used_region, "price": price, "price_old": price_old,
                "currency": p.currency, "type": p.type, "_score": score,
            })

        def price_key(x):
            return (x["price"] is None, x["price"] if x["price"] is not None else float("inf"))
        result.sort(key=lambda x: (-x["_score"],) + price_key(x) + (x["name"] or "",))
        for r in result:
            r.pop("_score", None)
        return result

catalog = Catalog(PRODUCTS_JSON_URL, DISK_CACHE_PATH)

# ─────────────────────────────────────────────────────────────
# Аналитика / БД
# ─────────────────────────────────────────────────────────────
def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""CREATE TABLE IF NOT EXISTS searches(
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL,
            ip TEXT, ua TEXT, host TEXT, region TEXT, q TEXT NOT NULL,
            page INTEGER, per_page INTEGER, total INTEGER, took_ms INTEGER)""")
        con.execute("""CREATE TABLE IF NOT EXISTS clicks(
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL,
            ip TEXT, ua TEXT, host TEXT, region TEXT, q TEXT,
            item_name TEXT, item_url TEXT, price REAL)""")
        con.commit()

def db_execute(sql: str, args: Tuple[Any, ...] = ()):
    with sqlite3.connect(DB_PATH) as con:
        con.execute(sql, args); con.commit()

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
app = FastAPI(title="Upravdom Search API", version="3.2.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*", "X-API-Key"],
)

# ── модели запросов/ответов
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
    region: Optional[str] = None  # <— явный оверрайд

# ── резолвер региона
def resolve_requested_region(request: Request, region_override: Optional[str] = None) -> str:
    """
    Приоритет:
      1) явный параметр функции (из эндпоинта),
      2) query-параметр ?region=...,
      3) X-Forwarded-Host / Host.
    """
    # Optional explicit domain override: ?domain=spb.upravdom.com or ?domain=spb
    qp_domain = request.query_params.get("domain")
    if qp_domain:
        dom = (qp_domain or "").strip().lower()
        # If it looks like a hostname (has a dot), extract region from subdomain
        if "." in dom:
            return hard_region_from_host(dom)
        # Otherwise treat as a region key/synonym
        return normalize_region_key(dom)
    if region_override:
        return normalize_region_key(region_override)
    qp_region = request.query_params.get("region")
    if qp_region:
        return normalize_region_key(qp_region)
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or ""
    return hard_region_from_host(host)

from datetime import datetime

def _fmt_price(p):
    if p is None:
        return "—"
    try:
        # целые цены без дробей, иначе 2 знака
        return ("{:,.0f}" if float(p).is_integer() else "{:,.2f}").format(float(p)).replace(",", " ")
    except Exception:
        return str(p)

def render_cli(payload: Dict[str, Any]) -> str:
    """Форматированный человекопонятный вывод для CLI."""
    lines = []
    lines.append("Upravdom Search · query=\"{}\" · region={} · page {}/{} (per_page={})".format(
        payload.get("query", ""), payload.get("requested_region", ""), payload.get("page", 1),
        payload.get("pages", 1), payload.get("per_page", 0)))
    lines.append("total: {} | has_prev: {} | has_next: {}".format(
        payload.get("total", 0), payload.get("has_prev", False), payload.get("has_next", False)))
    lines.append("")
    items = payload.get("items", [])
    if not items:
        lines.append("нет результатов")
        return "\n".join(lines) + "\n"

    # шапка таблицы
    header = ["#", "Название", "Цена", "Старая", "Регион", "Ед.", "URL"]
    col_widths = [3, 60, 10, 10, 7, 5, 50]
    def cut(s, w):
        s = "" if s is None else str(s)
        return (s[:w-1] + "…") if len(s) > w else s
    def row_to_str(cols):
        pads = ["<", "<", ">", ">", "<", "<", "<"]
        cells = []
        for i, (c, w) in enumerate(zip(cols, col_widths)):
            txt = cut(c, w)
            if pads[i] == ">":
                cells.append(txt.rjust(w))
            else:
                cells.append(txt.ljust(w))
        return " ".join(cells)

    lines.append(row_to_str(header))
    lines.append("-" * (sum(col_widths) + len(col_widths) - 1))

    for idx, it in enumerate(items, 1):
        name = it.get("name") or "—"
        price = _fmt_price(it.get("price"))
        price_old = _fmt_price(it.get("price_old"))
        region = it.get("region") or "—"
        unit = it.get("measure_code") or "—"
        url = it.get("url") or "—"
        lines.append(row_to_str([str(idx), name, price, price_old, region, unit, url]))

    return "\n".join(lines) + "\n"

def paginate_and_respond(request: Request, q: str, page: int, per_page: int, strict_region: bool, region: Optional[str] = None) -> JSONResponse:
    start = time.time()
    catalog.ensure_ready()

    requested_region = resolve_requested_region(request, region_override=region)

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

    payload = {"query": q, "requested_region": requested_region, "total": total,
               "page": page, "per_page": per_page, "pages": pages,
               "has_prev": page > 1, "has_next": page < pages, "items": slice_items}

    # Если клиент просит человекочитаемый текст: ?format=cli|text|plain или Accept: text/plain
    fmt = (request.query_params.get("format") or "").lower()
    accept = (request.headers.get("accept") or "").lower()
    want_text = fmt in {"cli", "text", "plain"} or "text/plain" in accept
    if want_text:
        from fastapi.responses import PlainTextResponse
        text = render_cli(payload)
        return PlainTextResponse(text, headers={"Cache-Control": "no-store"})

    # лог поиска
    try:
        took_ms = int((time.time() - start) * 1000)
        ip = get_client_ip(request)
        ua = request.headers.get("user-agent", "")[:400]
        host = request.headers.get("x-forwarded-host") or request.headers.get("host") or ""
        db_execute("INSERT INTO searches(ts, ip, ua, host, region, q, page, per_page, total, took_ms) VALUES(?,?,?,?,?,?,?,?,?,?)",
                   (int(time.time()), ip, ua, host, requested_region, q, page, per_page, total, took_ms))
    except Exception as e:
        print(f"[WARN] log_search failed: {e}")

    etag = make_etag("v1", q, str(page), str(per_page), requested_region, str(catalog.mtime), "strict" if strict_region else "nostrict")
    if request.headers.get("if-none-match") == etag:
        return JSONResponse(status_code=304, content=None, headers={"ETag": etag})
    return JSONResponse(content=payload, headers={"Cache-Control": "public, max-age=30", "ETag": etag})

# ── эндпоинты поиска
@app.get("/search", response_model=SearchResponse)
def search_q(request: Request,
             q: str = Query(..., min_length=1),
             page: int = Query(1, ge=1),
             per_page: int = Query(DEFAULT_PER_PAGE, ge=1, le=MAX_PER_PAGE),
             strict_region: bool = Query(False),
             region: Optional[str] = Query(None)):
    ensure_api_allowed(request)
    return paginate_and_respond(request, q, page, per_page, strict_region, region)

@app.get("/search/{q}", response_model=SearchResponse)
def search_path(request: Request, q: str,
                page: int = Query(1, ge=1),
                per_page: int = Query(DEFAULT_PER_PAGE, ge=1, le=MAX_PER_PAGE),
                strict_region: bool = Query(False),
                region: Optional[str] = Query(None)):
    ensure_api_allowed(request)
    return paginate_and_respond(request, q, page, per_page, strict_region, region)

@app.post("/search", response_model=SearchResponse)
def search_post(request: Request, body: SearchRequest = Body(...)):
    ensure_api_allowed(request)
    per_page = min(MAX_PER_PAGE, max(1, body.per_page))
    page = max(1, body.page)
    return paginate_and_respond(request, body.q, page, per_page, body.strict_region, body.region)

# ── диагностика
@app.get("/whoami")
def whoami(request: Request):
    return {
        "x-forwarded-host": request.headers.get("x-forwarded-host"),
        "host": request.headers.get("host"),
        "parsed_subdomain": extract_subdomain(request.headers.get("x-forwarded-host") or request.headers.get("host") or ""),
        "region_param": request.query_params.get("region"),
        "domain_param": request.query_params.get("domain"),
        "requested_region": resolve_requested_region(request, region_override=request.query_params.get("region")),
        "api_key_present": bool(_get_api_key(request)),
    }

@app.api_route("/out", methods=["GET", "HEAD", "POST"])
def out(request: Request, u: str = Query(..., description="target URL"), name: str = Query("", description="item name"), price: float = Query(None)):
    # Всегда редиректим на наш базовый домен, используя то, что пришло в JSON как путь.
    # Принимаем на вход либо относительный путь, либо полный URL — в любом случае забираем только path+query.
    try:
        parsed = urlparse(u)
        path = parsed.path or u  # если это был относительный путь без схемы
        query = ("?" + parsed.query) if parsed.query else ""
        # нормализуем двойные слэши
        safe_url = SITE_BASE.rstrip("/") + "/" + path.lstrip("/") + query
    except Exception:
        return PlainTextResponse("Bad redirect", status_code=400)

    try:
        ip = get_client_ip(request)
        ua = request.headers.get("user-agent", "")[:400]
        host = request.headers.get("x-forwarded-host") or request.headers.get("host") or ""
        with sqlite3.connect(DB_PATH) as con:
            con.execute("""INSERT INTO clicks(ts, ip, ua, host, region, q, item_name, item_url, price)
                           VALUES(?,?,?,?,?,?,?,?,?)""",
                        (int(time.time()), ip, ua, host, "", "", name[:200] if name else "", safe_url[:2000],
                         float(price) if price is not None else None))
            con.commit()
    except Exception as e:
        print(f"[WARN] log click failed: {e}")

    # Для трекинга из JS (sendBeacon POST/HEAD) возвращаем 204 без редиректа
    if request.method != "GET":
        return PlainTextResponse("", status_code=204)
    return RedirectResponse(safe_url, status_code=302)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/debug")
def debug():
    try:
        catalog.ensure_ready()
        with catalog._lock:
            resp = {
                "source_url": catalog.source_url,
                "products_loaded": len(catalog.products),
                "known_regions": sorted(catalog.known_regions.keys()),
                "mtime": catalog.mtime
            }
            host_sample = os.getenv("DEBUG_HOST", "")
            req_region = hard_region_from_host(host_sample) if host_sample else None
            if host_sample:
                resp["host_seen"] = host_sample
                resp["requested_region_from_host"] = req_region
            return resp
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── подключаем админку из отдельного файла admin.py (если есть)
try:
    import admin
    admin.mount_admin(app)
except Exception as e:
    print(f"[WARN] admin module not mounted: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("search_service:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)