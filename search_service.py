#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, time, re, unicodedata, hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from collections import Counter

from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# -----------------------------
# Конфиг
# -----------------------------
PRODUCTS_JSON_PATH = os.getenv("PRODUCTS_JSON", "/mnt/data/products_unified.json")
MAX_PER_PAGE = 20  # пагинация Управдом
DEFAULT_PER_PAGE = 20

# -----------------------------
# Утилиты
# -----------------------------
WHITESPACE_RE = re.compile(r"\s+", re.UNICODE)

def normalize_text(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFKC", s).replace("Ё","Е").replace("ё","е")
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
    if v is None: return None
    try: return float(v)
    except: return None

def coerce_prices_map(prices_field: Union[List[Dict[str, Any]], Dict[str, Any], None]) -> Dict[str, Dict[str, Optional[float]]]:
    """
    Приводим к виду: {"spb": {"price": 2500.0, "price_old": None}, ...}
    Поддержаны:
      - [{"spb":{"price":..,"price_old":..}}, {"msk":{...}}]
      - {"spb":{"price":..},"msk":{..}}
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
                    "price_old": _to_float(payload.get("price_old"))
                }
        return res
    if isinstance(prices_field, list):
        for chunk in prices_field:
            if not isinstance(chunk, dict): continue
            for rk, payload in chunk.items():
                if isinstance(payload, dict):
                    res[str(rk).lower()] = {
                        "price": _to_float(payload.get("price")),
                        "price_old": _to_float(payload.get("price_old"))
                    }
        return res
    return res

def extract_subdomain(host: str) -> Optional[str]:
    host = (host or "").split(",")[0].strip().lower().split(":")[0]
    parts = host.split(".")
    # spb.upravdom.com -> spb ; upravdom.com -> None
    if len(parts) >= 3:
        return parts[0]
    return None

def hard_region_from_host(host: str, known_regions: set) -> str:
    """
    Жёсткое правило:
      - нет субдомена → 'msk'
      - есть субдомен и он среди известных ключей → он
      - иначе → 'msk'
    """
    sub = extract_subdomain(host)
    if not sub:
        return "msk"
    sub = sub.lower()
    return sub if sub in known_regions else "msk"

# -----------------------------
# Модели и каталог
# -----------------------------
class Product(BaseModel):
    name: str
    url: Optional[str] = None
    img: Optional[str] = None
    measure_code: Optional[str] = None
    sale: Optional[bool] = None
    is_hit: Optional[bool] = None
    discount_percent: Optional[float] = None
    prices: Dict[str, Dict[str, Optional[float]]] = {}

    # доп.поля (если есть)
    image: Optional[str] = None
    id_code: Optional[str] = None
    currency: Optional[str] = "RUB"
    type: Optional[str] = None

class Catalog:
    def __init__(self, json_path: str):
        self.json_path = Path(json_path)
        self.products: List[Product] = []
        self._mt: float = 0.0
        self.known_regions: Counter[str] = Counter()

    @property
    def mtime(self) -> float:
        return self._mt

    def _load(self) -> None:
        if not self.json_path.exists():
            raise FileNotFoundError(f"Файл каталога не найден: {self.json_path}")
        text = self.json_path.read_text(encoding="utf-8").strip()

        # JSON-массив или NDJSON
        items: List[Dict[str, Any]] = []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                items = parsed
            else:
                raise ValueError
        except Exception:
            items = []
            for line in text.splitlines():
                line = line.strip()
                if not line: continue
                try: items.append(json.loads(line))
                except Exception: continue

        self.products = []
        self.known_regions = Counter()

        for it in items:
            prices_map = coerce_prices_map(it.get("prices"))
            if not prices_map and "price" in it:
                prices_map = {"msk": {"price": _to_float(it.get("price")), "price_old": None}}

            self.known_regions.update(prices_map.keys())

            self.products.append(Product(
                name = it.get("name") or "",
                url  = it.get("url"),
                img  = it.get("img") or it.get("image") or it.get("picture"),
                measure_code = it.get("measure_code"),
                sale  = it.get("sale"),
                is_hit= it.get("is_hit"),
                discount_percent = it.get("discount_percent"),
                prices = prices_map,

                image = it.get("image") or it.get("picture"),
                id_code = it.get("id_code"),
                currency = it.get("currency") or "RUB",
                type = it.get("type"),
            ))

        self._mt = self.json_path.stat().st_mtime

    def ensure_loaded(self) -> None:
        cur = self.json_path.stat().st_mtime
        if not self.products or cur != self._mt:
            self._load()

    def search_all(self, query: str, requested_region: str) -> List[Dict[str, Any]]:
        """
        Возвращает полный список совпадений (без пагинации).
        Цены берём из requested_region, иначе — первая доступная цена.
        """
        self.ensure_loaded()

        q = normalize_text(query)
        q_ru = normalize_text(naive_lat_to_ru(query))
        tokens = [t for t in q.split(" ") if t]
        tokens_ru = [t for t in q_ru.split(" ") if t]

        result: List[Dict[str, Any]] = []

        for p in self.products:
            name_norm = normalize_text(p.name)
            ok_tokens    = all(t in name_norm for t in tokens) if tokens else True
            ok_tokens_ru = all(t in name_norm for t in tokens_ru) if tokens_ru else True
            if not (ok_tokens or ok_tokens_ru):
                continue

            # Цена из нужного региона, иначе — первая доступная
            price_block = p.prices.get(requested_region)
            price = price_block["price"] if price_block else None
            price_old = price_block["price_old"] if price_block else None
            used_region = requested_region if price is not None else None

            if price is None:
                for rk in sorted(p.prices.keys()):
                    blk = p.prices[rk]
                    if blk and blk.get("price") is not None:
                        price = blk["price"]
                        price_old = blk.get("price_old")
                        used_region = rk
                        break

            score = 0
            if ok_tokens or ok_tokens_ru:
                score += sum(1 for t in tokens if t and t in name_norm)
                score += sum(1 for t in tokens_ru if t and t in name_norm)

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

catalog = Catalog(PRODUCTS_JSON_PATH)

# -----------------------------
# FastAPI
# -----------------------------
app = FastAPI(title="Upravdom Search API", version="1.4.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS"],
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

def make_etag(*parts: str) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(p.encode("utf-8"))
    return '"' + h.hexdigest()[:32] + '"'  # короткий etag

@app.get("/search", response_model=SearchResponse)
def search(request: Request,
           q: str = Query(..., min_length=1, description="Поисковый запрос"),
           page: int = Query(1, ge=1),
           per_page: int = Query(DEFAULT_PER_PAGE, ge=1, le=MAX_PER_PAGE)):
    # регион строго из Host
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or ""
    # сначала подгрузим каталог, чтобы получить известные регионы
    catalog.ensure_loaded()
    requested_region = hard_region_from_host(host, set(catalog.known_regions.keys()))

    try:
        all_items = catalog.search_all(q, requested_region)
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))

    total = len(all_items)
    pages = max(1, (total + per_page - 1) // per_page)
    # если page > pages — возвращаем пустую выдачу, но корректную пагинацию
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

    # ETag = hash(q, page, per_page, requested_region, file_mtime)
    etag = make_etag(q, str(page), str(per_page), requested_region, str(catalog.mtime))
    if_none_match = request.headers.get("if-none-match")
    if if_none_match and if_none_match == etag:
        return JSONResponse(status_code=304, content=None, headers={"ETag": etag})

    return JSONResponse(
        content=payload,
        headers={
            "Cache-Control": "public, max-age=30",
            "ETag": etag,
        }
    )

@app.get("/health")
def health(): return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("search_service:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)



# Локально проверить
# Без субдомена → регион msk
# curl -H "Host: upravdom.com" "http://127.0.0.1:8000/search?q=ламинат&page=1&per_page=20"
#
# С субдоменом → spb
# curl -H "Host: spb.upravdom.com" \
#   "http://127.0.0.1:8000/search?q=ламинат&page=2&per_page=20"
#
# Кеширование (If-None-Match)
# ETAG=$(curl -si -H "Host: upravdom.com" "http://127.0.0.1:8000/search?q=ламинат" | grep -i etag | awk '{print $2}')
# curl -si -H "Host: upravdom.com" -H "If-None-Match: $ETAG" "http://127.0.0.1:8000/search?q=ламинат"

# Конфигурация NGINX
# location /api/search {
#     proxy_pass http://127.0.0.1:8000/search;
#     proxy_set_header Host $host;
#     proxy_set_header X-Forwarded-Host $host;
#     proxy_set_header X-Real-IP $remote_addr;
#     proxy_read_timeout 5s;
#     add_header Access-Control-Allow-Origin *;
#     add_header Access-Control-Allow-Methods GET,OPTIONS;
# }