# admin.py
import os
import time
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo  # stdlib (Python 3.9+)

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse
from fastapi import Request

DB_PATH     = os.getenv("ANALYTICS_DB", "./analytics.db")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "devtoken")
ADMIN_TZ    = os.getenv("ADMIN_TZ", "Europe/Moscow")  # <— задай свой TZ

router = APIRouter()

# ─────────────────────────────────────────────────────────────
# HTML (тот же, что был, без изменений логики на клиенте)
# ─────────────────────────────────────────────────────────────
ADMIN_HTML = """<!doctype html><html><head>
<meta charset="utf-8"/><title>Search Monitor</title>
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
async function api(path){ const r = await fetch(path + (path.includes('?')?'&':'?') + 'token='+encodeURIComponent(token)); if(!r.ok){document.body.innerHTML='<p>Auth failed</p>'; throw new Error('auth');} return r.json(); }
function t(ts){const d=new Date(ts*1000);return d.toLocaleString();}
function esc(s){return (s||'').toString().replace(/[&<>"]/g, m=>({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;' }[m]));}
(async ()=>{
  const stats = await api('/admin/api/stats24');
  document.getElementById('m1').textContent = stats.searches_24h;
  document.getElementById('m2').textContent = stats.unique_q_24h;
  document.getElementById('avg').textContent = stats.avg_total_24h.toFixed(1);
  document.getElementById('ctr').textContent = (stats.ctr_24h*100).toFixed(1)+'%';
  const ctx = document.getElementById('chart').getContext('2d');
  new Chart(ctx, { type:'line', data:{labels:stats.series.hours, datasets:[
      {label:'Запросы', data:stats.series.counts, tension:.2},
      {label:'Клики', data:stats.series.clicks, tension:.2}
  ]}, options:{responsive:true, plugins:{legend:{position:'bottom'}}}});
  const qs = await api('/admin/api/last_searches?limit=50');
  const tb = document.querySelector('#qtable tbody');
  tb.innerHTML = qs.map(r=>`<tr><td>${t(r.ts)}</td><td><code>${esc(r.q)}</code></td><td>${r.total}</td><td>${esc(r.region||'')}</td><td>${r.page}/${r.per_page}</td><td>${esc(r.ip||'')}</td><td><small>${esc(r.ua||'')}</small></td></tr>`).join('');
  const cs = await api('/admin/api/last_clicks?limit=50');
  const tb2 = document.querySelector('#ctable tbody');
  tb2.innerHTML = cs.map(r=>`<tr><td>${t(r.ts)}</td><td><code>${esc(r.q||'')}</code></td><td>${esc(r.item_name||'')}</td><td><a href="${esc(r.item_url)}" target="_blank">${esc(r.item_url)}</a></td><td>${r.price==null?'':r.price}</td><td>${esc(r.region||'')}</td><td>${esc(r.ip||'')}</td></tr>`).join('');
})();
</script>
</body></html>"""

# ─────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────
def require_admin_token(request: Request):
    token = request.query_params.get("token")
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True

def db_query(sql: str, args=()):
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        cur = con.execute(sql, args)
        return cur.fetchall()

# ─────────────────────────────────────────────────────────────
# routes
# ─────────────────────────────────────────────────────────────
@router.get("/admin", response_class=HTMLResponse)
def admin_ui(_: bool = Depends(require_admin_token)):
    return HTMLResponse(ADMIN_HTML)

@router.get("/admin/api/stats24")
def admin_stats24(_: bool = Depends(require_admin_token)):
    # Часовой пояс панели (по умолчанию Europe/Moscow)
    try:
        tz = ZoneInfo(ADMIN_TZ)
    except Exception:
        tz = ZoneInfo("UTC")

    now_dt   = datetime.now(tz)
    start_dt = now_dt - timedelta(hours=24)

    rows_s = db_query("SELECT * FROM searches WHERE ts>=? ORDER BY ts ASC", (int(start_dt.timestamp()),))
    rows_c = db_query("SELECT * FROM clicks   WHERE ts>=? ORDER BY ts ASC", (int(start_dt.timestamp()),))

    searches_24h  = len(rows_s)
    unique_q_24h  = len({r["q"] for r in rows_s})
    avg_total_24h = (sum(r["total"] for r in rows_s)/searches_24h) if searches_24h else 0.0
    ctr_24h       = (len(rows_c)/searches_24h) if searches_24h else 0.0

    buckets_s = [0]*24
    buckets_c = [0]*24

    for r in rows_s:
        dt = datetime.fromtimestamp(r["ts"], tz)
        if dt < start_dt: continue
        idx = int((dt - start_dt).total_seconds() // 3600)
        if 0 <= idx < 24: buckets_s[idx] += 1

    for r in rows_c:
        dt = datetime.fromtimestamp(r["ts"], tz)
        if dt < start_dt: continue
        idx = int((dt - start_dt).total_seconds() // 3600)
        if 0 <= idx < 24: buckets_c[idx] += 1

    hours = [(start_dt + timedelta(hours=i)).strftime("%H:00") for i in range(24)]

    return {
        "searches_24h": searches_24h,
        "unique_q_24h": unique_q_24h,
        "avg_total_24h": avg_total_24h,
        "ctr_24h": ctr_24h,
        "series": {"hours": hours, "counts": buckets_s, "clicks": buckets_c},
    }

@router.get("/admin/api/last_searches")
def admin_last_searches(limit: int = 50, _: bool = Depends(require_admin_token)):
    limit = max(1, min(500, limit))
    rows = db_query("SELECT ts, ip, ua, host, region, q, page, per_page, total FROM searches ORDER BY id DESC LIMIT ?", (limit,))
    return [dict(r) for r in rows]

@router.get("/admin/api/last_clicks")
def admin_last_clicks(limit: int = 50, _: bool = Depends(require_admin_token)):
    limit = max(1, min(500, limit))
    rows = db_query("SELECT ts, ip, ua, host, region, q, item_name, item_url, price FROM clicks ORDER BY id DESC LIMIT ?", (limit,))
    return [dict(r) for r in rows]

# ─────────────────────────────────────────────────────────────
# public hook: смонтировать админку на приложение
# ─────────────────────────────────────────────────────────────
def mount_admin(app):
    app.include_router(router)