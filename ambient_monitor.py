#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["aiohttp>=3.9"]
# ///
"""
ambient_monitor.py
------------------
環境（氣溫 AT / 大氣壓力 AP / 相對濕度 AH）三源比對記錄器 + 網頁曲線。

資料來源（每 poll_interval_s 秒各抓一次，寫進 SQLite 一列）：
  1. Trident    — 烘豆控制器上的 BME280，純 HTTP  GET <trident_url>/api/ambient
  2. Open-Meteo — 所在地區的數值模型「現況」。優先向 weather-proxy 的 /api/preview
                  拿（共用它的快取）；proxy 沒開就讀 weather_config.json 的經緯度
                  直接打 Open-Meteo。
  3. CWA        — 中央氣象署自動/人工測站的真實觀測值（需 CWA 開放資料 API key，
                  在網頁設定頁填入並選站）。

網頁 (http://localhost:8766/)：三張曲線圖（AT/AP/AH，每張三條線）、最新值與差值、
時間範圍切換、氣壓「換算到海平面」開關（依各源海拔）、設定頁、CSV 匯出。

執行（uv 會依檔頭的 inline metadata 自動準備 aiohttp）：
    uv run ambient_monitor.py
"""

import asyncio
import json
import pathlib
import sqlite3
import ssl
import time
import traceback
from urllib.parse import quote

from aiohttp import ClientSession, ClientTimeout, web

HERE = pathlib.Path(__file__).parent
CONFIG_PATH = HERE / "config.json"
DB_PATH = HERE / "ambient.db"

# ---- 預設設定（config.json 裡有的鍵會覆蓋）---------------------------------
DEFAULTS = {
    "host": "0.0.0.0",
    "port": 8766,
    "poll_interval_s": 60,            # 採樣間隔；網頁可改，下一輪生效
    "trident_url": "http://trident.local",
    "bme280_altitude_m": None,        # BME280 所在樓層海拔（m）；None = 不換算
    "proxy_url": "http://127.0.0.1:8765",
    "weather_config_path": str(HERE.parent / "weather-proxy" / "weather_config.json"),
    "openmeteo_elevation_m": None,    # 覆寫 Open-Meteo 的模型海拔；None = 用 API 回的
    "cwa_api_key": "",
    "cwa_station": None,              # {"id","name","county","town","altitude","dataset"}
}

# Open-Meteo / CWA 的資料本身 15 / 10 分鐘才變一次，短於這個間隔就回快取，
# 避免採樣間隔調很短時去打爆外部 API。
NET_CACHE_TTL = 300

CWA_BASE = "https://opendata.cwa.gov.tw/api/v1/rest/datastore"
CWA_DATASETS = {
    "O-A0001-001": "自動站",
    "O-A0003-001": "人工站",
}
CWA_MISSING = -99.0

# CWA 的憑證鏈中間憑證缺 Subject Key Identifier；Python 3.13+ 預設開
# VERIFY_X509_STRICT 會直接拒絕（curl / 瀏覽器都不會）。只關掉 strict 旗標，
# 其餘驗證（CA、主機名稱、有效期）照舊。
_CWA_SSL = ssl.create_default_context()
_CWA_SSL.verify_flags &= ~ssl.VERIFY_X509_STRICT


class NotConfigured(RuntimeError):
    """來源尚未設定（不是故障）——狀態列顯示為中性而非錯誤。"""


# ---- 設定讀寫 ----------------------------------------------------------------
def load_config():
    cfg = dict(DEFAULTS)
    if CONFIG_PATH.exists():
        try:
            cfg.update(json.loads(CONFIG_PATH.read_text(encoding="utf-8")))
        except Exception as e:
            print("讀取 config.json 失敗，改用預設值:", e)
    return cfg


def save_config(cfg):
    out = {k: cfg.get(k) for k in DEFAULTS}
    CONFIG_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")


# ---- SQLite（寬表：一列 = 一個採樣時刻的三個來源）----------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS samples (
  ts        INTEGER PRIMARY KEY,   -- unix seconds
  t_temp    REAL, t_press REAL, t_hum REAL,   -- Trident BME280
  o_temp    REAL, o_press REAL, o_hum REAL,   -- Open-Meteo
  c_temp    REAL, c_press REAL, c_hum REAL,   -- CWA 測站
  c_obstime TEXT                              -- CWA 觀測時間（ISO）
);
"""
COLS = ["t_temp", "t_press", "t_hum", "o_temp", "o_press", "o_hum",
        "c_temp", "c_press", "c_hum"]


def db_connect():
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(SCHEMA)
    return con


def db_insert(con, ts, row):
    con.execute(
        "INSERT OR REPLACE INTO samples (ts, t_temp,t_press,t_hum, o_temp,o_press,o_hum,"
        " c_temp,c_press,c_hum, c_obstime) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (ts, *[row.get(c) for c in COLS], row.get("c_obstime")),
    )
    con.commit()


def db_history(con, hours, max_points=900):
    """回傳 [since, now] 的資料，依時間分桶取平均，讓點數 <= max_points。"""
    now = int(time.time())
    since = now - int(hours * 3600)
    bucket = max(1, int(hours * 3600 // max_points))
    avg = ", ".join(f"AVG({c})" for c in COLS)
    cur = con.execute(
        f"SELECT (ts/?)*? AS b, {avg} FROM samples WHERE ts >= ? GROUP BY b ORDER BY b",
        (bucket, bucket, since),
    )
    return {"since": since, "now": now, "bucket_s": bucket,
            "cols": ["ts", *COLS],
            "rows": [list(r) for r in cur.fetchall()]}


def db_latest(con):
    cur = con.execute(
        "SELECT ts, t_temp,t_press,t_hum, o_temp,o_press,o_hum, c_temp,c_press,c_hum,"
        " c_obstime FROM samples ORDER BY ts DESC LIMIT 1")
    r = cur.fetchone()
    if not r:
        return None
    return dict(zip(["ts", *COLS, "c_obstime"], r))


def db_export_rows(con, hours):
    since = int(time.time()) - int(hours * 3600)
    return con.execute(
        "SELECT ts, t_temp,t_press,t_hum, o_temp,o_press,o_hum, c_temp,c_press,c_hum,"
        " c_obstime FROM samples WHERE ts >= ? ORDER BY ts", (since,)).fetchall()


# ---- 來源 1：Trident BME280 ----------------------------------------------------
async def fetch_trident(session, cfg):
    url = cfg["trident_url"].rstrip("/") + "/api/ambient"
    async with session.get(url, timeout=ClientTimeout(total=5)) as r:
        d = await r.json(content_type=None)
    if not d.get("valid"):
        raise RuntimeError("Trident 回 valid=false（BME280 尚無有效讀數）")
    return {"temp": d["temp"], "pressure": d["pressure"], "humidity": d["humidity"]}


# ---- 來源 2：Open-Meteo（proxy 優先，否則直連）----------------------------------
_om_cache = {"ts": 0.0, "data": None, "via": None, "elevation": None}


def read_weather_config(cfg):
    p = pathlib.Path(cfg["weather_config_path"])
    if not p.exists():
        raise RuntimeError(f"找不到 weather_config.json：{p}")
    return json.loads(p.read_text(encoding="utf-8"))


async def fetch_openmeteo(session, cfg):
    now = time.time()
    if _om_cache["data"] and now - _om_cache["ts"] < NET_CACHE_TTL:
        return _om_cache["data"]

    # (a) weather-proxy 有開 → 用它的快取值
    if cfg.get("proxy_url"):
        try:
            url = cfg["proxy_url"].rstrip("/") + "/api/preview"
            async with session.get(url, timeout=ClientTimeout(total=2)) as r:
                d = await r.json(content_type=None)
            if d.get("valid"):
                data = {"temp": d["temp"], "pressure": d["pressure"], "humidity": d["humidity"]}
                _om_cache.update(ts=now, data=data, via="proxy")
                if _om_cache["elevation"] is None:
                    try:
                        _om_cache["elevation"] = read_weather_config(cfg).get("elevation")
                    except Exception:
                        pass
                return data
        except Exception:
            pass  # proxy 沒開 → 直連

    # (b) 讀 proxy 的地區設定，直接打 Open-Meteo
    wc = read_weather_config(cfg)
    url = ("https://api.open-meteo.com/v1/forecast"
           f"?latitude={wc['latitude']}&longitude={wc['longitude']}"
           "&current=temperature_2m,relative_humidity_2m,surface_pressure")
    async with session.get(url, timeout=ClientTimeout(total=10)) as r:
        j = await r.json(content_type=None)
    cur = j["current"]
    data = {"temp": cur["temperature_2m"], "pressure": cur["surface_pressure"],
            "humidity": cur["relative_humidity_2m"]}
    _om_cache.update(ts=now, data=data, via="direct",
                     elevation=j.get("elevation", wc.get("elevation")))
    return data


# ---- 來源 3：CWA 測站 ------------------------------------------------------------
_cwa_cache = {"ts": 0.0, "station": None, "data": None}


def _cwa_num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if f <= CWA_MISSING else f


def _cwa_parse_station(st):
    geo = st.get("GeoInfo", {})
    we = st.get("WeatherElement", {})
    lat = lon = None
    for c in geo.get("Coordinates", []):
        if c.get("CoordinateName") == "WGS84":
            lat, lon = _cwa_num(c.get("StationLatitude")), _cwa_num(c.get("StationLongitude"))
    return {
        "id": st.get("StationId"),
        "name": st.get("StationName"),
        "county": geo.get("CountyName", ""),
        "town": geo.get("TownName", ""),
        "altitude": _cwa_num(geo.get("StationAltitude")),
        "lat": lat, "lon": lon,
        "obstime": st.get("ObsTime", {}).get("DateTime"),
        "temp": _cwa_num(we.get("AirTemperature")),
        "pressure": _cwa_num(we.get("AirPressure")),
        "humidity": _cwa_num(we.get("RelativeHumidity")),
    }


async def cwa_get(session, key, dataset, params=""):
    url = f"{CWA_BASE}/{dataset}?Authorization={quote(key)}{params}"
    async with session.get(url, timeout=ClientTimeout(total=15), ssl=_CWA_SSL) as r:
        if r.status == 401:
            raise RuntimeError("CWA API key 不正確（401）")
        j = await r.json(content_type=None)
    if j.get("success") not in (True, "true"):
        raise RuntimeError(f"CWA 回應失敗：{str(j)[:200]}")
    return j.get("records", {}).get("Station", [])


async def cwa_search_stations(session, key, q):
    q = (q or "").strip()
    out = []
    for ds, kind in CWA_DATASETS.items():
        for st in await cwa_get(session, key, ds):
            p = _cwa_parse_station(st)
            hay = f"{p['name']}{p['county']}{p['town']}{p['id']}"
            if not q or q in hay:
                p["dataset"] = ds
                p["kind"] = kind
                out.append(p)
    return out


async def fetch_cwa(session, cfg):
    st = cfg.get("cwa_station")
    key = cfg.get("cwa_api_key")
    if not st or not key:
        raise NotConfigured("未設定 CWA API key 或測站")
    now = time.time()
    if (_cwa_cache["data"] and _cwa_cache["station"] == st["id"]
            and now - _cwa_cache["ts"] < NET_CACHE_TTL):
        return _cwa_cache["data"]
    rows = await cwa_get(session, key, st["dataset"], f"&StationId={quote(st['id'])}")
    if not rows:
        raise RuntimeError(f"CWA 沒有回傳測站 {st['id']} 的資料")
    p = _cwa_parse_station(rows[0])
    data = {"temp": p["temp"], "pressure": p["pressure"], "humidity": p["humidity"],
            "obstime": p["obstime"]}
    _cwa_cache.update(ts=now, station=st["id"], data=data)
    return data


# ---- 採樣主迴圈 -----------------------------------------------------------------
async def poller(app):
    session = app["session"]
    con = app["db"]
    status = app["status"]
    while True:
        cfg = load_config()
        t0 = time.time()
        ts = int(t0)
        results = await asyncio.gather(
            fetch_trident(session, cfg),
            fetch_openmeteo(session, cfg),
            fetch_cwa(session, cfg),
            return_exceptions=True,
        )
        row = {}
        for name, prefix, res in zip(("trident", "openmeteo", "cwa"), ("t", "o", "c"), results):
            if isinstance(res, Exception):
                status[name] = {"ok": False, "skipped": isinstance(res, NotConfigured),
                                "error": str(res) or res.__class__.__name__, "ts": ts}
                continue
            status[name] = {"ok": True, "skipped": False, "error": None, "ts": ts}
            row[f"{prefix}_temp"] = res.get("temp")
            row[f"{prefix}_press"] = res.get("pressure")
            row[f"{prefix}_hum"] = res.get("humidity")
            if prefix == "c":
                row["c_obstime"] = res.get("obstime")
        if any(v is not None for k, v in row.items() if k != "c_obstime"):
            try:
                db_insert(con, ts, row)
            except Exception:
                traceback.print_exc()
        status["last_poll"] = ts
        status["openmeteo_via"] = _om_cache["via"]
        status["openmeteo_elevation"] = _om_cache["elevation"]
        interval = max(5, int(cfg.get("poll_interval_s") or 60))
        await asyncio.sleep(max(1.0, interval - (time.time() - t0)))


# ---- HTTP API -----------------------------------------------------------------
def _hours(request, default=24.0):
    try:
        return max(0.05, min(24 * 365, float(request.query.get("hours", default))))
    except ValueError:
        return default


async def api_history(request):
    return web.json_response(db_history(request.app["db"], _hours(request)))


async def api_latest(request):
    return web.json_response({"latest": db_latest(request.app["db"]),
                              "status": request.app["status"]})


async def api_status(request):
    return web.json_response(request.app["status"])


def _public_cfg(cfg):
    out = dict(cfg)
    out["cwa_api_key_set"] = bool(cfg.get("cwa_api_key"))
    out["cwa_api_key"] = ""  # 不把 key 送回瀏覽器
    return out


async def api_config_get(request):
    return web.json_response(_public_cfg(load_config()))


async def api_config_post(request):
    body = await request.json()
    cfg = load_config()
    try:
        if "poll_interval_s" in body:
            cfg["poll_interval_s"] = max(5, int(body["poll_interval_s"]))
        if "trident_url" in body and body["trident_url"]:
            cfg["trident_url"] = str(body["trident_url"]).strip()
        for k in ("bme280_altitude_m", "openmeteo_elevation_m"):
            if k in body:
                v = body[k]
                cfg[k] = None if v in (None, "") else float(v)
        if body.get("cwa_api_key"):            # 空字串 = 不變
            cfg["cwa_api_key"] = str(body["cwa_api_key"]).strip()
        if "cwa_station" in body:
            st = body["cwa_station"]
            cfg["cwa_station"] = None if not st else {
                "id": st["id"], "name": st.get("name", ""),
                "county": st.get("county", ""), "town": st.get("town", ""),
                "altitude": st.get("altitude"), "dataset": st.get("dataset", "O-A0001-001"),
            }
            _cwa_cache["data"] = None
    except (KeyError, TypeError, ValueError) as e:
        return web.json_response({"ok": False, "error": f"欄位不正確：{e}"}, status=400)
    save_config(cfg)
    return web.json_response({"ok": True, "config": _public_cfg(cfg)})


async def api_cwa_stations(request):
    cfg = load_config()
    key = request.query.get("key") or cfg.get("cwa_api_key")
    if not key:
        return web.json_response({"ok": False, "error": "請先填 CWA API key"}, status=400)
    try:
        stations = await cwa_search_stations(request.app["session"], key, request.query.get("q", ""))
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=502)
    return web.json_response({"ok": True, "stations": stations[:50], "total": len(stations)})


async def export_csv(request):
    rows = db_export_rows(request.app["db"], _hours(request, 24 * 7))
    lines = ["time,ts,trident_temp,trident_pressure,trident_humidity,"
             "openmeteo_temp,openmeteo_pressure,openmeteo_humidity,"
             "cwa_temp,cwa_pressure,cwa_humidity,cwa_obstime"]
    for r in rows:
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r[0]))
        lines.append(",".join([t] + ["" if v is None else str(v) for v in r]))
    return web.Response(text="\n".join(lines) + "\n", content_type="text/csv",
                        headers={"Content-Disposition": 'attachment; filename="ambient.csv"'})


async def index(request):
    return web.Response(text=INDEX_HTML, content_type="text/html")


# ---- 網頁 ---------------------------------------------------------------------
INDEX_HTML = r"""<!doctype html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>環境監測比對</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  :root {
    color-scheme: light;
    --page: #f9f9f7; --surface: #fcfcfb;
    --ink: #0b0b0b; --ink-2: #52514e; --muted: #898781;
    --grid: #e1e0d9; --axis: #c3c2b7; --border: rgba(11,11,11,.10);
    --s1: #2a78d6; --s2: #eb6834; --s3: #1baf7a;   /* Trident / Open-Meteo / CWA */
    --good: #006300; --bad: #d03b3b;
  }
  @media (prefers-color-scheme: dark) {
    :root:not([data-theme="light"]) {
      color-scheme: dark;
      --page: #0d0d0d; --surface: #1a1a19;
      --ink: #fff; --ink-2: #c3c2b7; --muted: #898781;
      --grid: #2c2c2a; --axis: #383835; --border: rgba(255,255,255,.10);
      --s1: #3987e5; --s2: #d95926; --s3: #199e70;
      --good: #0ca30c; --bad: #e66767;
    }
  }
  :root[data-theme="dark"] {
    color-scheme: dark;
    --page: #0d0d0d; --surface: #1a1a19;
    --ink: #fff; --ink-2: #c3c2b7; --muted: #898781;
    --grid: #2c2c2a; --axis: #383835; --border: rgba(255,255,255,.10);
    --s1: #3987e5; --s2: #d95926; --s3: #199e70;
    --good: #0ca30c; --bad: #e66767;
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--page); color: var(--ink);
         font: 14px/1.5 system-ui, -apple-system, "Segoe UI", "Microsoft JhengHei", sans-serif; }
  .wrap { max-width: 1100px; margin: 0 auto; padding: 16px; }
  header { display: flex; flex-wrap: wrap; align-items: baseline; gap: 8px 16px; margin-bottom: 12px; }
  h1 { font-size: 18px; margin: 0; }
  .status { color: var(--ink-2); font-size: 13px; display: flex; flex-wrap: wrap; gap: 6px 14px; }
  .status .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%;
                 background: var(--muted); margin-right: 5px; vertical-align: 1px; }
  .status .ok .dot { background: var(--good); }
  .status .err .dot { background: var(--bad); }
  .spacer { flex: 1; }
  .toolbar { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin: 8px 0 16px; }
  .seg { display: inline-flex; border: 1px solid var(--border); border-radius: 8px; overflow: hidden; }
  .seg button { border: 0; background: transparent; color: var(--ink-2); padding: 6px 12px;
                cursor: pointer; font: inherit; }
  .seg button[aria-pressed="true"] { background: var(--surface); color: var(--ink); font-weight: 600;
                                     box-shadow: inset 0 0 0 1px var(--border); }
  button.plain, a.plain { border: 1px solid var(--border); background: var(--surface); color: var(--ink);
                          border-radius: 8px; padding: 6px 12px; cursor: pointer; font: inherit;
                          text-decoration: none; }
  label.chk { display: inline-flex; align-items: center; gap: 6px; color: var(--ink-2); cursor: pointer; }
  .tiles { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-bottom: 16px; }
  .tile { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 12px 14px; }
  .tile h3 { margin: 0 0 8px; font-size: 13px; color: var(--ink-2); font-weight: 600; }
  .tile table { width: 100%; border-collapse: collapse; }
  .tile td { padding: 3px 0; vertical-align: baseline; }
  .tile td.key { width: 14px; }
  .key i { display: inline-block; width: 14px; height: 0; border-top: 2px solid var(--c); vertical-align: middle; }
  .tile td.src { color: var(--ink-2); font-size: 12px; padding-left: 6px; }
  .tile td.val { text-align: right; font-size: 18px; font-weight: 600; }
  .tile td.unit { color: var(--muted); font-size: 12px; padding-left: 4px; width: 32px; }
  .tile td.delta { text-align: right; color: var(--ink-2); font-size: 12px; width: 62px;
                   font-variant-numeric: tabular-nums; }
  .card { background: var(--surface); border: 1px solid var(--border); border-radius: 12px;
          padding: 12px 14px 8px; margin-bottom: 16px; }
  .card h2 { font-size: 14px; margin: 0 0 6px; font-weight: 600; }
  .card h2 small { color: var(--muted); font-weight: 400; margin-left: 6px; }
  .chart { position: relative; height: 240px; }
  .loading .chart { opacity: .5; transition: opacity .2s; }
  details.settings summary { cursor: pointer; color: var(--ink-2); }
  .form { display: grid; grid-template-columns: max-content 1fr; gap: 10px 12px; align-items: center;
          margin-top: 12px; }
  .form input[type=text], .form input[type=number], .form input[type=password] {
    width: 100%; max-width: 420px; padding: 6px 8px; font: inherit; border: 1px solid var(--border);
    border-radius: 6px; background: var(--page); color: var(--ink); }
  .form .hint { grid-column: 2; color: var(--muted); font-size: 12px; margin-top: -6px; }
  .form .row { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
  ul.stations { list-style: none; margin: 6px 0 0; padding: 0; max-height: 220px; overflow: auto;
                border: 1px solid var(--border); border-radius: 8px; }
  ul.stations li { display: flex; justify-content: space-between; align-items: center; gap: 8px;
                   padding: 6px 10px; border-bottom: 1px solid var(--border); }
  ul.stations li:last-child { border-bottom: 0; }
  ul.stations .meta { color: var(--muted); font-size: 12px; }
  .msg { color: var(--ink-2); font-size: 13px; min-height: 1.2em; }
  .msg.bad { color: var(--bad); }
  table.data { width: 100%; border-collapse: collapse; font-size: 12px; font-variant-numeric: tabular-nums; }
  table.data th, table.data td { padding: 4px 6px; border-bottom: 1px solid var(--grid); text-align: right; white-space: nowrap; }
  table.data th:first-child, table.data td:first-child { text-align: left; }
  table.data th { color: var(--ink-2); font-weight: 600; position: sticky; top: 0; background: var(--surface); }
  .scroll { max-height: 360px; overflow: auto; }
  @media (max-width: 760px) { .tiles { grid-template-columns: 1fr; } .chart { height: 200px; }
                              .form { grid-template-columns: 1fr; } .form .hint { grid-column: 1; } }
</style>
</head>
<body>
<div class="wrap" id="app">
  <header>
    <h1>環境監測比對 — 室內 BME280 vs 地區天氣 vs 氣象署測站</h1>
    <span class="spacer"></span>
    <div class="status" id="status"></div>
  </header>

  <div class="toolbar">
    <div class="seg" id="ranges" role="group" aria-label="時間範圍"></div>
    <label class="chk"><input type="checkbox" id="msl"> 氣壓換算到海平面</label>
    <span class="spacer"></span>
    <button class="plain" id="tableBtn" aria-pressed="false">資料表</button>
    <a class="plain" id="csv" href="/export.csv?hours=168">匯出 CSV</a>
  </div>

  <div class="tiles" id="tiles"></div>

  <div class="card"><h2>氣溫 AT <small>°C</small></h2><div class="chart"><canvas id="c_temp"></canvas></div></div>
  <div class="card"><h2 id="pressTitle">氣壓 AP <small>hPa</small></h2><div class="chart"><canvas id="c_press"></canvas></div></div>
  <div class="card"><h2>相對濕度 AH <small>%</small></h2><div class="chart"><canvas id="c_hum"></canvas></div></div>

  <div class="card" id="tableCard" hidden>
    <h2>資料表 <small>目前範圍，依分桶平均</small></h2>
    <div class="scroll"><table class="data" id="dataTable"></table></div>
  </div>

  <div class="card">
    <details class="settings" id="settings">
      <summary>設定</summary>
      <div class="form">
        <label for="interval">採樣間隔（秒）</label>
        <input type="number" id="interval" min="5" step="1">
        <label for="trident">Trident 位址</label>
        <input type="text" id="trident" placeholder="http://trident.local">
        <label for="bmeAlt">BME280 海拔（m）</label>
        <input type="number" id="bmeAlt" step="0.1" placeholder="例：樓層地板海拔，如 30">
        <div class="hint">室內感測器所在樓層的海拔；用於「氣壓換算到海平面」。留空 = 不換算 Trident。</div>
        <label for="omAlt">Open-Meteo 海拔（m）</label>
        <input type="number" id="omAlt" step="0.1" placeholder="留空 = 用 API 回的模型海拔">
        <div class="hint" id="omAltHint"></div>
        <label for="cwaKey">CWA API key</label>
        <input type="password" id="cwaKey" placeholder="留空 = 不變" autocomplete="off">
        <div class="hint">到 <a href="https://opendata.cwa.gov.tw/user/authkey" target="_blank" rel="noopener">opendata.cwa.gov.tw</a> 免費註冊後取得「授權碼」。</div>
        <label for="stq">CWA 測站</label>
        <div>
          <div class="row">
            <input type="text" id="stq" placeholder="搜尋站名 / 縣市 / 鄉鎮，例：汐止 或 新北市" style="max-width:300px">
            <button class="plain" id="stSearch">搜尋</button>
            <span id="stCurrent" class="msg"></span>
          </div>
          <ul class="stations" id="stList" hidden></ul>
        </div>
        <span></span>
        <div class="row"><button class="plain" id="save">儲存設定</button><span class="msg" id="saveMsg"></span></div>
      </div>
    </details>
  </div>
</div>

<script>
const $ = s => document.querySelector(s);
const SRC = [
  { key: 't', name: 'Trident BME280', css: '--s1' },
  { key: 'o', name: 'Open-Meteo',     css: '--s2' },
  { key: 'c', name: 'CWA 測站',       css: '--s3' },
];
const METRICS = [
  { key: 'temp',  title: '氣溫 AT',   unit: '°C',  canvas: 'c_temp',  dec: 1 },
  { key: 'press', title: '氣壓 AP',   unit: 'hPa', canvas: 'c_press', dec: 1 },
  { key: 'hum',   title: '相對濕度 AH', unit: '%',   canvas: 'c_hum',   dec: 0 },
];
const RANGES = [['1h',1],['6h',6],['24h',24],['3d',72],['7d',168],['30d',720]];

let hours = +(localStorage.getItem('am.hours') || 24);
let msl = localStorage.getItem('am.msl') === '1';
let showTable = false;
let cfg = {}, status = {}, hist = null;
const charts = {};

const cssVar = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const fmtTime = (ts, long) => {
  const d = new Date(ts * 1000), p = n => String(n).padStart(2, '0');
  const hm = `${p(d.getHours())}:${p(d.getMinutes())}`;
  return long ? `${d.getMonth()+1}/${d.getDate()} ${hm}` : hm;
};
const fmtNum = (v, dec) => (v == null || isNaN(v)) ? '—' : v.toFixed(dec);

// 站壓 → 海平面壓（國際標準大氣公式；h = 海拔 m，T = 該處氣溫 °C）
function toMSL(p, h, t) {
  if (p == null || h == null) return p;
  const T = (t == null ? 15 : t) + 273.15;
  return p / Math.pow(1 - 0.0065 * h / (T + 0.0065 * h), 5.257);
}
function altitudeOf(src) {
  if (src === 't') return cfg.bme280_altitude_m;
  if (src === 'o') return cfg.openmeteo_elevation_m ?? status.openmeteo_elevation ?? null;
  if (src === 'c') return cfg.cwa_station?.altitude ?? null;
  return null;
}
// 取一列裡某源某指標的值（氣壓依開關換算）
function valueOf(row, src, metric) {
  const v = row[`${src}_${metric}`];
  if (metric === 'press' && msl) return toMSL(v, altitudeOf(src), row[`${src}_temp`]);
  return v;
}

// ---- 圖表 -------------------------------------------------------------
function chartColors() {
  return { ink: cssVar('--ink'), ink2: cssVar('--ink-2'), muted: cssVar('--muted'),
           grid: cssVar('--grid'), axis: cssVar('--axis'), surface: cssVar('--surface') };
}
function makeChart(m) {
  const c = chartColors();
  const ctx = document.getElementById(m.canvas);
  return new Chart(ctx, {
    type: 'line',
    data: { datasets: SRC.map(s => ({
      label: s.name, data: [], parsing: false, borderColor: cssVar(s.css), backgroundColor: cssVar(s.css),
      borderWidth: 2, pointRadius: 0, pointHoverRadius: 4, pointHitRadius: 12, tension: 0, spanGaps: false,
    })) },
    options: {
      responsive: true, maintainAspectRatio: false, animation: false, normalized: true,
      interaction: { mode: 'index', intersect: false, axis: 'x' },
      plugins: {
        legend: { position: 'top', align: 'end', labels: { usePointStyle: true, pointStyle: 'line',
                  color: c.ink2, boxWidth: 18, font: { size: 12 } } },
        tooltip: { backgroundColor: c.surface, titleColor: c.ink2, bodyColor: c.ink, borderColor: c.grid,
                   borderWidth: 1, usePointStyle: true, boxPadding: 4, padding: 8,
                   callbacks: {
                     title: items => items.length ? fmtTime(items[0].parsed.x / 1000, true) : '',
                     label: it => ` ${fmtNum(it.parsed.y, m.dec)} ${m.unit}  ${it.dataset.label}`,
                     labelPointStyle: () => ({ pointStyle: 'line', rotation: 0 }),
                   } },
      },
      scales: {
        x: { type: 'linear', grid: { color: c.grid, drawTicks: false }, border: { color: c.axis },
             ticks: { color: c.muted, maxTicksLimit: 8, maxRotation: 0, font: { size: 11 },
                      callback: v => fmtTime(v / 1000, hours > 24) } },
        y: { grid: { color: c.grid, drawTicks: false }, border: { display: false },
             ticks: { color: c.muted, font: { size: 11 }, callback: v => v.toFixed(m.dec) } },
      },
    },
  });
}
function restyleCharts() {
  const c = chartColors();
  for (const m of METRICS) {
    const ch = charts[m.key];
    ch.data.datasets.forEach((d, i) => { d.borderColor = d.backgroundColor = cssVar(SRC[i].css); });
    ch.options.plugins.legend.labels.color = c.ink2;
    Object.assign(ch.options.plugins.tooltip, { backgroundColor: c.surface, titleColor: c.ink2, bodyColor: c.ink, borderColor: c.grid });
    ch.options.scales.x.grid.color = c.grid; ch.options.scales.x.border.color = c.axis; ch.options.scales.x.ticks.color = c.muted;
    ch.options.scales.y.grid.color = c.grid; ch.options.scales.y.ticks.color = c.muted;
    ch.update('none');
  }
}
function rowsFromHist() {
  if (!hist) return [];
  return hist.rows.map(r => Object.fromEntries(hist.cols.map((c, i) => [c, r[i]])));
}
function renderCharts() {
  const rows = rowsFromHist();
  for (const m of METRICS) {
    const ch = charts[m.key];
    ch.data.datasets.forEach((d, i) => {
      d.data = rows.map(r => ({ x: r.ts * 1000, y: valueOf(r, SRC[i].key, m.key) ?? null }));
    });
    ch.options.scales.x.min = hist.since * 1000;
    ch.options.scales.x.max = hist.now * 1000;
    ch.options.scales.x.ticks.callback = v => fmtTime(v / 1000, hours > 24);
    ch.update('none');
  }
  $('#pressTitle').innerHTML = msl ? '氣壓 AP <small>hPa，已換算海平面</small>' : '氣壓 AP <small>hPa，站壓原值</small>';
  if (showTable) renderTable(rows);
}

// ---- 最新值 tile --------------------------------------------------------
function renderTiles(latest) {
  const root = $('#tiles'); root.innerHTML = '';
  for (const m of METRICS) {
    const tile = document.createElement('div'); tile.className = 'tile';
    const h = document.createElement('h3'); h.textContent = `${m.title}（${m.unit}）`; tile.appendChild(h);
    const tb = document.createElement('table');
    const base = latest ? valueOf(latest, 't', m.key) : null;
    for (const s of SRC) {
      const v = latest ? valueOf(latest, s.key, m.key) : null;
      const tr = document.createElement('tr');
      const k = document.createElement('td'); k.className = 'key'; k.innerHTML = '<i></i>'; k.firstChild.style.setProperty('--c', `var(${s.css})`);
      const n = document.createElement('td'); n.className = 'src'; n.textContent = s.name;
      const val = document.createElement('td'); val.className = 'val'; val.textContent = fmtNum(v, m.dec);
      const u = document.createElement('td'); u.className = 'unit'; u.textContent = m.unit;
      const d = document.createElement('td'); d.className = 'delta';
      if (s.key !== 't' && v != null && base != null) {
        const diff = v - base; d.textContent = `Δ ${diff >= 0 ? '+' : ''}${diff.toFixed(m.dec)}`;
        d.title = '相對 Trident BME280 的差值';
      }
      tr.append(k, n, val, u, d); tb.appendChild(tr);
    }
    tile.appendChild(tb); root.appendChild(tile);
  }
}

// ---- 狀態列 -------------------------------------------------------------
function renderStatus() {
  const root = $('#status'); root.innerHTML = '';
  const add = (cls, text, title) => { const s = document.createElement('span'); s.className = cls;
    s.innerHTML = '<span class="dot"></span>'; s.appendChild(document.createTextNode(text)); if (title) s.title = title; root.appendChild(s); };
  for (const s of SRC) {
    const key = { t: 'trident', o: 'openmeteo', c: 'cwa' }[s.key];
    const st = status[key];
    let extra = '';
    if (key === 'openmeteo' && st?.ok) extra = status.openmeteo_via === 'proxy' ? '（經 proxy）' : '（直連）';
    if (key === 'cwa' && cfg.cwa_station) extra = `（${cfg.cwa_station.name}）`;
    add(!st || st.skipped ? '' : (st.ok ? 'ok' : 'err'), s.name + extra, st?.error || '');
  }
  if (status.last_poll) add('', `上次採樣 ${fmtTime(status.last_poll, true)} · 每 ${cfg.poll_interval_s}s`);
}

// ---- 資料表 -------------------------------------------------------------
function renderTable(rows) {
  const t = $('#dataTable'); t.innerHTML = '';
  const thead = t.createTHead().insertRow();
  ['時間', ...METRICS.flatMap(m => SRC.map(s => `${m.title.split(' ')[1]} ${s.name}`))]
    .forEach(h => { const th = document.createElement('th'); th.textContent = h; thead.appendChild(th); });
  const tb = t.createTBody();
  for (const r of rows.slice().reverse()) {
    const tr = tb.insertRow();
    tr.insertCell().textContent = fmtTime(r.ts, true);
    for (const m of METRICS) for (const s of SRC) tr.insertCell().textContent = fmtNum(valueOf(r, s.key, m.key), m.dec);
  }
}

// ---- 載入 ---------------------------------------------------------------
async function loadHistory() {
  document.getElementById('app').classList.add('loading');
  try {
    hist = await (await fetch(`/api/history?hours=${hours}`)).json();
    renderCharts();
  } finally { document.getElementById('app').classList.remove('loading'); }
}
async function loadLatest() {
  const j = await (await fetch('/api/latest')).json();
  status = j.status || {};
  renderTiles(j.latest); renderStatus();
  $('#omAltHint').textContent = status.openmeteo_elevation != null
    ? `目前 API 回的模型海拔：${status.openmeteo_elevation} m` : '';
}
async function loadConfig() {
  cfg = await (await fetch('/api/config')).json();
  $('#interval').value = cfg.poll_interval_s;
  $('#trident').value = cfg.trident_url;
  $('#bmeAlt').value = cfg.bme280_altitude_m ?? '';
  $('#omAlt').value = cfg.openmeteo_elevation_m ?? '';
  $('#cwaKey').placeholder = cfg.cwa_api_key_set ? '已設定（留空 = 不變）' : '尚未設定';
  renderStationCurrent();
}
function renderStationCurrent() {
  const s = cfg.cwa_station;
  $('#stCurrent').textContent = s ? `目前：${s.county}${s.town} ${s.name}（${s.id}，海拔 ${s.altitude ?? '?'} m）` : '尚未選站';
}
async function refresh() {
  await Promise.all([loadLatest(), loadHistory()]);
}

// ---- 設定頁互動 -----------------------------------------------------------
let pendingStation;
async function searchStations() {
  const list = $('#stList'); list.hidden = false; list.innerHTML = '<li>搜尋中…</li>';
  const key = $('#cwaKey').value.trim();
  const url = `/api/cwa/stations?q=${encodeURIComponent($('#stq').value.trim())}${key ? '&key=' + encodeURIComponent(key) : ''}`;
  let j;
  try { j = await (await fetch(url)).json(); } catch (e) { j = { ok: false, error: String(e) }; }
  list.innerHTML = '';
  if (!j.ok) { const li = document.createElement('li'); li.textContent = '失敗：' + j.error; list.appendChild(li); return; }
  if (!j.stations.length) { const li = document.createElement('li'); li.textContent = '找不到測站'; list.appendChild(li); return; }
  for (const s of j.stations) {
    const li = document.createElement('li');
    const span = document.createElement('span');
    span.textContent = `${s.county}${s.town} ${s.name}`;
    const meta = document.createElement('div'); meta.className = 'meta';
    meta.textContent = `${s.id} · ${s.kind} · 海拔 ${s.altitude ?? '?'} m · 現在 ${fmtNum(s.temp,1)}°C / ${fmtNum(s.pressure,1)} hPa / ${fmtNum(s.humidity,0)}%`;
    span.appendChild(meta);
    const b = document.createElement('button'); b.className = 'plain'; b.textContent = '選取';
    b.onclick = () => { pendingStation = s; cfg.cwa_station = s; renderStationCurrent(); list.hidden = true; };
    li.append(span, b); list.appendChild(li);
  }
  if (j.total > j.stations.length) { const li = document.createElement('li'); li.className = 'meta';
    li.textContent = `只顯示前 ${j.stations.length} / ${j.total} 站，請縮小搜尋`; list.appendChild(li); }
}
async function saveSettings() {
  const body = {
    poll_interval_s: +$('#interval').value,
    trident_url: $('#trident').value.trim(),
    bme280_altitude_m: $('#bmeAlt').value,
    openmeteo_elevation_m: $('#omAlt').value,
    cwa_api_key: $('#cwaKey').value.trim(),
  };
  if (pendingStation) body.cwa_station = pendingStation;
  const msg = $('#saveMsg'); msg.className = 'msg'; msg.textContent = '儲存中…';
  const j = await (await fetch('/api/config', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })).json();
  if (j.ok) { msg.textContent = '已儲存，下一輪採樣生效'; $('#cwaKey').value = ''; pendingStation = null; await loadConfig(); await loadLatest(); renderCharts(); }
  else { msg.className = 'msg bad'; msg.textContent = j.error || '儲存失敗'; }
}

// ---- 初始化 -------------------------------------------------------------
function initRanges() {
  const root = $('#ranges');
  for (const [label, h] of RANGES) {
    const b = document.createElement('button'); b.textContent = label; b.setAttribute('aria-pressed', h === hours);
    b.onclick = () => { hours = h; localStorage.setItem('am.hours', h);
      root.querySelectorAll('button').forEach(x => x.setAttribute('aria-pressed', x === b));
      $('#csv').href = `/export.csv?hours=${h}`; loadHistory(); };
    root.appendChild(b);
  }
  $('#csv').href = `/export.csv?hours=${hours}`;
}
for (const m of METRICS) charts[m.key] = makeChart(m);
initRanges();
$('#msl').checked = msl;
$('#msl').onchange = e => { msl = e.target.checked; localStorage.setItem('am.msl', msl ? '1' : '0'); renderCharts(); loadLatest(); };
$('#tableBtn').onclick = () => { showTable = !showTable; $('#tableBtn').setAttribute('aria-pressed', showTable);
  $('#tableCard').hidden = !showTable; if (showTable) renderTable(rowsFromHist()); };
$('#stSearch').onclick = searchStations;
$('#stq').addEventListener('keydown', e => { if (e.key === 'Enter') { e.preventDefault(); searchStations(); } });
$('#save').onclick = saveSettings;
matchMedia('(prefers-color-scheme: dark)').addEventListener('change', restyleCharts);

(async () => { await loadConfig(); await loadLatest(); await loadHistory(); })();
setInterval(refresh, 60000);
</script>
</body>
</html>
"""


# ---- 啟動 ---------------------------------------------------------------------
async def on_startup(app):
    app["session"] = ClientSession()
    app["db"] = db_connect()
    app["status"] = {"trident": None, "openmeteo": None, "cwa": None, "last_poll": None,
                     "openmeteo_via": None, "openmeteo_elevation": None}
    app["poller"] = asyncio.create_task(poller(app))


async def on_cleanup(app):
    app["poller"].cancel()
    try:
        await app["poller"]
    except asyncio.CancelledError:
        pass
    await app["session"].close()
    app["db"].close()


def main():
    cfg = load_config()
    if not CONFIG_PATH.exists():
        save_config(cfg)  # 產生一份可編輯的 config.json
    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    app.add_routes([
        web.get("/", index),
        web.get("/api/history", api_history),
        web.get("/api/latest", api_latest),
        web.get("/api/status", api_status),
        web.get("/api/config", api_config_get),
        web.post("/api/config", api_config_post),
        web.get("/api/cwa/stations", api_cwa_stations),
        web.get("/export.csv", export_csv),
    ])
    print(f"環境監測比對:  http://localhost:{cfg['port']}/   （資料庫 {DB_PATH.name}，設定 {CONFIG_PATH.name}）",
          flush=True)
    print(f"採樣間隔 {cfg['poll_interval_s']}s · Trident {cfg['trident_url']} · "
          f"CWA {'已設定' if cfg.get('cwa_api_key') and cfg.get('cwa_station') else '未設定（到網頁「設定」填 key 並選站）'}",
          flush=True)
    web.run_app(app, host=cfg["host"], port=int(cfg["port"]), print=None)


if __name__ == "__main__":
    main()
