# ambient-monitor

把三個來源的**環境氣溫 (AT) / 大氣壓力 (AP) / 相對濕度 (AH)** 定時記錄下來，
畫成曲線放在一起比對：

| 來源 | 是什麼 | 取得方式 |
|---|---|---|
| **Trident BME280** | 烘豆控制器（[Trident](../skyproject/SkywalkerRoasterLab/Trident)，`bme280-sensor` 分支）上的實體感測器，**室內** | 純 HTTP `GET http://trident.local/api/ambient` |
| **Open-Meteo** | 所在地區的數值模型「現況」（15 分鐘更新一次，不是真實測站） | 優先向 [weather-proxy](../weather-proxy) 的 `/api/preview` 拿；proxy 沒開就讀它的 `weather_config.json` 經緯度直接打 Open-Meteo |
| **CWA 測站** | 中央氣象署自動 / 人工測站的**真實觀測值**（約 10 分鐘更新一次） | CWA 開放資料 API（要 API key，在網頁「設定」填入並選站） |

## 執行

需要 [uv](https://github.com/astral-sh/uv)；相依套件（aiohttp）寫在檔頭的 inline metadata，uv 會自動準備：

```bash
uv run C:\myproject\ambient-monitor\ambient_monitor.py
```

或直接雙擊 `run.bat`。然後瀏覽器開 **http://localhost:8766/**（綁 `0.0.0.0`，LAN 上的手機也能看）。

第一次執行會產生 `config.json`（可直接編輯）和 `ambient.db`（SQLite）。

## 網頁

- **三張曲線圖**（AT / AP / AH），每張三條線；滑鼠移過去顯示該時刻三源的值。
- **最新值 tile**：三個指標 × 三個來源，附「Δ 相對 Trident」的差值。
- **時間範圍**：1h / 6h / 24h / 3d / 7d / 30d（伺服器端依範圍分桶平均，點數 ≤ 900）。
- **氣壓換算到海平面**：勾了以後三源的氣壓都依各自海拔換算成海平面壓（國際標準大氣公式，用該源自己的氣溫）——因為 BME280 讀的是你所在樓層的絕對壓、Open-Meteo 是模型地表壓、測站是站壓，每差 8 m 約差 1 hPa，不換算不好比。各源海拔：
  - Trident：設定頁的「BME280 海拔」（留空 = 不換算 Trident）
  - Open-Meteo：API 回傳的模型海拔（可在設定頁覆寫）
  - CWA：所選測站的站高（選站時自動帶入）
- **資料表**：目前範圍的數值表（無障礙 / 對數字用）。
- **匯出 CSV**：目前範圍的原始（未分桶、未換算）資料。
- **設定**：採樣間隔（預設 60 s，最小 5 s，儲存後下一輪生效）、Trident 位址、海拔、CWA API key 與測站搜尋。

## CWA 測站設定

1. 到 <https://opendata.cwa.gov.tw/user/authkey> 免費註冊，取得「授權碼」。
2. 網頁「設定」→ 貼上 CWA API key → 在「CWA 測站」輸入站名 / 縣市 / 鄉鎮（例：`汐止`）→ 搜尋 → 選取 → 儲存設定。
   搜尋結果會顯示每站的種類（自動站 `O-A0001-001` / 人工站 `O-A0003-001`）、海拔、以及現在的讀數，方便挑離你最近、而且三個要素都有值的站（有些自動站沒有氣壓）。

API key 存在 `config.json`（已被 `.gitignore` 忽略），不會送回瀏覽器。

> 註：氣象署憑證鏈的中間憑證缺 Subject Key Identifier，Python 3.13+ 預設的
> `VERIFY_X509_STRICT` 會拒絕連線（`certificate verify failed: Missing Subject Key
> Identifier`）。程式對 CWA 連線只關掉 strict 旗標，CA / 主機名稱 / 有效期驗證照舊。

## 資料

`ambient.db` 一張寬表 `samples`：`ts`（unix 秒）+ 三源各 `_temp / _press / _hum`（原始站壓、未換算）+ `c_obstime`（CWA 觀測時間）。
任一來源抓不到就存 NULL，不影響其他來源。Open-Meteo / CWA 有 5 分鐘快取，採樣間隔調很短也不會打爆外部 API。

## API

| 方法 | 路徑 | 說明 |
|---|---|---|
| GET | `/api/latest` | 最新一列 + 各來源狀態 |
| GET | `/api/history?hours=24` | 分桶平均後的時序資料 |
| GET | `/api/status` | 各來源狀態、上次採樣時間、Open-Meteo 來源（proxy / 直連）與模型海拔 |
| GET / POST | `/api/config` | 讀 / 寫設定 |
| GET | `/api/cwa/stations?q=汐止[&key=…]` | 搜尋 CWA 測站 |
| GET | `/export.csv?hours=168` | CSV 匯出 |

## 檔案

| 檔案 | 版控 | 說明 |
|---|---|---|
| `ambient_monitor.py` | ✅ | 本體（單檔：採樣器 + API + 網頁） |
| `run.bat` | ✅ | 雙擊啟動 |
| `config.json` | ❌ | 執行期設定（含 CWA key） |
| `ambient.db` | ❌ | 量測資料 |
