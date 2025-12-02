#!/usr/bin/env python3
"""
tln_adaptive_bot.py - Render-ready resilient adaptive bot + status endpoints

Variant 1 (patched):
 - fetch full history from Dexscreener at startup (rebuild price_history.csv
   when full history is available)
 - cache / backoff to reduce API calls for Render Free
 - optional export_state_to_remote() using boto3/S3 if AWS env vars provided
 - keep watchdog + flask endpoints + export/debug endpoints
 - safe for paper-trading only
"""

import requests, time, csv, json, os, math, threading, traceback
from datetime import datetime, timezone
from flask import Flask, jsonify, send_file, Response

# -----------------------------
# CONFIG (editează pentru cloud)
# -----------------------------
BOT_VERSION = "1.2-render-variant1-20251121"
TOKEN_CONTRACT = "0xAa90a8CDAB8B8E902293a2817d1d286f66cBcec5"
DEXSCREENER_TOKEN_API = f"https://api.dexscreener.com/latest/dex/tokens/{TOKEN_CONTRACT}"
DEXSCREENER_CHART_API = f"https://api.dexscreener.com/latest/dex/token/{TOKEN_CONTRACT}/chart"  # fallback (some installations)

# Moralis OHLCV history (TLNGOLD/USDT)
MORALIS_API_KEY = os.environ.get("MORALIS_API_KEY", "").strip()
MORALIS_BASE_URL = "https://deep-index.moralis.io/api/v2.2"

# Pair TLNGOLD/USDT Pancake v2 (BNB Smart Chain)
TLN_USDT_PAIR = "0x1e7ea2ec47199191dc3b642bb6c24ed03cc1a641"

# Timeframe pentru istoricul OHLCV (configurat pentru 1h, dar îl poți override din env)
MORALIS_TIMEFRAME = os.environ.get("MORALIS_TIMEFRAME", "1h")

# Data de început (Moralis va returna efectiv doar de când există pair-ul)
MORALIS_FROM_DATE = os.environ.get("MORALIS_FROM_DATE", "2024-01-01T00:00:00Z")

# Heuristici pentru a decide dacă price_history.csv e "incomplet"
HISTORY_MIN_POINTS = int(os.environ.get("HISTORY_MIN_POINTS", "100"))
HISTORY_MAX_AGE_HOURS = int(os.environ.get("HISTORY_MAX_AGE_HOURS", "6"))

# recommended cloud run params
POLL_INTERVAL = 60              # seconds between checks
ROLLING_WINDOW = 20
K_STD = 1.5
POSITION_SIZE_USDT = 20.0
SLIPPAGE_PCT = 0.003
FEE_PCT = 0.0025
COOLDOWN_AFTER_TRADE = 300
MAX_DAILY_SPEND_USDT = 200.0

# Files
STATE_FILE = "state.json"
PRICE_FILE = "price_history.csv"
TRADES_FILE = "trades.csv"
LOG_FILE = "bot.log"

# Flask / Render
TEST_MODE = False
FLASK_ENABLED = True
FLASK_PORT = int(os.environ.get("PORT", 5000))

# resilience
RETRY_BACKOFF_BASE = 5    # seconds initial backoff after an uncaught exception
MAX_BACKOFF = 300         # cap backoff to 5 minutes

# API caching / rate-limit friendly
CACHE_TTL_SEC = 20        # if price unchanged, keep cached value for this time
_last_price_cache = {"price": None, "ts": 0}

# remote export / persistence
REMOTE_EXPORT_INTERVAL_SEC = int(os.environ.get("REMOTE_EXPORT_INTERVAL_SEC", 3600))  # hourly default
_last_remote_export = {"ts": 0, "status": None}

# optional S3 config via env (if you want long-term storage)
AWS_S3_BUCKET = os.environ.get("AWS_S3_BUCKET")
AWS_S3_PREFIX = os.environ.get("AWS_S3_PREFIX", "tln_bot_exports")
# NOTE: boto3 is optional; we try to import and if missing, export_state_to_remote is a no-op
try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
    _HAS_BOTO3 = True
except Exception:
    _HAS_BOTO3 = False

# globals for endpoints
LAST_PRICE = None
LAST_PRICE_TS = None
LAST_LOOP_TS = None
PRICE_SAMPLES_COUNT = 0

_lock = threading.Lock()

# -----------------------------
# Utilities: logging & csv
# -----------------------------
def send_telegram_message(text: str):
    """Trimite un mesaj Telegram folosind tokenul și chat_id-ul din variabilele de mediu."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")

    if not token or not chat_id:
        log("Telegram not configured; skipping message.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }

    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            log(f"Telegram error: {r.text}")
        else:
            log("Telegram message sent.")
    except Exception as e:
        log(f"Telegram exception: {e}")

def now_iso():
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

def log(msg):
    ts = now_iso()
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

def ensure_csv_headers():
    # trades
    if not os.path.exists(TRADES_FILE):
        with open(TRADES_FILE, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["timestamp","action","price","amount_tln","usdt_value","bot_version"])
    # price_history.csv NU îl mai creăm aici;
    # el va fi creat/reconstruit de logica Moralis (OHLCV) sau de backtest,
    # ca să nu stricăm formatul său.

def append_price(ts, price):
    try:
        with open(PRICE_FILE, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([ts, f"{price:.8f}"])
    except Exception as e:
        log(f"append_price error: {e}")

def append_trade(ts, action, price, amount_tln, usdt_value):
    try:
        with open(TRADES_FILE, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([ts, action, f"{price:.8f}", f"{amount_tln:.8f}", f"{usdt_value:.8f}", BOT_VERSION])
    except Exception as e:
        log(f"append_trade error: {e}")

# -----------------------------
# State management
# -----------------------------
def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            log("load_state: failed to parse state.json, initializing new.")
    init = {
        "usdt": 219.0,
        "tln": 0.0,
        "last_trade_ts": None,
        "daily_spent": 0.0,
        "last_day": None
    }
    save_state(init)
    return init

def save_state(state):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        log(f"save_state error: {e}")

# -----------------------------
# Moralis OHLCV history (TLNGOLD/USDT)
# -----------------------------
def _moralis_get(url, params):
    if not MORALIS_API_KEY:
        raise RuntimeError("MORALIS_API_KEY is not set; cannot fetch history from Moralis.")
    headers = {
        "X-API-Key": MORALIS_API_KEY,
        "accept": "application/json",
    }
    delay = 1.0
    max_retries = 5
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=20)
            if r.status_code == 200:
                return r.json()
            else:
                log(f"Moralis GET {url} failed ({r.status_code}): {r.text[:200]}")
        except Exception as e:
            log(f"Moralis GET exception: {e}")
        if attempt < max_retries - 1:
            log(f"Moralis: retrying in {delay:.1f}s...")
            time.sleep(delay)
            delay *= 2
    raise RuntimeError(f"Moralis GET {url} failed after {max_retries} attempts.")


def is_price_history_incomplete(path=PRICE_FILE):
    """
    Heuristică simplă:
    - dacă fișierul nu există -> incomplet
    - dacă are prea puține puncte -> incomplet
    - dacă ultimul punct este prea vechi -> incomplet
    """
    if not os.path.exists(path):
        log("price_history.csv does not exist -> considered incomplete.")
        return True

    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception as e:
        log(f"Failed to read {path}: {e} -> considered incomplete.")
        return True

    if len(rows) < HISTORY_MIN_POINTS:
        log(f"price_history.csv has only {len(rows)} rows (< {HISTORY_MIN_POINTS}) -> incomplete.")
        return True

    last = rows[-1]
    ts_str = last.get("timestamp_iso") or last.get("timestamp")
    if not ts_str:
        log("Last row in price_history.csv has no timestamp -> incomplete.")
        return True

    try:
        # suportă formate ISO cu 'Z'
        ts_str2 = ts_str.replace("Z", "+00:00") if "Z" in ts_str and "+" not in ts_str else ts_str
        last_dt = datetime.fromisoformat(ts_str2)
    except Exception:
        try:
            # fallback: timestamp Unix
            last_dt = datetime.fromtimestamp(int(last.get("timestamp_unix", 0)), tz=timezone.utc)
        except Exception as e:
            log(f"Could not parse last timestamp in price_history.csv: {e} -> incomplete.")
            return True

    if last_dt.tzinfo is None:
        last_dt = last_dt.replace(tzinfo=timezone.utc)

    age_hours = (datetime.now(timezone.utc) - last_dt).total_seconds() / 3600.0
    if age_hours > HISTORY_MAX_AGE_HOURS:
        log(f"Last price point is {age_hours:.1f}h old (> {HISTORY_MAX_AGE_HOURS}h) -> incomplete.")
        return True

    log("price_history.csv looks recent and complete enough.")
    return False


def fetch_ohlcv_from_moralis():
    """
    Descarcă tot istoricul OHLCV (timeframe 1h) pentru TLNGOLD/USDT de la Moralis.
    Returnează o listă de dict-uri cu:
      - timestamp_unix
      - timestamp_iso
      - open, high, low, close, volume
    """
    url = f"{MORALIS_BASE_URL}/pairs/{TLN_USDT_PAIR}/ohlcv"
    params = {
        "chain": "bsc",
        "timeframe": MORALIS_TIMEFRAME,
        "fromDate": MORALIS_FROM_DATE,
        # toDate -> acum
        "toDate": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    all_items = []
    cursor = None
    page = 0

    while True:
        if cursor:
            params["cursor"] = cursor
        log(f"Moralis OHLCV: requesting page {page+1}...")
        data = _moralis_get(url, params)
        # Moralis folosește de obicei 'result' pentru listă
        items = data.get("result") or data.get("items") or []
        if not isinstance(items, list):
            raise RuntimeError(f"Unexpected Moralis OHLCV response: {data}")
        log(f"Moralis OHLCV: got {len(items)} items on this page.")
        all_items.extend(items)

        cursor = data.get("cursor") or data.get("nextCursor") or data.get("next_page")
        page += 1
        if not cursor:
            break

    log(f"Moralis OHLCV: total {len(all_items)} candles fetched.")
    if not all_items:
        raise RuntimeError("Moralis OHLCV: no data returned for TLNGOLD/USDT.")

    normalized = []
    for c in all_items:
        ts = c.get("timestamp") or c.get("time")
        if ts is None:
            continue
        try:
            ts_int = int(ts)
        except Exception:
            try:
                dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                ts_int = int(dt.timestamp())
            except Exception as e:
                log(f"Skipping candle with invalid timestamp {ts}: {e}")
                continue

        def to_float(x):
            if x is None:
                return None
            try:
                return float(x)
            except Exception:
                return None

        normalized.append({
            "timestamp_unix": ts_int,
            "timestamp_iso": datetime.utcfromtimestamp(ts_int).isoformat() + "Z",
            "open": to_float(c.get("open")),
            "high": to_float(c.get("high")),
            "low": to_float(c.get("low")),
            "close": to_float(c.get("close")),
            "volume": to_float(c.get("volume")),
        })

    # sortăm după timp
    normalized.sort(key=lambda x: x["timestamp_unix"])
    return normalized


def rebuild_price_history_from_moralis(path=PRICE_FILE):
    """
    Reconstruiește price_history.csv în format OHLCV (Format A)
    folosind istoricul complet Moralis pentru TLNGOLD/USDT.
    """
    candles = fetch_ohlcv_from_moralis()
    fieldnames = ["timestamp_unix", "timestamp_iso", "open", "high", "low", "close", "volume"]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in candles:
            w.writerow(row)

    log(f"Rebuilt {path} with {len(candles)} OHLCV candles from Moralis.")
    return True

# -----------------------------
# OHLCV history loader (for technical analysis)
# -----------------------------
def load_ohlcv_history(path=PRICE_FILE, max_rows=None):
    """
    Încarcă price_history.csv în format OHLCV (Format A).
    Returnează listă de dict-uri:
      {
        "timestamp_unix": int,
        "timestamp_iso": str,
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": float
      }
    """
    if not os.path.exists(path):
        log(f"OHLCV history file {path} does not exist.")
        return []

    rows = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                try:
                    ts_unix = int(row.get("timestamp_unix") or row.get("timestamp") or 0)
                    close = float(row.get("close"))
                    open_p = float(row.get("open"))
                    high = float(row.get("high"))
                    low = float(row.get("low"))
                    vol = float(row.get("volume"))
                except Exception:
                    continue
                rows.append({
                    "timestamp_unix": ts_unix,
                    "timestamp_iso": row.get("timestamp_iso") or "",
                    "open": open_p,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": vol
                })
    except Exception as e:
        log(f"load_ohlcv_history error: {e}")
        return []

    rows.sort(key=lambda x: x["timestamp_unix"])
    if max_rows and len(rows) > max_rows:
        rows = rows[-max_rows:]
    return rows

# -----------------------------
# Technical analysis: support/resistance, trend, volatility
# -----------------------------
def compute_technical_state(candles):
    """
    Primește o listă de lumânări OHLCV (1h).
    Returnează un dict cu:
      - trend: "bull"/"bear"/"sideways"
      - support, resistance
      - volatility_rel
      - atr (Average True Range simplificat)
      - ma_fast (24h), ma_slow (168h)
      - last_close
    """
    if not candles or len(candles) < 50:
        log("Not enough candles for technical analysis.")
        return None

    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    n = len(closes)

    # suport / rezistență = percentile 20% și 80% din ultimele 200 ore
    tail = closes[-200:] if n > 200 else closes[:]
    tail_sorted = sorted(tail)

    def percentile(arr, p):
        if not arr:
            return None
        k = max(0, min(len(arr) - 1, int(len(arr) * p)))
        return arr[k]

    support = percentile(tail_sorted, 0.2)
    resistance = percentile(tail_sorted, 0.8)

    # medii mobile
    def mean_last(arr, k):
        if len(arr) < 1:
            return None
        if len(arr) < k:
            k = len(arr)
        return sum(arr[-k:]) / k

    ma_fast = mean_last(closes, 24)   # ~1 zi
    ma_slow = mean_last(closes, 168)  # ~1 săptămână

    # trend simplu
    trend = "sideways"
    if ma_fast is not None and ma_slow is not None and ma_slow > 0:
        ratio = ma_fast / ma_slow
        if ratio > 1.01:
            trend = "bull"
        elif ratio < 0.99:
            trend = "bear"

    # volatilitate relativă pe ultimele 48 ore
    vol_tail = closes[-48:] if n > 48 else closes[:]
    if len(vol_tail) > 1:
        mean_v = sum(vol_tail) / len(vol_tail)
        var = sum((p - mean_v) ** 2 for p in vol_tail) / (len(vol_tail) - 1)
        std = math.sqrt(var)
        volatility_rel = std / mean_v if mean_v > 0 else 0.0
    else:
        volatility_rel = 0.0

    # ATR simplu pe ultimele 48 ore
    atr_values = []
    prev_close = closes[0]
    for i in range(1, len(candles)):
        h = highs[i]
        l = lows[i]
        c_prev = prev_close
        tr = max(h - l, abs(h - c_prev), abs(l - c_prev))
        atr_values.append(tr)
        prev_close = closes[i]
    if atr_values:
        atr_tail = atr_values[-48:] if len(atr_values) > 48 else atr_values
        atr = sum(atr_tail) / len(atr_tail)
    else:
        atr = 0.0

    last_close = closes[-1]

    log(f"Technical state: trend={trend}, support={support:.6f}, resistance={resistance:.6f}, "
        f"vol_rel={volatility_rel:.4f}, atr={atr:.6f}, last_close={last_close:.6f}")

    return {
        "trend": trend,
        "support": support,
        "resistance": resistance,
        "volatility_rel": volatility_rel,
        "atr": atr,
        "ma_fast": ma_fast,
        "ma_slow": ma_slow,
        "last_close": last_close
    }

# -----------------------------
# Entry plan: buy, take-profit, stop-loss
# -----------------------------
def compute_entry_levels(tech_state):
    """
    Din starea tehnică derivăm nivelele CONSERVATOARE:
      - buy_price
      - take_profit
      - stop_loss

    Stilul conservator pentru TLNGOLD:
      - BUY = suport - 0.5 * ATR
      - TP  = rezistență - 0.5 * ATR
      - SL  = BUY - 3 * ATR
    """
    support = tech_state.get("support")
    resistance = tech_state.get("resistance")
    atr = tech_state.get("atr") or 0.0
    # trend îl păstrăm în caz că vrem să-l folosim în versiunile viitoare,
    # dar în varianta simplă conservatoare NU mai ajustăm după trend.
    trend = tech_state.get("trend")

    if support is None or resistance is None:
        log("Support/resistance missing; cannot compute entry levels.")
        return None

    # BUY: zona de valoare, puțin sub suport (deep value)
    if atr > 0:
        buy_price = max(0.0, support - 0.5 * atr)
    else:
        # fallback: ușor sub suport
        buy_price = max(0.0, support * 0.98)

    # TAKE PROFIT: puțin sub rezistență, ca să fie atins mai des
    if atr > 0:
        take_profit = max(0.0, resistance - 0.5 * atr)
    else:
        take_profit = max(0.0, resistance * 0.98)

    # STOP LOSS: mult mai jos, stil conservator (3 * ATR)
    if atr > 0:
        stop_loss = max(0.0, buy_price - 3.0 * atr)
    else:
        # fallback: ~11% sub BUY
        stop_loss = max(0.0, buy_price * 0.89)

    log(
        f"Entry levels (conservative): "
        f"buy={buy_price:.6f}, tp={take_profit:.6f}, sl={stop_loss:.6f}, "
        f"support={support:.6f}, resistance={resistance:.6f}, atr={atr:.6f}"
    )
    return {
        "buy_price": buy_price,
        "take_profit": take_profit,
        "stop_loss": stop_loss
    }

# -----------------------------
# Price fetch with caching/backoff
# -----------------------------
def fetch_price_live(force_refresh=False):
    """Return float price or None on any problem. Uses small cache to reduce calls."""
    global _last_price_cache
    now_ts = time.time()
    # simple cache check
    if not force_refresh:
        cached = _last_price_cache.get("price")
        cached_ts = _last_price_cache.get("ts", 0)
        if cached is not None and (now_ts - cached_ts) < CACHE_TTL_SEC:
            return cached

    try:
        r = requests.get(DEXSCREENER_TOKEN_API, timeout=10)
        r.raise_for_status()
        data = r.json()
        pairs = data.get("pairs") or []
        price_val = None
        for p in pairs:
            price = p.get("priceUsd")
            if price:
                try:
                    pval = float(price)
                    if pval > 0 and math.isfinite(pval):
                        price_val = pval
                        break
                except:
                    continue
        # fallback
        if price_val is None and pairs:
            try:
                price_val = float(pairs[0].get("priceUsd", 0))
            except:
                price_val = None
        if price_val is not None:
            _last_price_cache["price"] = price_val
            _last_price_cache["ts"] = now_ts
            return price_val
        return None
    except Exception as e:
        log(f"Price fetch error: {e}")
        return None

# -----------------------------
# Simulation execution (robust)
# -----------------------------
def simulate_buy(state, price, amount_usdt):
    # reset daily if needed
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if state.get("last_day") != today:
        state["daily_spent"] = 0.0
        state["last_day"] = today

    remaining_daily = max(0.0, MAX_DAILY_SPEND_USDT - state.get("daily_spent", 0.0))
    available_usdt = min(state.get("usdt", 0.0), remaining_daily)
    amount_to_use = min(amount_usdt, available_usdt)

    if amount_to_use <= 0:
        return False, "zero_amount"

    slippage = amount_to_use * SLIPPAGE_PCT
    fee = amount_to_use * FEE_PCT
    effective = amount_to_use - slippage - fee

    # avoid tiny amounts that produce zero tokens
    tln_bought = effective / price if price > 0 else 0.0
    if tln_bought <= 0:
        return False, "zero_qty"

    # update state and persist
    state["usdt"] = round(state.get("usdt", 0.0) - amount_to_use, 8)
    state["tln"] = round(state.get("tln", 0.0) + tln_bought, 8)
    state["last_trade_ts"] = time.time()
    state["daily_spent"] = round(state.get("daily_spent", 0.0) + amount_to_use, 8)

    save_state(state)
    append_trade(int(time.time()), "BUY", price, tln_bought, amount_to_use)
    log(f"SIM BUY: {tln_bought:.6f} TLN @ {price:.6f} (spent {amount_to_use:.4f} USDT)")
    # --- Telegram notification for BUY executed ---
    msg_buy = (
        "🟢 <b>TLN Bot — BUY Executat</b>\n\n"
        f"Preț BUY: <b>{price:.6f}</b>\n"
        f"TLN cumpărat: <b>{tln_bought:.6f}</b>\n"
        f"Suma folosită: <b>{amount_to_use:.2f} USDT</b>\n\n"
        f"Balanță curentă USDT: {state.get('usdt',0.0):.2f}\n"
        f"Mod: <b>waiting_for_sell</b>\n"
    )
    send_telegram_message(msg_buy)
    return True, None

def simulate_sell(state, price, pct=1.0):
    if state.get("tln", 0.0) <= 0:
        return False, "no_tln"
    sell_amount_tln = state["tln"] * pct
    if sell_amount_tln <= 0:
        return False, "zero_qty"

    gross = sell_amount_tln * price
    slippage = gross * SLIPPAGE_PCT
    fee = gross * FEE_PCT
    net = gross - slippage - fee

    # update state
    state["tln"] = round(state.get("tln", 0.0) - sell_amount_tln, 8)
    state["usdt"] = round(state.get("usdt", 0.0) + net, 8)
    state["last_trade_ts"] = time.time()

    save_state(state)
    append_trade(int(time.time()), "SELL", price, sell_amount_tln, net)
    log(f"SIM SELL: {sell_amount_tln:.6f} TLN @ {price:.6f} (received {net:.4f} USDT)")
    return True, None

# -----------------------------
# Threshold computation
# -----------------------------
def compute_thresholds(prices_window, k_std):
    n = len(prices_window)
    if n == 0: return None
    mean = sum(prices_window) / n
    var = sum((p - mean)**2 for p in prices_window) / n
    std = math.sqrt(var)
    buy = max(0.0, mean - k_std * std)
    sell = mean + k_std * std
    return mean, std, buy, sell

# -----------------------------
# Backtest runner (unchanged)
# -----------------------------
def run_backtest(history_csv):
    ensure_csv_headers()
    state = load_state()
    window = []
    try:
        with open(history_csv, "r", encoding="utf-8") as f:
            r = csv.reader(f)
            header = next(r, None)
            for row in r:
                try:
                    ts = int(row[0])
                    price = float(row[1])
                except:
                    continue
                append_price(ts, price)
                window.append(price)
                if len(window) > ROLLING_WINDOW:
                    window.pop(0)
                if len(window) >= 3:
                    mean, std, buy_thr, sell_thr = compute_thresholds(window, K_STD)
                    if price <= buy_thr:
                        simulate_buy(state, price, POSITION_SIZE_USDT)
                    elif price >= sell_thr:
                        simulate_sell(state, price, pct=1.0)
    except FileNotFoundError:
        log(f"Backtest file not found: {history_csv}")
        return
    final_usdt = state["usdt"]
    final_tln = state["tln"]
    last_price = window[-1] if window else 0
    total = final_usdt + final_tln * last_price
    log(f"Backtest complete. Final USDT={final_usdt:.4f}, TLN={final_tln:.6f}, TotalEquiv={total:.4f}")

# -----------------------------
# Remote export (optional)
# -----------------------------
def export_state_to_remote():
    """
    Try to upload state.json and trades/price files to S3 if configured.
    If boto3 missing or AWS not configured, just log and return False.
    """
    global _last_remote_export
    now_ts = time.time()
    # rate-limit uploads
    if now_ts - _last_remote_export.get("ts", 0) < REMOTE_EXPORT_INTERVAL_SEC:
        return _last_remote_export.get("status", None)

    if not _HAS_BOTO3:
        log("export_state_to_remote: boto3 not available, skipping remote export.")
        _last_remote_export.update({"ts": now_ts, "status": False})
        return False

    if not AWS_S3_BUCKET:
        log("export_state_to_remote: AWS_S3_BUCKET not set, skipping remote export.")
        _last_remote_export.update({"ts": now_ts, "status": False})
        return False

    s3 = boto3.client("s3")
    try:
        for fname in [STATE_FILE, TRADES_FILE, PRICE_FILE]:
            if not os.path.exists(fname):
                continue
            key = f"{AWS_S3_PREFIX}/{os.path.basename(fname)}"
            log(f"Uploading {fname} -> s3://{AWS_S3_BUCKET}/{key}")
            s3.upload_file(fname, AWS_S3_BUCKET, key)
        _last_remote_export.update({"ts": now_ts, "status": True})
        log("export_state_to_remote: upload successful.")
        return True
    except (BotoCoreError, ClientError) as e:
        log(f"export_state_to_remote error: {e}")
        _last_remote_export.update({"ts": now_ts, "status": False})
        return False
    except Exception as e:
        log(f"export_state_to_remote unexpected error: {e}")
        _last_remote_export.update({"ts": now_ts, "status": False})
        return False

# -----------------------------
# Main live loop (keeps own exceptions local)
# -----------------------------
def run_live(poll_interval=POLL_INTERVAL):
    global LAST_PRICE, LAST_PRICE_TS, LAST_LOOP_TS, PRICE_SAMPLES_COUNT, _last_remote_export
    ensure_csv_headers()

    # 1) Istoric Moralis dacă e nevoie
    try:
        if MORALIS_API_KEY:
            if is_price_history_incomplete(PRICE_FILE):
                log("price_history.csv missing or incomplete -> rebuilding from Moralis...")
                rebuild_price_history_from_moralis(PRICE_FILE)
            else:
                log("price_history.csv seems OK; skipping Moralis rebuild.")
        else:
            log("MORALIS_API_KEY is not set; skipping Moralis price history rebuild.")
    except Exception as e:
        log(f"Moralis history init failed: {e}. Will continue with existing price_history.csv (if any).")

    # 2) Încărcăm state + istoricul OHLCV
    state = load_state()
    usdt_balance_before = state.get("usdt", 0.0)
    candles = load_ohlcv_history(PRICE_FILE)
    if not candles or len(candles) < 50:
        log("LIVE: not enough OHLCV data; bot cannot trade safely.")
    else:
        # dacă nu avem încă un entry_plan, îl calculăm
        if "entry_plan" not in state or not isinstance(state["entry_plan"], dict):
            log("LIVE: computing technical state and entry plan...")
            tech = compute_technical_state(candles)
            if tech:
                levels = compute_entry_levels(tech)
                if levels:
                    # TLN_V2 simplă: presupunem că acest ciclu pornește din USDT,
                    # deci modul inițial este ÎNTOTDEAUNA "waiting_for_buy".
                    # Dacă există deja TLN în portofel, doar logăm un avertisment,
                    # dar nu tratăm poziția ca fiind "în curs" în această versiune.
                    tln_balance = state.get("tln", 0.0)
                    if tln_balance > 0:
                        log(
                            f"LIVE: WARNING - TLN balance={tln_balance:.6f} > 0, "
                            "dar TLN_V2 simplă pornește din scenariul 'fără poziție'. "
                            "Poziția existentă NU este gestionată de acest entry_plan."
                        )

                    mode = "waiting_for_buy"

                    state["entry_plan"] = {
                        "buy_price": levels["buy_price"],
                        "take_profit": levels["take_profit"],
                        "stop_loss": levels["stop_loss"],
                        "mode": mode,
                        "created_at": now_iso()
                    }
                    save_state(state)
                    log(f"LIVE: entry_plan initialized with mode={mode}.")
                 
                    # Mesaj Telegram la începutul ciclului
                    msg = (
                        "📊 <b>TLN Bot — Ciclul Nou a Început</b>\n\n"
                        f"Trend: <b>{tech.get('trend')}</b>\n"
                        f"Volatilitate: {tech.get('volatility_rel'):.4f}\n"
                        f"ATR: {tech.get('atr'):.6f}\n\n"
                        f"BUY: <b>{levels['buy_price']:.6f}</b>\n"
                        f"TP: <b>{levels['take_profit']:.6f}</b>\n"
                        f"SL: <b>{levels['stop_loss']:.6f}</b>\n\n"
                        f"Capital disponibil: {state.get('usdt',0.0):.2f} USDT\n"
                        f"Mod: <b>{mode}</b>\n"
                    )
                    send_telegram_message(msg)          
                else:
                    log("LIVE: could not compute entry levels; entry_plan not set.")
            else:
                log("LIVE: technical state not available; entry_plan not set.")
        else:
            log("LIVE: entry_plan already present in state; reusing it.")
 
    price_buffer = []

    log(f"Starting LIVE mode. Version={BOT_VERSION}")
    while True:
        try:
            price = fetch_price_live()
            if price is None:
                log("Price fetch failed; retrying in 10s")
                time.sleep(10)
                continue

            ts = int(time.time())

            # update global last price (pentru /status)
            with _lock:
                LAST_PRICE = price
                LAST_PRICE_TS = ts

            log(f"LIVE price {price:.8f}")

            state = load_state()
            entry_plan = state.get("entry_plan")
            tln_balance = state.get("tln", 0.0)
            usdt_balance = state.get("usdt", 0.0)

            if not entry_plan or not isinstance(entry_plan, dict):
                log("LIVE: no entry_plan in state; sleeping (no trading).")
                time.sleep(poll_interval)
                continue

            mode = entry_plan.get("mode", "inactive")
            buy_price = entry_plan.get("buy_price")
            take_profit = entry_plan.get("take_profit")
            stop_loss = entry_plan.get("stop_loss")

            # log de status
            log(f"LIVE status: mode={mode}, buy={buy_price}, tp={take_profit}, sl={stop_loss}, "
                f"USDT={usdt_balance:.4f}, TLN={tln_balance:.6f}")

            if mode == "waiting_for_buy":
                # nu avem poziție TLN (ideal tln_balance=0)
                if price <= buy_price and usdt_balance > 0:
                    amount_to_use = min(POSITION_SIZE_USDT, usdt_balance)
                    if amount_to_use > 0:
                        log(f"LIVE: price <= buy_price -> BUY using {amount_to_use:.4f} USDT.")
                        ok, info = simulate_buy(state, price, amount_to_use)
                        if ok:
                            # recitim state (simulate_buy a făcut save_state)
                            state = load_state()
                            tln_balance = state.get("tln", 0.0)
                            entry_plan = state.get("entry_plan", entry_plan)
                            entry_plan["mode"] = "waiting_for_sell"
                            state["entry_plan"] = entry_plan
                            save_state(state)
                            log("LIVE: entry_plan mode switched to waiting_for_sell.")
                        else:
                            log(f"LIVE: BUY skipped: {info}")
                    else:
                        log("LIVE: no USDT available for BUY.")
                else:
                    log("LIVE: waiting_for_buy -> no condition met this tick.")

            elif mode == "waiting_for_sell":
                # avem TLN și așteptăm TP sau SL
                if tln_balance <= 0:
                    log("LIVE: waiting_for_sell but TLN balance is 0 -> switching to inactive.")
                    entry_plan["mode"] = "inactive"
                    state["entry_plan"] = entry_plan
                    save_state(state)
                else:
                    if price >= take_profit:
                        log("LIVE: price >= take_profit -> SELL ALL (take profit).")
                        simulate_sell(state, price, pct=1.0)
                        state = load_state()
                        entry_plan = state.get("entry_plan", entry_plan)
                        entry_plan["mode"] = "inactive"
                        state["entry_plan"] = entry_plan
                        save_state(state)
                        log("LIVE: entry_plan mode set to inactive (take profit reached).")
                     
                        # Reîncărcăm state-ul pentru balanțe actualizate
                        final_state = load_state()
                        final_usdt = final_state.get("usdt", 0.0)
                        final_tln = final_state.get("tln", 0.0)
                        
                        msg_final = (
                            "🏁 <b>TLN Bot — Ciclul s-a Încheiat</b>\n\n"
                            f"BUY: <b>{entry_plan['buy_price']:.6f}</b>\n"
                            f"SELL: <b>{price:.6f}</b>\n\n"
                        )
                        
                        # profit/pierdere calculat simplu
                        profit = final_usdt - usdt_balance_before
                        pct = (profit / usdt_balance_before) * 100 if usdt_balance_before > 0 else 0
                        
                        if price >= entry_plan["take_profit"]:
                            msg_final += f"🏆 <b>TAKE PROFIT</b> atins!\n"
                        else:
                            msg_final += f"⚠️ <b>STOP LOSS</b> activat.\n"
                        
                        msg_final += (
                            f"\nProfit/Pierdere: <b>{profit:.2f} USDT ({pct:.2f}%)</b>\n"
                            f"Balanță finală: <b>{final_usdt:.2f} USDT</b>\n"
                            f"Mod: <b>inactive</b>\n"
                        )
                        
                        send_telegram_message(msg_final)            
                    elif price <= stop_loss:
                        log("LIVE: price <= stop_loss -> SELL ALL (stop loss).")
                        simulate_sell(state, price, pct=1.0)
                        state = load_state()
                        entry_plan = state.get("entry_plan", entry_plan)
                        entry_plan["mode"] = "inactive"
                        state["entry_plan"] = entry_plan
                        save_state(state)
                        log("LIVE: entry_plan mode set to inactive (stop loss hit).")

                        # Reîncărcăm state-ul pentru balanțe actualizate
                        final_state = load_state()
                        final_usdt = final_state.get("usdt", 0.0)
                        final_tln = final_state.get("tln", 0.0)
                        
                        msg_final = (
                            "🏁 <b>TLN Bot — Ciclul s-a Încheiat</b>\n\n"
                            f"BUY: <b>{entry_plan['buy_price']:.6f}</b>\n"
                            f"SELL: <b>{price:.6f}</b>\n\n"
                        )
                        
                        # profit/pierdere calculat simplu
                        profit = final_usdt - usdt_balance_before
                        pct = (profit / usdt_balance_before) * 100 if usdt_balance_before > 0 else 0
                        
                        if price >= entry_plan["take_profit"]:
                            msg_final += f"🏆 <b>TAKE PROFIT</b> atins!\n"
                        else:
                            msg_final += f"⚠️ <b>STOP LOSS</b> activat.\n"
                        
                        msg_final += (
                            f"\nProfit/Pierdere: <b>{profit:.2f} USDT ({pct:.2f}%)</b>\n"
                            f"Balanță finală: <b>{final_usdt:.2f} USDT</b>\n"
                            f"Mod: <b>inactive</b>\n"
                        )
                        
                        send_telegram_message(msg_final)             
                    else:
                        log("LIVE: waiting_for_sell -> HOLD (no TP/SL condition).")
            else:
                # inactive sau mod necunoscut
                log("LIVE: entry_plan mode is inactive; no further trades will be made.")
                time.sleep(poll_interval)
                continue

            LAST_LOOP_TS = int(time.time())
            time.sleep(poll_interval)

        except Exception as e:
            log(f"Unhandled exception in run_live: {e}")
            tb = traceback.format_exc()
            log(tb)
            raise

# -----------------------------
# Flask endpoints for health/status
# -----------------------------
app = Flask(__name__)

@app.route("/")
def home():
    return f"TLN Adaptive Bot active. Version={BOT_VERSION}"

# health endpoint 
@app.route("/health")
def health():
    return jsonify({"mode": "live" if not TEST_MODE else "test", "time": int(time.time()), "version": BOT_VERSION})

# anti-sleep ping endpoint
@app.route("/ping")
def ping():
    return {"status": "alive"}

# status endpoint
@app.route("/status")
def status():
    # state
    state = load_state()
    last_trade_ts = state.get("last_trade_ts")
    last_trade_iso = datetime.utcfromtimestamp(last_trade_ts).isoformat() if last_trade_ts else None

    # trade count
    try:
        with open(TRADES_FILE, "r", encoding="utf-8") as f:
            trade_count = sum(1 for _ in csv.reader(f)) - 1
            if trade_count < 0:
                trade_count = 0
    except Exception:
        trade_count = 0

    with _lock:
        last_price = LAST_PRICE
        last_price_ts = LAST_PRICE_TS
        samples = PRICE_SAMPLES_COUNT

    return jsonify({
        "version": BOT_VERSION,
        "mode": "live" if not TEST_MODE else "test",
        "usdt_balance": state.get("usdt", 0.0),
        "tln_balance": state.get("tln", 0.0),
        "last_price": last_price,
        "last_price_ts": last_price_ts,
        "last_trade_time": last_trade_iso,
        "trade_count": trade_count,
        "samples_in_buffer": samples,
        "server_time": int(time.time())
    })

@app.route("/dashboard")
def dashboard():
    # alias to /status
    return status()

# -----------------------------
# EXPORT: DOWNLOAD FILES
# -----------------------------

@app.route("/export/trades")
def export_trades():
    if not os.path.exists(TRADES_FILE):
        return jsonify({"error": "trades.csv not found"}), 404
    return send_file(TRADES_FILE, as_attachment=True)

@app.route("/export/prices")
def export_prices():
    if not os.path.exists(PRICE_FILE):
        return jsonify({"error": "price_history.csv not found"}), 404
    return send_file(PRICE_FILE, as_attachment=True)

@app.route("/export/state")
def export_state():
    if not os.path.exists(STATE_FILE):
        return jsonify({"error": "state.json not found"}), 404
    return send_file(STATE_FILE, as_attachment=True)

# -----------------------------
# Debug endpoints (read-only)
# -----------------------------
@app.route("/debug/files")
def debug_files():
    """Return list of generated files."""
    files = []
    for fname in [STATE_FILE, PRICE_FILE, TRADES_FILE, LOG_FILE]:
        size = os.path.getsize(fname) if os.path.exists(fname) else 0
        files.append({"file": fname, "exists": os.path.exists(fname), "size": size})
    return jsonify({"version": BOT_VERSION, "files": files})

@app.route("/debug/trades")
def debug_trades():
    """Return last 200 lines from trades.csv."""
    if not os.path.exists(TRADES_FILE):
        return jsonify({"error": "trades.csv not found"})
    try:
        with open(TRADES_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        return jsonify({
            "version": BOT_VERSION,
            "lines": lines[-200:]  # last 200 entries
        })
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/debug/log")
def debug_log():
    """Return last 300 lines from bot.log."""
    if not os.path.exists(LOG_FILE):
        return jsonify({"error": "bot.log not found"})
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        return jsonify({
            "version": BOT_VERSION,
            "lines": lines[-300:]  # last 300 logs
        })
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/debug/remote_export_status")
def debug_remote_export_status():
    return jsonify(_last_remote_export)

def run_flask():
    if FLASK_ENABLED:
        try:
            app.run(host="0.0.0.0", port=FLASK_PORT)
        except Exception as e:
            log(f"Flask run error: {e}")

# -----------------------------
# Watchdog / Entrypoint
# -----------------------------
def main_watchdog(mode="live", history_file=None):
    backoff = RETRY_BACKOFF_BASE
    # start flask once (daemon)
    if FLASK_ENABLED:
        threading.Thread(target=run_flask, daemon=True).start()
    while True:
        try:
            if mode == "backtest":
                run_backtest(history_file or PRICE_FILE)
                log("Backtest finished; exiting watchdog.")
                return
            else:
                run_live(POLL_INTERVAL)
                # dacă run_live iese fără excepție, dormim ca să evităm loop turbo
                time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            log("Interrupted by user. Exiting watchdog.")
            return
        except Exception as e:
            log(f"Watchdog caught exception: {e}")
            tb = traceback.format_exc()
            log(tb)
            log(f"Restarting run_live after {backoff} seconds...")
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
            continue

# -----------------------------
# CLI
# -----------------------------
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["live","backtest"], default="live")
    p.add_argument("--history", help="CSV for backtest (timestamp,price)")
    p.add_argument("--test", action="store_true")
    args = p.parse_args()
    if args.test:
        TEST_MODE = True
    if args.mode == "backtest":
        main_watchdog(mode="backtest", history_file=args.history)
    else:
        main_watchdog(mode="live")
