#!/usr/bin/env python3
"""
tln_adaptive_bot.py - Replit-ready resilient adaptive bot + status endpoints

- Restart loop / watchdog: bucla principală rulează într-un loop extern care
  repornește run_live() la excepții.
- Endpoints:
    /        -> health
    /health  -> health JSON
    /status  -> detailed status (usdt, tln, last_price, last_exec_ts, trade_count)
    /dashboard -> same as /status
- Safety: no private keys, paper-trading only
"""

import requests, time, csv, json, os, math, threading, traceback
from datetime import datetime
from flask import Flask, jsonify

# -----------------------------
# CONFIG (editează pentru cloud)
# -----------------------------
BOT_VERSION = "1.1-replit-stable-20251118"
TOKEN_CONTRACT = "0xAa90a8CDAB8B8E902293a2817d1d286f66cBcec5"
DEXSCREENER_TOKEN_API = f"https://api.dexscreener.com/latest/dex/tokens/{TOKEN_CONTRACT}"

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

# Flask / Replit
TEST_MODE = False
FLASK_ENABLED = True
FLASK_PORT = int(os.environ.get("PORT", 5000))

# resilience
RETRY_BACKOFF_BASE = 5    # seconds initial backoff after an uncaught exception
MAX_BACKOFF = 300         # cap backoff to 5 minutes

# globals for endpoints
LAST_PRICE = None
LAST_PRICE_TS = None
LAST_LOOP_TS = None
PRICE_SAMPLES_COUNT = 0

_lock = threading.Lock()

# -----------------------------
# Utilities: logging & csv
# -----------------------------
def log(msg):
    ts = datetime.utcnow().isoformat()
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
    # price history
    if not os.path.exists(PRICE_FILE):
        with open(PRICE_FILE, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["timestamp","price"])

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
# Price fetch with validation
# -----------------------------
def fetch_price_live():
    """Return float price or None on any problem."""
    try:
        r = requests.get(DEXSCREENER_TOKEN_API, timeout=10)
        r.raise_for_status()
        data = r.json()
        pairs = data.get("pairs") or []
        for p in pairs:
            price = p.get("priceUsd")
            if price:
                # validate number-like
                try:
                    pval = float(price)
                    if pval > 0 and math.isfinite(pval):
                        return pval
                except:
                    continue
        # fallback if no usable pair
        if pairs:
            try:
                pval = float(pairs[0].get("priceUsd", 0))
                if pval > 0 and math.isfinite(pval):
                    return pval
            except:
                pass
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
# Main live loop (keeps own exceptions local)
# -----------------------------
def run_live(poll_interval=POLL_INTERVAL):
    global LAST_PRICE, LAST_PRICE_TS, LAST_LOOP_TS, PRICE_SAMPLES_COUNT
    ensure_csv_headers()
    state = load_state()
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
            append_price(ts, price)

            # update global last price safely
            with _lock:
                LAST_PRICE = price
                LAST_PRICE_TS = ts

            price_buffer.append(price)
            PRICE_SAMPLES_COUNT = len(price_buffer)
            if len(price_buffer) > ROLLING_WINDOW:
                price_buffer.pop(0)

            log(f"Price {price:.8f} | samples {len(price_buffer)}/{ROLLING_WINDOW}")

            # quick-exit in TEST_MODE
            if TEST_MODE:
                ok, info = simulate_buy(state, price, min(POSITION_SIZE_USDT, state["usdt"]))
                if not ok:
                    log(f"Test buy failed: {info}")
                else:
                    log("Test buy recorded in trades.csv (TEST_MODE). Exiting.")
                return

            if len(price_buffer) < max(3, ROLLING_WINDOW):
                time.sleep(poll_interval)
                continue

            mean, std, buy_thr, sell_thr = compute_thresholds(price_buffer, K_STD)
            log(f"Mean={mean:.8f} Std={std:.8f} BUY<{buy_thr:.8f} SELL>{sell_thr:.8f}")

            # cooldown check
            last_trade_ts = state.get("last_trade_ts") or 0
            if time.time() - last_trade_ts < COOLDOWN_AFTER_TRADE:
                log("In cooldown after last trade; skipping decision.")
                time.sleep(poll_interval)
                continue

            # Decision
            if price <= buy_thr:
                ok, info = simulate_buy(state, price, min(POSITION_SIZE_USDT, state["usdt"]))
                if not ok:
                    log(f"Buy skipped: {info}")
            elif price >= sell_thr and state.get("tln", 0.0) > 0:
                simulate_sell(state, price, pct=1.0)

            LAST_LOOP_TS = int(time.time())
            time.sleep(poll_interval)

        except Exception as e:
            # catch unexpected exception inside live loop,
            # log traceback and re-raise to outer watchdog (or sleep and continue local)
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

from flask import send_file

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
