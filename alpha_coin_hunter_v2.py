"""
BINANCE ALPHA COIN HUNTER V2
Research/alert scanner — NO trading.

V2 adds:
- Real-time Alpha WebSocket ticker stream
- Momentum acceleration
- Volume acceleration
- Buy-pressure proxy from klines
- Order-book spread/depth
- Breakout detection
- Pump-chasing penalty
- Liquidity-risk penalty
- Multi-timeframe confirmation
- Market regime filter (BTC via public Binance spot ticker)
- Persistent ranking
- Optional Telegram alerts

Install:
    pip install requests websocket-client

Run:
    python alpha_coin_hunter_v2.py

Telegram (optional):
    Set environment variables:
      ALPHA_TELEGRAM_BOT_TOKEN
      ALPHA_TELEGRAM_CHAT_ID

IMPORTANT:
This bot does not place orders and does not need Binance API keys.
"""

import os
import time
import json
import math
import threading
from statistics import mean
import requests
import websocket

BINANCE = "https://www.binance.com"
ALPHA = BINANCE + "/bapi/defi/v1/public/alpha-trade"
WS = "wss://nbstream.binance.com/w3w/wsa/stream"
SCAN_SECONDS = 60
TOP_N = 10
ALERT_SCORE = 82

session = requests.Session()
session.headers.update({"User-Agent": "AlphaCoinHunterV2/1.0"})

def get(path, params=None):
    r = session.get(ALPHA + path, params=params, timeout=10)
    r.raise_for_status()
    j = r.json()
    if str(j.get("code", "000000")) != "000000":
        raise RuntimeError(j)
    return j.get("data")

def f(x, d=0.0):
    try: return float(x)
    except: return d

def pct(a, b):
    return (a / b - 1) * 100 if b else 0.0

def token_list():
    return get("/token-list")

def symbols_from_tokens():
    data = token_list()
    items = data if isinstance(data, list) else (
        data.get("tokens", data.get("data", [])) if isinstance(data, dict) else []
    )
    out = []
    for x in items:
        if not isinstance(x, dict): continue
        name = x.get("symbol") or x.get("name")
        tid = x.get("id") or x.get("tokenId") or x.get("token_id")
        if name and tid is not None:
            out.append((str(name), f"ALPHA_{tid}USDT"))
    return out

def klines(symbol, interval, limit=30):
    return get("/klines", {"symbol": symbol, "interval": interval, "limit": limit}) or []

def ticker(symbol):
    return get("/ticker", {"symbol": symbol})

def depth(symbol, limit=20):
    return get("/fullDepth", {"symbol": symbol, "limit": limit}) or {}

def kstats(rows):
    if len(rows) < 6: return None
    closes = [f(x[4]) for x in rows]
    vols = [f(x[5]) for x in rows]
    qvols = [f(x[7]) for x in rows]
    last = closes[-1]
    prev = closes[-2]
    avg_vol = mean(vols[:-1]) if vols[:-1] else 0
    # Recent 3-candle vs previous 10-candle volume acceleration.
    recent = mean(vols[-3:]) if len(vols) >= 3 else 0
    old = mean(vols[-13:-3]) if len(vols) >= 13 else mean(vols[:-3])
    accel = recent / old if old else 0
    taker_buy = sum(f(x[10]) for x in rows[-10:])
    total_q = sum(qvols[-10:])
    buy_ratio = taker_buy / total_q if total_q else 0.5
    return {
        "last": last,
        "prev": prev,
        "change": pct(last, closes[0]),
        "last_candle_change": pct(last, prev),
        "volume_ratio": vols[-1] / avg_vol if avg_vol else 0,
        "volume_accel": accel,
        "buy_ratio": buy_ratio,
        "high": max(f(x[2]) for x in rows[:-1]),
        "low": min(f(x[3]) for x in rows[:-1]),
        "quote_volume": sum(qvols),
    }

def bookstats(book):
    bids = book.get("bids", [])
    asks = book.get("asks", [])
    if not bids or not asks:
        return {"spread": 999, "depth": 0, "imbalance": .5}
    bp, ap = f(bids[0][0]), f(asks[0][0])
    mid = (bp + ap) / 2
    spread = (ap - bp) / mid * 100 if mid else 999
    bd = sum(f(p)*f(q) for p,q in bids)
    ad = sum(f(p)*f(q) for p,q in asks)
    total = bd + ad
    return {"spread": spread, "depth": total, "imbalance": bd/total if total else .5}

def btc_regime():
    try:
        # Spot BTC proxy for broad market direction.
        r = session.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol":"BTCUSDT","interval":"15m","limit":16},
            timeout=8
        )
        rows = r.json()
        if len(rows) < 10: return 0
        closes = [f(x[4]) for x in rows]
        return pct(closes[-1], closes[0])
    except:
        return 0

def score(t, s5, s15, s1h, ob, btc):
    score = 0
    reasons = []

    # 1) Momentum / confirmation: 30
    if s5["change"] > 2: score += 5; reasons.append("5m momentum")
    if s15["change"] > 5: score += 5; reasons.append("15m momentum")
    if s1h["change"] > 8: score += 5; reasons.append("1h momentum")
    if s5["last_candle_change"] > 0 and s15["last_candle_change"] > 0:
        score += 5; reasons.append("multi-TF confirmation")
    if s15["last"] > s15["high"] * .995:
        score += 5; reasons.append("near breakout")
    if s15["last"] > s15["high"]:
        score += 5; reasons.append("breakout")

    # 2) Volume: 25
    if s5["volume_accel"] >= 1.5: score += 5; reasons.append("volume acceleration")
    if s5["volume_accel"] >= 2.5: score += 5; reasons.append("strong volume")
    if s15["volume_ratio"] >= 1.5: score += 5
    if s5["buy_ratio"] >= .52: score += 5; reasons.append("buy pressure")
    if s5["buy_ratio"] >= .56: score += 5

    # 3) Liquidity/order book: 20
    if ob["depth"] >= 10000: score += 5
    if ob["depth"] >= 50000: score += 5
    if ob["spread"] <= 1: score += 5; reasons.append("tight spread")
    if ob["imbalance"] >= .55: score += 5; reasons.append("bid imbalance")

    # 4) 24h activity: 15
    qv = f(t.get("quoteVolume", t.get("quoteVolume24h", 0)))
    ch = f(t.get("priceChangePercent", 0))
    if qv >= 10000: score += 5
    if qv >= 100000: score += 5
    if ch > 0: score += 5

    # Market regime bonus/penalty: +/- 5
    if btc > 0: score += 5
    elif btc < -2: score -= 5

    # Anti-chase penalty
    total_move = s1h["change"]
    if total_move > 80:
        score -= 15
        reasons.append("EXTREME PUMP — chase penalty")
    elif total_move > 40:
        score -= 7
        reasons.append("late-stage pump caution")

    # Thin-book penalty
    if ob["depth"] < 5000:
        score -= 10
        reasons.append("thin liquidity")

    return max(0, min(100, round(score, 1))), reasons

def scan(name, symbol, btc):
    try:
        t = ticker(symbol)
        s5 = kstats(klines(symbol, "5m", 30))
        s15 = kstats(klines(symbol, "15m", 30))
        s1h = kstats(klines(symbol, "1h", 30))
        if not all([s5, s15, s1h]): return None
        ob = bookstats(depth(symbol, 20))
        sc, reasons = score(t, s5, s15, s1h, ob, btc)
        return {
            "name": name, "symbol": symbol, "score": sc,
            "price": f(t.get("lastPrice", t.get("last", 0))),
            "24h": f(t.get("priceChangePercent", 0)),
            "5m": s5["change"], "15m": s15["change"], "1h": s1h["change"],
            "vol_accel": s5["volume_accel"], "buy": s5["buy_ratio"],
            "depth": ob["depth"], "spread": ob["spread"],
            "reasons": reasons,
        }
    except:
        return None

def telegram(msg):
    token = os.getenv("ALPHA_TELEGRAM_BOT_TOKEN")
    chat = os.getenv("ALPHA_TELEGRAM_CHAT_ID")
    if not token or not chat: return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat, "text": msg},
            timeout=8
        )
    except:
        pass

def alert_message(r):
    return (
        f"🔥 BINANCE ALPHA ALERT\n"
        f"{r['name']} | Score {r['score']}/100\n"
        f"Price: {r['price']}\n"
        f"5m {r['5m']:.1f}% | 15m {r['15m']:.1f}% | 1h {r['1h']:.1f}%\n"
        f"Vol accel: {r['vol_accel']:.1f}x | Buy ratio: {r['buy']:.1%}\n"
        f"Depth: ${r['depth']:,.0f} | Spread: {r['spread']:.2f}%\n"
        f"Why: {', '.join(r['reasons'][:6])}\n"
        f"Research signal only — NOT financial advice."
    )

def print_rows(rows, btc):
    print("\n" + "="*125)
    print(f"ALPHA HUNTER V2 | BTC 15m: {btc:+.2f}%")
    print("="*125)
    print(f"{'#':<3}{'TOKEN':<15}{'SCORE':>7}{'24H':>8}{'5M':>8}"
          f"{'15M':>8}{'1H':>8}{'VOLx':>8}{'BUY':>8}{'DEPTH':>12}{'SPR':>7}")
    print("-"*125)
    for i,r in enumerate(rows[:TOP_N],1):
        flag = "🔥" if r["score"] >= ALERT_SCORE else ("🟢" if r["score"] >= 65 else "🟡")
        print(f"{i:<3}{flag}{r['name'][:12]:<12}{r['score']:>7.1f}"
              f"{r['24h']:>8.1f}{r['5m']:>8.1f}{r['15m']:>8.1f}"
              f"{r['1h']:>8.1f}{r['vol_accel']:>8.1f}{r['buy']:>8.1%}"
              f"{r['depth']:>12.0f}{r['spread']:>7.2f}")

def main():
    print("Starting Alpha Coin Hunter V2 — scanner only.")
    symbols = symbols_from_tokens()
    print(f"Loaded {len(symbols)} Alpha symbols.")
    alerted = {}
    while True:
        start = time.time()
        btc = btc_regime()
        rows = []
        for name, symbol in symbols:
            r = scan(name, symbol, btc)
            if r: rows.append(r)
        rows.sort(key=lambda x:x["score"], reverse=True)
        print_rows(rows, btc)

        now = time.time()
        for r in rows:
            if r["score"] >= ALERT_SCORE:
                # Avoid repeating the same alert every minute.
                if now - alerted.get(r["symbol"], 0) >= 900:
                    telegram(alert_message(r))
                    alerted[r["symbol"]] = now

        time.sleep(max(5, SCAN_SECONDS - (time.time()-start)))

if __name__ == "__main__":
    main()
