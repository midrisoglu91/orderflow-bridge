import os
import json
import asyncio
from datetime import datetime, timezone
import aiohttp
import websockets
from fastapi import FastAPI

# ── إعدادات من البيئة ──
SYMBOL = os.getenv("SYMBOL", "btcusdt").lower()
INTERVAL_SEC = int(os.getenv("INTERVAL_SEC", "60"))
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

app = FastAPI(title="OrderFlow Bridge", version="1.0")
state = {"cvd": 0.0}

def ts_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

async def send_telegram(text: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(url, json=payload, timeout=10)
    except Exception as e:
        print("Telegram error:", e)

@app.get("/")
async def root():
    return {"status": "running", "symbol": SYMBOL}

@app.get("/test")
async def test_message():
    text = "✅ Test message from OrderFlow Bridge Bot!"
    await send_telegram(text)
    return {"status": "sent", "text": text}

async def run_collector():
    url = f"wss://fstream.binance.com/ws/{SYMBOL}@aggTrade"
    print("Connecting:", url)
    last_bar_start = None
    bar_delta = 0.0
    bar_vol = 0.0
    bar_open = None
    bar_close = None
    async for ws in websockets.connect(url):
        try:
            async for msg in ws:
                data = json.loads(msg)
                price = float(data["p"])
                qty = float(data["q"])
                mflag = data["m"]
                ts_event = data["E"] / 1000.0
                current_bar_start = int(ts_event // INTERVAL_SEC) * INTERVAL_SEC
                if last_bar_start is None:
                    last_bar_start = current_bar_start
                    bar_open = price
                    bar_close = price
                    bar_delta = 0.0
                    bar_vol = 0.0
                if current_bar_start != last_bar_start:
                    state["cvd"] += bar_delta
                    text = (f"📊 {SYMBOL.upper()} | Δ={bar_delta:.2f}, Vol={bar_vol:.2f}, "
                            f"CVD={state['cvd']:.2f}\nTime={ts_iso(last_bar_start + INTERVAL_SEC)}")
                    await send_telegram(text)
                    last_bar_start = current_bar_start
                    bar_open = price
                    bar_close = price
                    bar_delta = 0.0
                    bar_vol = 0.0
                bar_close = price
                bar_vol += qty
                bar_delta += (-qty if mflag else qty)
        except Exception as e:
            print("WS error:", e)
            await asyncio.sleep(3)

@app.on_event("startup")
async def startup():
    asyncio.create_task(run_collector())
