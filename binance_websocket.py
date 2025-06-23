import websocket
import json
import time as t
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
import os


producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                          value_serializer=lambda x: json.dumps(x).encode('utf-8')
                          )

topic = 'crypto'

SYMBOLS = ['btcusdt', 'ethusdt']
last_send_time = 0
SEND_INTERVAL = 10 

def on_message(ws, message):
    global last_send_time
    data = json.loads(message)
    dt = data['data']
    bids=dt['b']
    asks=dt['a']
    symbol=dt['s']
    ts=dt['T']
    time = datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S')
    msg = {
        "platform": "Binance",
        "symbol": symbol,
        "time": time,
        "bid": float(bids),
        "ask": float(asks)
    }
    now = t.time()
    if now - last_send_time >= SEND_INTERVAL:
        producer.send(topic, value=msg)
        print(msg)
        last_send_time=now


def on_error(ws, error):
    print("Error: ", error)

def on_close(ws, code, reason):
    print(f"WebSocket closed: {code} - {reason}")
    print("Reconnecting in 5 seconds...")
    time.sleep(5)
    start_websocket()

def on_open(ws):
    streams = [f"{symbol}@bookTicker" for symbol in SYMBOLS]
    params = {
        "method": "SUBSCRIBE",
        "params": streams,
        "id": 1
    }
    ws.send(json.dumps(params))
    print("Subscribed to: ", streams)

def start_websocket():
    ws = websocket.WebSocketApp(
            'wss://fstream.binance.com/stream',
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
    )                
    ws.run_forever(ping_interval=15, ping_timeout=10)

start_websocket()    