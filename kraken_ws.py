import websocket
import json
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
import time
producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                         value_serializer = lambda x: json.dumps(x).encode('utf-8')
                         )

topic = 'crypto'

SYMBOLS = ["XBT/USD", "ETH/USD"]
last_send_time = 0
SEND_INTERVAL = 20 

def on_message(ws, message):
    global last_send_time
    data = json.loads(message)
    #print(data)


    if isinstance(data, list):
        dt=data[1]
        ask=dt['a'][0]
        bid=dt['b'][0]
        msg = {
            "platform": "Kraken",
            "symbol": data[3],
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "bid": float(bid),
            "ask": float(ask) 
        }
        now = time.time()
        if now - last_send_time >= SEND_INTERVAL:            
            producer.send(topic, value=msg)
            print(msg)
            last_send_time = now

        '''pair = data[3]
        trades=data[1]
        for trade in trades:
            price = float(trade[0]) 
            ts = float(trade[2])
            time = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            
            msg = f"{pair}  | price: {price}  | time: {time}"
            producer.send(topic, value={
                    "symbol": pair,
                    "price": price,
                    "time":time
            })
            print(f"Sent to kafka{msg}")'''

def on_error(ws, error):
    print(f"error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")

def on_open(ws):
    print("Connected to kraken websocket")
    subscribe_message = {
        'event': 'subscribe',
        'pair':SYMBOLS,
        'subscription': {
            'name':'ticker'
            }
    }

    ws.send(json.dumps(subscribe_message))
    print(f"Subscribed to book-1 for: {SYMBOLS}")

ws = websocket.WebSocketApp(
    "wss://ws.kraken.com",
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)
ws.run_forever()               