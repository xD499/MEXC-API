import asyncio
import websockets
import json
import pandas as pd


async def handle_data(websocket, ohlc_data):
    while True:
        response = await websocket.recv()
        data = json.loads(response)

        if 'd' in data and 'k' in data['d']:
            kline_data = data['d']['k']

            timestamp = kline_data['T']
            ohlc = {
                'timestamp': timestamp,
                'open': kline_data['o'],
                'high': kline_data['h'],
                'low': kline_data['l'],
                'close': kline_data['c']
            }

            ohlc_data.append(ohlc)
            df = pd.DataFrame(ohlc_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')


async def main():
    uri = 'wss://wbs.mexc.com/ws'
    async with websockets.connect(uri) as websocket:
        print("Connected to WebSocket")

        message = {
            "method": "SUBSCRIPTION",
            "params": [
                "spot@public.kline.v3.api@BTCUSDT@Min15"
            ]
        }

        json_message = json.dumps(message)

        await websocket.send(json_message)
        print(f"Sent: {json_message}")

        ohlc_data = []

        await handle_data(websocket, ohlc_data)

if __name__ == '__main__':
    asyncio.run(main())
