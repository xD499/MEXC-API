import asyncio
import websockets
import json
import pandas as pd


async def handle_data(websocket, ohlc_data):
    # Continuous loop to receive the WebSocket stream
    while True:
        response = await websocket.recv()
        data = json.loads(response)

        if 'd' in data and 'k' in data['d']:
            kline_data = data['d']['k']

            # Create a new row with the OHLCV data
            new_data = {
                't': pd.to_datetime(kline_data['T'], unit='s'),
                'o': float(kline_data['o']),
                'h': float(kline_data['h']),
                'l': float(kline_data['l']),
                'c': float(kline_data['c']),
                'v': float(kline_data['v'])
            }

            # Convert the dictionary to a single-row DataFrame
            new_data_df = pd.DataFrame([new_data])

            # Append new row to the existing ohlc_data DataFrame
            ohlc_data = pd.concat([ohlc_data, new_data_df], ignore_index=True)

            # Print the updated DataFrame (you can remove or customize this as needed)
            print(ohlc_data)


async def main():
    uri = 'wss://wbs.mexc.com/ws'
    async with websockets.connect(uri) as websocket:
        print("Connected to WebSocket")

        # Subscription message (sent only once)
        message = {
            "method": "SUBSCRIPTION",
            "params": [
                "spot@public.kline.v3.api@BTCUSDT@Min15"
            ]
        }

        # Send subscription message
        json_message = json.dumps(message)
        await websocket.send(json_message)
        print(f"Sent: {json_message}")

        # Initialize an empty DataFrame to store OHLCV data
        ohlc_data = pd.DataFrame(columns=['t', 'o', 'h', 'l', 'c', 'v'])

        # Start handling the WebSocket stream and updating ohlc_data
        await handle_data(websocket, ohlc_data)

if __name__ == '__main__':
    asyncio.run(main())
