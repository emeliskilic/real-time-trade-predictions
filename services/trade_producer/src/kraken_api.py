from typing import List, Dict
import json

from websocket import create_connection

class KrakenWebsocketTradeAPI:

    URL = 'wss://ws.kraken.com/v2'

    def __init__(
            self,
            product_id: str
    ):
        self.product_id = product_id

        # establish connection
        self._ws = create_connection(self.URL)
        print("Connection established")

        # subscribe to the trades for the given product_id
        self._subscribe(product_id)

    def _subscribe(self, product_id: str):
        print(f"Subcribing to trades for {product_id}")
        msg = {
                "method": "subscribe",
                "params": {
                    "channel": "trade",
                    "symbol": [
                        product_id,
                    ],
                "snapshot": False
            }
        }
        self._ws.send(json.dumps(msg))
        print("Subscription worked!")

        # Discarding the first two messages as they are not informative
        _ = self._ws.recv()
        _ = self._ws.recv()


    def get_trades(self) -> List[Dict]:
        
        # mock_trades = [
        #     {
        #         'product_id': 'BTC-USD',
        #         'price': 60000,
        #         'volume': 0.01,
        #         'timestamp': 1630000000
        #     },
        #     {
        #         'product_id': 'BTC-USD',
        #         'price': 59000,
        #         'volume': 0.01,
        #         'timestamp': 1640000000
        #     },
        # ]

        message = self._ws.recv()

        if 'heartbeat' in message:
            # when we get a heartbeat, we return an empty list
            return []
        
        # Parse the message string as a dictionary
        message = json.loads(message)

        # extract trades from the message['data']
        trades = []
        for trade in message['data']:
            trades.append({
                'product_id': self.product_id,
                'price': trade['price'],
                'volume': trade['qty'],
                'timestamp': trade['timestamp'],
            })

        # breakpoint()
        return trades