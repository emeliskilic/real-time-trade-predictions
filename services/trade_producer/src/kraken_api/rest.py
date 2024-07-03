from typing import List, Dict
import requests
import json
from loguru import logger

class KrakenRestAPI:

    URL = 'https://api.kraken.com/0/public/Trades?pair={product_id}&since={since_sec}'
    
    def __init__(
            self,
            product_id: List[str],
            from_ms: int,
            to_ms: int,
                ) -> None:
        self.product_id = product_id
        self.from_ms = from_ms
        self.to_ms = to_ms
        self._is_done = False

    def get_trades(self) -> List[Dict]:

        payload={}
        headers = {
        'Accept': 'application/json'
        }
        since_sec = self.from_ms//1000
        url = self.URL.format(product_id=self.product_id, since_sec=since_sec)
        response = requests.request("GET", url, headers=headers, data=payload)
        
        # print(response.text)
        data = json.loads(response.text)

        # challenge: check if there is an error in the response
        #if data['error'] is not None:
        #    raise Exception(data['error'])

        trades = []
        for trade in data['result'][self.product_id]:
            trades.append({
                'price': float(trade[0]),
                'volume': float(trade[1]),
                'time': int(trade[2]),
                'product_id': self.product_id
            })

        last_ts_in_ns = int(data['result']['last'])
        last_ts = last_ts_in_ns // 1_000_000
        if last_ts >= self.to_ms:
            self._is_done = True

        logger.debug('len trades: ' + str(len(trades)))
        logger.debug('last ts: ' + str(last_ts))

        return trades

    def is_done(self) -> bool:
        return self._is_done
