from typing import List, Dict, Tuple
import requests
import json
from loguru import logger
from time import sleep

class KrakenRestAPIMultipleProducts:
    def __init__(
        self,
        product_ids: List[str],
        last_n_days: int
            ) -> None:
        self.product_ids = product_ids
        self.kraken_apis = [KrakenRestAPI(product_id=product_id, last_n_days=last_n_days) for product_id in product_ids]

    def get_trades(self) -> List[Dict]:
        trades = []
        for kraken_api in self.kraken_apis:
            if kraken_api.is_done():
                continue
            else:
                trades += kraken_api.get_trades()
        return trades
    
    def is_done(self) -> bool:
        for kraken_api in self.kraken_apis:
            if not kraken_api.is_done():
                return False
        return True


class KrakenRestAPI:

    URL = 'https://api.kraken.com/0/public/Trades?pair={product_id}&since={since_sec}'
    
    def __init__(
            self,
            product_id: List[str],
            last_n_days: int
                ) -> None:
        self.product_id = product_id
        self.from_ms, self.to_ms = self._init_from_to_ms(last_n_days)
        self.last_trade_ms = self.from_ms
        self._is_done = False

    @staticmethod
    def _init_from_to_ms(last_n_days: int) -> Tuple[int,int]:
        from datetime import datetime, timezone
        today_date = datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0)
        to_ms = int(today_date.timestamp() * 1000)
        from_ms = to_ms - last_n_days * 24 * 60 * 60 * 1000
        return from_ms, to_ms

    def get_trades(self) -> List[Dict]:

        payload={}
        headers = {
        'Accept': 'application/json'
        }
        since_sec = self.last_trade_ms//1000
        url = self.URL.format(product_id=self.product_id, since_sec=since_sec)
        response = requests.request("GET", url, headers=headers, data=payload)
        
        # print(response.text)
        data = json.loads(response.text)

        # challenge: check if there is an error in the response
        if ('error' in data) and ('EGeneral:Too many requests' in data['error']):
            logger.info('too many requests, waiting for 30 seconds')
            sleep(30)

        trades = [
            {
                'price': float(trade[0]),
                'volume': float(trade[1]),
                'time': int(trade[2]),
                'product_id': self.product_id
            } 
            for trade in data['result'][self.product_id]
        ] 

        # filter out the trades that are beyond the to_ms
        trades = [trade for trade in trades if trade['time'] <= self.to_ms//1000]

        # check if we are done fetching historical data
        last_ts_in_ns = int(data['result']['last'])
        self.last_trade_ms = last_ts_in_ns // 1_000_000
        if self.last_trade_ms >= self.to_ms:
            self._is_done = True

        logger.debug('len trades: ' + str(len(trades)))
        logger.debug('last ts: ' + str(self.last_trade_ms))

        sleep(1)
        return trades

    def is_done(self) -> bool:
        return self._is_done
