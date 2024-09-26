from typing import Dict, List
import time
from loguru import logger
from quixstreams import Application

from src.config import config
from src.kraken_api.websocket import KrakenWebsocketTradeAPI
from src.kraken_api.rest import KrakenRestAPI

def produce_trades(
        kafka_broker_address: str, 
        kafka_topic_name: str, 
        product_id: str,
        live_or_historical: str,
        last_n_days: int) -> None:
    """
    Reads trades from the Kraken websocket API and saves them into a Kafka topic.

    Args:
        kafka_broker_address (str): The address of the Kafka broker
        kafka_topic_name (str): The name of the Kafka topic
        product_id (str): The product ID for which we want to get the trades.

    Returns: None
    """
    assert live_or_historical in {'live', 'historical'}
    app = Application(broker_address=kafka_broker_address)

    topic = app.topic(name=kafka_topic_name, value_serializer='json')

    logger.info(f'Creating the Kraken API to fetch data for {product_id}')

    if live_or_historical == 'live':
        kraken_api = KrakenWebsocketTradeAPI(product_id=product_id)
    else:
        kraken_api = KrakenRestAPI(product_id=product_id, last_n_days=last_n_days)

    logger.info('Creating the producer...')

    # Create a Producer instance
    with app.get_producer() as producer:
        while True:
            # check if we are done getting historical data
            if kraken_api.is_done():
                logger.info('Done fetching historical data')
                break

            # Get the trades from the Kraken API
            trades: List[Dict] = kraken_api.get_trades()

            for trade in trades:
                # Serialize an event using the defined Topic
                message = topic.serialize(key=trade['product_id'], value=trade)

                # Produce a message into the Kafka topic
                producer.produce(topic=topic.name, value=message.value, key=message.key)

                logger.info(trade)

            from time import sleep

            sleep(1)


if __name__ == '__main__':
    produce_trades(
        kafka_broker_address=config.kafka_broker_address, 
        kafka_topic_name=config.kafka_topic_name,
        product_id=config.product_id,
        live_or_historical=config.live_or_historical,
        last_n_days=config.last_n_days
        )
