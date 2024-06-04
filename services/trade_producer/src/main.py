from quixstreams import Application
from typing import List, Dict

from src.kraken_api import KrakenWebsocketTradeAPI


def produce_trades(
        kafka_broker_address: str,
        kafka_topic_name: str
) -> None:
    """
    Reads trades from the Kraken websocket API and saves them into a Kafka topic.

    Args:
        kafka_broker_address (str): The address of the Kafka broker
        kafka_topic_name (str): The name of the Kafka topic

    Returns: None
    """
    app = Application(broker_address=kafka_broker_address)

    topic = app.topic(name=kafka_topic_name, value_serializer='json')

    kraken_api = KrakenWebsocketTradeAPI(product_id='BTC/USD')

    # Create a Producer instance
    with app.get_producer() as producer:

        while True:

            # Get the trades from the Kraken API
            trades : List[Dict] = kraken_api.get_trades()

            for trade in trades:

                # Serialize an event using the defined Topic 
                message = topic.serialize(key=trade["product_id"], 
                                        value=trade)

                # Produce a message into the Kafka topic
                producer.produce(
                    topic=topic.name, 
                    value=message.value, 
                    key=message.key
                )

                print("Message sent!")
            from time import sleep
            sleep(1)

if __name__ == '__main__':
    produce_trades(
        kafka_broker_address="localhost:19092",
        kafka_topic_name="trade"
    )

