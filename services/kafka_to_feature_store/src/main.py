import json
from quixstreams import Application
from loguru import logger
from src.hopsworks_api import push_data_to_feature_store


def kafka_to_feature_store(
        kafka_topic: str,
        kafka_broker_address: str,
        feature_group_name: str,
        feature_group_version: int
) -> None:
    """
    Reads 'ohlc' data from the kafka topic and writes it to the feature store.
    """

    app = Application(
        broker_address=kafka_broker_address,
        consumer_group="kafka_to_feature_store"
    )

    # input_topic = app.topic(name=kafka_topic, value_serializer='json')

    with app.get_consumer() as consumer:
        consumer.subscribe(topics=[kafka_topic])

        while True:
            msg = consumer.poll(1)

            if msg is None:
                continue
            elif msg.error():
                logger.error('Kafka error: ', msg.error())
                continue
            else:
                logger.info(msg.value())
                ohlc = json.loads(msg.value().decode('utf-8'))
                push_data_to_feature_store(
                    feature_group_name=feature_group_name,
                    feature_group_version=feature_group_version,
                    data=ohlc,
                )

                # breakpoint()

            # Store the offset of the processed message on the consumer for the auto-commit mechanism
            # It will send it to Kafka in the background
            consumer.store_offsets(message=msg)


if __name__=='__main__':

    # Challenge: Load these values from the config.py file
    kafka_to_feature_store(
        kafka_topic='ohlc',
        kafka_broker_address='localhost:19092',
        feature_group_name='ohlc_feature_group',
        feature_group_version=1
    )