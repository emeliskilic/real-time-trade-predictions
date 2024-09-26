import json
from quixstreams import Application
from loguru import logger
from src.hopsworks_api import push_data_to_feature_store
from src.config import config


def kafka_to_feature_store(
        kafka_topic: str,
        kafka_broker_address: str,
        feature_group_name: str,
        feature_group_version: int,
        buffer_size: int
) -> None:
    """
    Reads 'ohlc' data from the kafka topic and writes it to the feature store.
    """

    app = Application(
        broker_address=kafka_broker_address,
        consumer_group="kafka_to_feature_store",
        #auto_offset_reset="earliest"
    )

    # input_topic = app.topic(name=kafka_topic, value_serializer='json')

    # List of the trades to be written to the feature_store at once
    buffer = []

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
                buffer.append(ohlc)

                if len(buffer) >= buffer_size:
                    push_data_to_feature_store(
                        feature_group_name=feature_group_name,
                        feature_group_version=feature_group_version,
                        data=buffer,
                    )

                # reset the buffer
                buffer = []

                # breakpoint()

            # Store the offset of the processed message on the consumer for the auto-commit mechanism
            # It will send it to Kafka in the background
            consumer.store_offsets(message=msg)


if __name__=='__main__':

    try:
        kafka_to_feature_store(
            kafka_topic=config.kafka_topic,
            kafka_broker_address=config.kafka_broker_address,
            feature_group_name=config.feature_group_name,
            feature_group_version=config.feature_group_version,
            buffer_size=config.buffer_size
        )
    except KeyboardInterrupt:
        logger.info('Exiting...')
