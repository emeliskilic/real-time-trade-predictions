import json
from typing import Optional
from quixstreams import Application
from loguru import logger
from datetime import datetime, timezone
from src.hopsworks_api import push_data_to_feature_store
from src.config import config


def get_current_timestamp():
    return int(datetime.now(timezone.utc).timestamp())

def kafka_to_feature_store(
        kafka_topic: str,
        kafka_broker_address: str,
        feature_group_name: str,
        feature_group_version: int,
        buffer_size: Optional[int] = 1,
        live_or_historical: Optional[str] = 'live'
) -> None:
    """
    Reads 'ohlc' data from the kafka topic and writes it to the feature store.
    """

    app = Application(
        broker_address=kafka_broker_address,
        consumer_group="kafka_to_feature_store",
        #auto_offset_reset="earliest"
    )

    last_saved_to_feature_store_ts = get_current_timestamp()

    # input_topic = app.topic(name=kafka_topic, value_serializer='json')

    # List of the trades to be written to the feature_store at once
    buffer = []

    with app.get_consumer() as consumer:
        consumer.subscribe(topics=[kafka_topic])

        while True:
            msg = consumer.poll(1)

            if msg is None:
                # no new messages
                n_sec = 30

                logger.debug(f"no new messages in {kafka_topic}")
                if (get_current_timestamp() - last_saved_to_feature_store_ts) > n_sec:
                    logger.debug("Exceeded the time limit. Forcing to push the data to feature store")
                    breakpoint()
                    push_data_to_feature_store(
                        feature_group_name=feature_group_name,
                        feature_group_version=feature_group_version,
                        data=buffer,
                        online_or_offline='online' if live_or_historical == 'live' else 'offline'
                    )
                else:
                    logger.debug("Not exceeded the time limit yet, continue polling data from Kafka")
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
                        online_or_offline='online' if live_or_historical == 'live' else 'offline'
                    )

                # reset the buffer
                buffer = []

                # update the last_saved_to_feature_store_ts
                last_saved_to_feature_store_ts = get_current_timestamp()


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
            buffer_size=config.buffer_size,
            live_or_historical=config.live_or_historical
        )
    except KeyboardInterrupt:
        logger.info('Exiting...')
