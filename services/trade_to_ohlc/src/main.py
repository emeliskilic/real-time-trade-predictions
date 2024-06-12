from datetime import timedelta

from loguru import logger


def trade_to_ohlc(
        kafka_input_topic: str,
        kafka_output_topic: str,
        kafka_broker_address: str,
        ohlc_window_seconds: int,
) -> None:
    """
    Reads trades from the kafka input topic
    Aggregates them into OHLC candles using the specified window in ohlc_window_seconds
    Saves the ohlc data into another kafka topic

    Args:
        kafka_input_topic : str : Kafka topic to read trade data from
        kafka_output_topic : str : Kafka topic to write ohlc data to
        kafka_broker_address : str : Kafka broker address
        ohlc_window_seconds : int : Window size in seconds for OHLC aggregation
    
    
    Returns: 
        None
    """

    from quixstreams import Application

    # This handles all low-level communication with Kafka
    app = Application(
        broker_address=kafka_broker_address,
        consumer_group="trade_to_ohlc",
        auto_offset_reset="earliest"
    )

    # Specify input and output topic for this application
    input_topic = app.topic(name=kafka_input_topic, value_serializer='json')
    output_topic = app.topic(name=kafka_output_topic, value_serializer='json')

    # Creating a streaming dataframe to apply transformation on the incoming data - start
    sdf = app.dataframe(topic=input_topic)

    def init_ohlc_candle(value: dict) -> dict:
        """
        Initialize the OHLC candle with the first trade
        """
        return {
            "open": value["price"],
            "high": value["price"],
            "low": value["price"],
            "close": value["price"],
            "product_id": value["product_id"],
        }

    def update_ohlc_candle(ohlc_candle: dict, trade: dict) -> dict:
        """
        Update the OHLC candle with the new trade and return the updated candle

        Args:
            ohlc_candle : dict : The current OHLC candle
            trade : dict : The incoming trade

        Returns:
            dict : The updated OHLC candle
        """
        return {
            "open": ohlc_candle["open"],
            "high": max(ohlc_candle["high"], trade['price']),
            "low": min(ohlc_candle["low"], trade['price']),
            "close": trade["price"],
            "product_id": ohlc_candle["product_id"]
        }

    # Apply transformation to the incoming data
    sdf = sdf.tumbling_window(duration_ms=timedelta(seconds=ohlc_window_seconds))
    sdf = sdf.reduce(reducer=update_ohlc_candle, initializer=init_ohlc_candle).current()

    # extraqct the open, high, low, close prices from the value key
    # The current format is the following: 
    # {'start': 1718137540000, 'end': 1718137550000, 'value': {'open': 3490.23, 'high': 3490.23, 'low': 3490.23, 'close': 3490.23, 'product_id': 'ETH/USD'}}
    sdf['open'] = sdf["value"]["open"]
    sdf['high'] = sdf["value"]["high"]
    sdf['low'] = sdf["value"]["low"]
    sdf['close'] = sdf["value"]["close"]
    sdf["product_id"] = sdf["value"]["product_id"]
    sdf["timestamp"] = sdf["end"]

    # let's keep only the keys we want
    sdf = sdf[["timestamp", "open", "high", "low", "close", "product_id"]]

    # Apply transformation on the incoming data - end
    sdf = sdf.update(logger.info)

    sdf = sdf.to_topic(output_topic)

    # kick-off the streaming application
    app.run(sdf)

if __name__=='__main__':
    from src.config import config

    trade_to_ohlc(
        kafka_input_topic=config.kafka_input_topic,
        kafka_output_topic=config.kafka_output_topic,
        kafka_broker_address=config.kafka_broker_address,
        ohlc_window_seconds=config.ohlc_window_seconds,
    )