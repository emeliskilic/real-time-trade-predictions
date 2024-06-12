import os
from dotenv import load_dotenv, find_dotenv

# load my .env file variables as environment variables so I can access them
# with os.environ[] statements
dotenv_path = find_dotenv()
load_dotenv(dotenv_path)


from pydantic_settings import BaseSettings

class Config(BaseSettings):
    kafka_broker_address: str = os.environ['KAFKA_BROKER_ADDRESS']
    kafka_input_topic: str = 'trade'
    kafka_output_topic: str = 'ohlc'
    ohlc_window_seconds: int = os.environ['OHLC_WINDOW_SECONDS']

config = Config()
