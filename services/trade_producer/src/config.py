import os
from dotenv import load_dotenv, find_dotenv

# load my .env file variables as environment variables so I can access them
# with os.environ[] statements
dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

# product_id = 'ETH/USD'
# kafka_broker_address = os.environ['KAFKA_BROKER_ADDRESS']
# kafka_topic_name='trade'

from pydantic_settings import BaseSettings

# challenge: assert live historical to be one of those values
# challenge: convert product_id to product_ids
class Config(BaseSettings):
    product_id: str = 'BTC/EUR'
    kafka_broker_address: str = os.environ['KAFKA_BROKER_ADDRESS']
    kafka_topic_name: str = 'trade'
    live_or_historical: str = 'live'
    last_n_days: int = 1

config = Config()