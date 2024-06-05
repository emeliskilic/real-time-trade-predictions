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

class Config(BaseSettings):
    product_id: str = 'ETH/USD'
    kafka_broker_address: str = os.environ['KAFKA_BROKER_ADDRESS']
    kafka_topic_name: str = 'trade'

config = Config()