import os
from dotenv import load_dotenv, find_dotenv
from pydantic_settings import BaseSettings

# load my .env file variables as environment variables so I can access them
# with os.environ[] statements
dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

class Config(BaseSettings):
    hopsworks_project_name: str = os.environ['HOPSWORKS_PROJECT_NAME']
    hopsworks_api_key: str = os.environ['HOPSWORKS_API_KEY']
    kafka_broker_address: str = os.environ['KAFKA_BROKER_ADDRESS']
    kafka_topic: str = os.environ['KAFKA_TOPIC']
    feature_group_name: str = os.environ['FEATURE_GROUP_NAME']
    feature_group_version: int = os.environ['FEATURE_GROUP_VERSION']
    buffer_size: int = os.environ['BUFFER_SIZE']
    live_or_historical: str = 'live' # by default

config = Config()