import os
from dotenv import load_dotenv, find_dotenv
from pydantic_settings import BaseSettings

# load my .env file variables as environment variables so I can access them
# with os.environ[] statements
dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

class Config(BaseSettings):
    # Challenge: Add the other ones here

    hopsworks_project_name: str = os.environ['HOPSWORKS_PROJECT_NAME']
    hopsworks_api_key: str = os.environ['HOPSWORKS_API_KEY']

config = Config()