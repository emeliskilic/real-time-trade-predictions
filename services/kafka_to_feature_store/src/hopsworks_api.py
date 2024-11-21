import hopsworks
import pandas as pd
from typing import List
from loguru import logger

from src.config import config

def push_data_to_feature_store(
        feature_group_name: str,
        feature_group_version: str,
        data: List[dict],
        online_or_offline: str
) -> None:
    """
    Pushes given data to the feature store
    """
    # authenticate with Hopsworks API
    project = hopsworks.login(
        project="emeliskilic",
        api_key_value=config.hopsworks_api_key
    )


    # Get the feature store
    feature_store = project.get_feature_store()

    # Get or create the feature group
    ohlc_feature_group = feature_store.get_or_create_feature_group(
        name=feature_group_name,
        version=feature_group_version,
        description="OHLC data coming from Kraken",
        primary_key=["product_id", "timestamp"],
        event_time="timestamp",
        online_enabled=True,
    )

    # breakpoint()

    data = pd.DataFrame(data)

    breakpoint()
    
    # Write the data to the feature group
    ohlc_feature_group.insert(data, 
                              write_options={
                                  "start_offline_materialization": True if online_or_offline == 'offline' else False})
