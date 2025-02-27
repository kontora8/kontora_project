import logging
from typing import List

import pydantic_core
import requests

from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.store_gateway import StoreGateway


class StoreApiAdapter(StoreGateway):
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url

    def save_data(self, processed_agent_data_batch: List[ProcessedAgentData]) -> bool:
        """
        Save the processed road data to the Store API.
        Parameters:
            processed_agent_data_batch (List[ProcessedAgentData]): Processed road data to be saved.
        Returns:
            bool: True if the data is successfully saved, False otherwise.
        """
        try:
            data_to_send = []
            start_id = 0
            for i, item in enumerate(processed_agent_data_batch):
                item_dict = item.model_dump(mode='json')
                if "agent_data" in item_dict and isinstance(item_dict["agent_data"], dict):
                    item_dict["agent_data"]["user_id"] = start_id + 1
                data_to_send.append(item_dict)

            response = requests.post(f"{self.api_base_url}/processed_agent_data/", json=data_to_send)

            if response.status_code >= 200 and response.status_code < 300:
                logging.info(f"Successfully sent {len(processed_agent_data_batch)} records to Store API")
                return True
            else:
                logging.error(f"Failed to save data to Store API. Status code: {response.status_code}, Response: {response.text}")
                return False
        except Exception as e:
            logging.error(f"Error saving data to Store API: {str(e)}")
            return False
