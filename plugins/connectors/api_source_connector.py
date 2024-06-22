import requests
from typing import Dict, Any, Callable
import logging
from plugins.base_plugin import BaseConnector

logger = logging.getLogger(__name__)

class ApiSourceConnector(BaseConnector):

  def __init__(self, config: Dict[str, Any]):
    """
    Initialize Api Source Connector with configured parameters
    
    Args:
      config (Dict[str, Any]): Configuration dictionary containing url, headers, params, item_key, method and data (if method supports)
    """

    self.config = config
    self.url = config.get('url')
    self.item_key = config.get('item_key')
    self.headers = config.get('headers', {})
    self.params = config.get('params', {})
    self.data = config.get('data', {})
    self.method = config.get('method', 'GET')

    if not self.url:
      raise ValueError("URL is required in the configuration")

  def get_response(self) -> requests.Response:
    """
    Make the HTTP request based on the configured arguments
    
    Returns:
      requests.Response: The response object from the HTTP request
    """

    try:
      if self.method == "GET":
        response = requests.get(self.url,
                                headers=self.headers,
                                params=self.params)
      elif self.method == "POST":
        response = requests.post(self.url,
                                 headers=self.headers,
                                 params=self.params,
                                 data=self.data)
      else:
        raise NotImplementedError(
            f"HTTP method {self.method} is not supported")

      response.raise_for_status()
      return response
    except requests.RequestException as e:
      logger.error(f"Request failed with error {e}")
      raise

  def fetch_data(self, callback: Callable[[Any], None]):
    """
    Fetch data from the API and apply the callback function to each item.

    Args:
      callback (Callable[[Any], None]): A function to process each item of the data.
    """

    try:
      response = self.get_response()
      response_data = response.json()

      data = response_data if self.item_key is None else response_data.get(
          self.item_key, [])

      for item in data:
        callback(item)
    except Exception as e:
      logger.error(f"Error fetching the data: {e}")
