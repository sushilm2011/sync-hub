import csv
from typing import Callable, Any
import logging
from plugins.base_plugin import BaseConnector

logger = logging.getLogger(__name__)


class CSVSourceConnector(BaseConnector):

  def __init__(self, file_path: str):
    """
    Initialize Csv Source Connector with the file path
    
    Args:
      file_path (str): Csv file path
    """
    self.file_path = file_path

  def fetch_data(self, callback: Callable[[Any], None]):
    """
    Fetch data from the csv file and apply the callback function to each item.

    Args:
      callback (Callable[[Any], None]): A function to process each item of the data.
    """

    try:
      with open(self.file_path, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
          callback(row)
    except FileNotFoundError:
      logger.error(f"File not found: {self.file_path}")
    except Exception as e:
      logger.error(f"Error reading CSV file: {e}")
