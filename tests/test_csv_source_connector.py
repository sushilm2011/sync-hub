import unittest
from unittest.mock import patch, mock_open
from plugins.connectors.csv_source_connector import CSVSourceConnector

CSV_DATA = """id,name,age
1,Alice,30
2,Bob,25
3,Charlie,35"""

EXPECTED_DATA = [{
    "id": "1",
    "name": "Alice",
    "age": "30"
}, {
    "id": "2",
    "name": "Bob",
    "age": "25"
}, {
    "id": "3",
    "name": "Charlie",
    "age": "35"
}]


class TestCSVSourceConnector(unittest.TestCase):

  def setUp(self):
    pass

  @patch("builtins.open", new_callable=mock_open, read_data="")
  def test_fetch_data_empty_csv(self, mock_file):
    connector = CSVSourceConnector("dummy_path.csv")
    result = []

    def callback(row):
      result.append(row)

    connector.execute(callback)
    self.assertEqual(result, [])

  @patch("builtins.open", new_callable=mock_open, read_data=CSV_DATA.split('\n')[0])
  def test_fetch_data_csv_header_only(self, mock_file):
    connector = CSVSourceConnector("dummy_path.csv")
    result = []

    def callback(row):
      result.append(row)

    connector.execute(callback)
    self.assertEqual(result, [])

  @patch("builtins.open",
         new_callable=mock_open,
         read_data=CSV_DATA)
  def test_fetch_data(self, mock_file):
    connector = CSVSourceConnector("dummy_path.csv")
    result = []

    def callback(row):
      result.append(row)

    connector.execute(callback)
    self.assertEqual(result, EXPECTED_DATA)
