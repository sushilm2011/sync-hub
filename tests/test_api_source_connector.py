import unittest
from unittest.mock import patch, Mock
import requests

from plugins.connectors.api_source_connector import ApiSourceConnector


class TestApiSourceConnector(unittest.TestCase):

  def setUp(self):
    self.config_get = {
        'url': 'https://mock-api.com/data',
        'headers': {
            'Authorization': 'Bearer abc'
        },
        'params': {
            'limit': 10
        }
    }

    self.config_post = {
        'url': 'https://mock-api.com/data',
        'headers': {
            'Authorization': 'Bearer abc'
        },
        'method': 'POST',
        'data': {
            'id': 1
        }
    }

    self.config_invalid_method = {
        'url': 'https://mock-api.com/data',
        'headers': {
            'Authorization': 'Bearer abc'
        },
        'method': 'PUT',
        'data': {
            'id': 1
        }
    }

  @patch('requests.get')
  def test_get_response(self, mock_get):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'key': 'value'}
    mock_get.return_value = mock_response

    connector = ApiSourceConnector(self.config_get)
    response = connector.get_response()

    mock_get.assert_called_once_with(self.config_get.get('url'),
                                     headers=self.config_get.get('headers'),
                                     params=self.config_get.get('params'))
    self.assertEqual(response.status_code, 200)
    self.assertEqual(response.json(), {'key': 'value'})

  @patch('requests.post')
  def test_get_response(self, mock_post):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'key': 'value'}
    mock_post.return_value = mock_response

    connector = ApiSourceConnector(self.config_post)
    response = connector.get_response()

    mock_post.assert_called_once_with(
        self.config_get.get('url'),
        headers=self.config_post.get('headers', {}),
        params=self.config_post.get('params', {}),
        data=self.config_post.get('data'))
    self.assertEqual(response.status_code, 200)
    self.assertEqual(response.json(), {'key': 'value'})

  def test_invalid_method(self):
    connector = ApiSourceConnector(self.config_invalid_method)
    with self.assertRaises(NotImplementedError):
      connector.get_response()

  @patch('requests.get')
  def test_fetch_data(self, mock_get):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{'item': 'value'}]
    mock_get.return_value = mock_response

    def mock_callback(item):
      self.assertEqual(item, {'item': 'value'})

    connector = ApiSourceConnector(self.config_get)
    connector.fetch_data(mock_callback)

  @patch('requests.get', side_effect=requests.exceptions.RequestException)
  def test_get_response_exception(self, mock_get):
    connector = ApiSourceConnector(self.config_get)
    with self.assertRaises(requests.exceptions.RequestException):
      connector.get_response()
