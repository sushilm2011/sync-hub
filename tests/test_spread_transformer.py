import unittest

from plugins.transformers.spread_transformer import SpreadTransformer

class TestSpreadTransformer(unittest.TestCase):
  def setUp(self):
    self.simple_config = {
      "spread_key": "child"
    }
    
    self.config_with_update_key = {
      "spread_key": "child",
      "update_key": "next"
    }
    
    self.config_with_default_value = {
      "spread_key": "child",
      "update_key": "next",
      "default_value": "N/A"
    }
    
  def test_get_without_spread_key(self):
    transformer = SpreadTransformer(config=self.simple_config)
    data_without_spread_key = transformer.get_without_spread_key({
      "hello": "world",
      "child": "I am child"
    })
    
    self.assertDictEqual(data_without_spread_key, {"hello": "world"})
    
  def test_get_without_spread_key_without_key(self):
    transformer = SpreadTransformer(config=self.simple_config)
    data_without_spread_key = transformer.get_without_spread_key({
      "hello": "world",
      "diff": "I am child"
    })
    
    self.assertDictEqual(data_without_spread_key, {
      "hello": "world", 
      "diff": "I am child"
    })
    
  def test_transform_data_without_update_key(self):
    transformer = SpreadTransformer(config=self.simple_config)
    data = {
      "hello": "world",
      "child": ["I am child"]
    }
    
    response = []
    def add_to_response(data):
      response.append(data)
    
    transformer.transform_data(data, add_to_response)
    
    self.assertEqual(response, [{
      "hello": "world",
      "child": "I am child"  
    }])
    
  def test_transform_data_with_update_key(self):
    transformer = SpreadTransformer(config=self.config_with_update_key)
    data = {
      "hello": "world",
      "child": ["I am child"]
    }
    
    response = []
    def add_to_response(data):
      response.append(data)
    
    transformer.transform_data(data, add_to_response)
    
    self.assertEqual(response, [{
      "hello": "world",
      self.config_with_update_key.get("update_key"): "I am child"  
    }])
    
  def test_transform_data_without_default_value(self):
    transformer = SpreadTransformer(config=self.config_with_update_key)
    data = {
      "hello": "world"
    }
    
    response = []
    def add_to_response(data):
      response.append(data)
    
    transformer.transform_data(data, add_to_response)
    
    self.assertEqual(response, [{
      "hello": "world",
      "next": {}  
    }])
    
  def test_transform_data_with_default_value(self):
    transformer = SpreadTransformer(config=self.config_with_default_value)
    data = {
      "hello": "world"
    }
    
    response = []
    def add_to_response(data):
      response.append(data)
    
    transformer.transform_data(data, add_to_response)
    
    self.assertEqual(response, [{
      "hello": "world",
      "next": self.config_with_default_value.get("default_value")
    }])