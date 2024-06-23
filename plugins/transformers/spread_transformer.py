from typing import Callable, Dict, Any
from plugins.base_plugin import BaseTransformer


class SpreadTransformer(BaseTransformer):

  def __init__(self, config: Dict[str, Any]) -> None:
    """
    Initialize Spread Transformer with config
    
    Args:
      config (Dict[str, Any]): Configuration dictionary containing spread_key, update_key (Update key with which the spreak key should be updated), default_value (Optional)
    """
    self.config = config.copy()
    self.spread_key = config.get('spread_key')
    self.update_key = config.get('update_key', self.spread_key)
    self.default_value = config.get('default_value', {})

  def get_without_spread_key(self, data: Dict[str, Any]):
    """
    Create a copy of data without the configured spread_key
    
    Args:
      Dict[str, Any]: data from which configured spread_key needs to be removed
    
    Returns:
      Dict[str, Any]: data without spread_key as key
    """
    transformed_data = {}
    transformed_data.update({
        k: v
        for k, v in data.items() if k != self.spread_key
    })
    return transformed_data

  def transform_data(self, data: Dict[str, Any], callback: Callable[[Any],
                                                                    None]):
    """
    Transforms the data passed, first it finds the configured spread_key
    Then for each value in the data[spread_key] it calls the callback for each value with a data clone without spread_key but updated value
    
    Example: {"name": "Hello", "child": ["World1", "World2"]} => calls callback 2 times if spread_key is "child"
    Callback1: {"name": "Hello", "child": "World1"}
    Callback2: {"name": "Hello", "child": "World2"}
    
    Args:
      data (Dict[str, Any]): Data to be transfomed
      callback (Callable[[Any], None]): Callback to be called with each transformed value
    """
    spread_items = data.get(self.spread_key, [])
    if len(spread_items) == 0:
      transformed_data = self.get_without_spread_key(data)
      transformed_data.update({self.update_key: self.default_value})
      callback(transformed_data)
    else:
      for item in spread_items:
        transformed_data = self.get_without_spread_key(data)
        transformed_data.update({self.update_key: item})
        callback(transformed_data)
