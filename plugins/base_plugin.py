from typing import Any, Callable


class BasePlugin:

  def execute(self, data):
    raise NotImplementedError("Plugins must implement the execute method")


class BaseConnector(BasePlugin):

  def fetch_data(self, callback):
    raise NotImplementedError(
        "Connectors must implement the fetch_data method")

  def execute(self, data):
    self.fetch_data(data)


class BaseTransformer(BasePlugin):

  def transform_data(self, data, callback):
    raise NotImplementedError(
        "Transformers must implement the transform_data method")

  def execute(self, data, callback):
    self.transform_data(data, callback)


class BaseSyncer(BasePlugin):

  def sync_data(self, data):
    raise NotImplementedError("Syncers must implement the sync_data method")

  def execute(self, data):
    self.sync_data(data)
