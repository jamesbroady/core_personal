from typing import Dict
from unittest.mock import MagicMock

class SparkContextMock():

    _jvm = MagicMock()
    _jvm.org.apache.spark.eventhubs.EventHubsUtils = MagicMock()

    def __init__(self):
        self._jvm.org.apache.spark.eventhubs.EventHubsUtils = MagicMock(return_value=None)

        self._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt = self._encrypt

    def _encrypt(self, eventhub_connection_string):
        return "encrypted;" + eventhub_connection_string

