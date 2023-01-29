from abc import abstractmethod
from src.sdk.python.rtdip_sdk.pipelines.interfaces import PipelineComponentBaseInterface

class DestinationInterface(PipelineComponentBaseInterface):

    @abstractmethod
    def destination_definition(self) -> dict:
        pass

    @abstractmethod
    def pre_write_validation(self) -> bool:
        pass

    @abstractmethod
    def post_write_validation(self) -> bool:
        pass

    @abstractmethod
    def write_batch(self):
        pass

    @abstractmethod
    def write_stream(self):
        pass