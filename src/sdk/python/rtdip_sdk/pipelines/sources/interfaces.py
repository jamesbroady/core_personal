from abc import abstractmethod
from src.sdk.python.rtdip_sdk.pipelines.interfaces import PipelineBaseInterface

class SourceInterface(PipelineBaseInterface):
    
    @abstractmethod
    def pre_read_validation(self) -> bool:
        pass

    @abstractmethod
    def post_read_validation(self) -> bool:
        pass

    @abstractmethod
    def read_batch(self):
        pass

    @abstractmethod
    def read_stream(self):
        pass