from abc import ABC, abstractmethod

from src.sdk.python.rtdip_sdk.pipelines.utils.models import Libraries, SystemType

class SourceInterface(ABC):
    
    @property
    @abstractmethod
    def system_type(self):
        pass
    
    @abstractmethod
    def libraries(self) -> Libraries:
        pass

    @property
    @abstractmethod
    def settings(self) -> dict:
        pass

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