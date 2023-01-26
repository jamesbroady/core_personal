from abc import ABC, abstractmethod
from src.sdk.python.rtdip_sdk.pipelines.utils.models import Libraries, SystemType

class PipelineBaseInterface(ABC):
        
        @property
        @abstractmethod
        def system_type(self) -> SystemType:
            pass
        
        @abstractmethod
        def libraries(self) -> Libraries:
            pass
    
        @abstractmethod
        def settings(self) -> dict:
            pass
