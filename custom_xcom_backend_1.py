from airflow.providers.common.io.xcom.backend import XComObjectStorageBackend
from airflow.models.xcom import BaseXCom
from airflow.utils.session import provide_session
import json

class CustomXComBackend(XComObjectStorageBackend):
    """
    Custom XCom backend that displays actual values in UI even when stored in GCS
    """
    
    @staticmethod
    @provide_session
    def deserialize_value(result, session=None):
        """
        Override deserialization to return actual content for UI display
        """
        if isinstance(result, str) and result.startswith('gs://'):
            # If stored in GCS, fetch and return the actual content
            return XComObjectStorageBackend.deserialize_value(result)
        return BaseXCom.deserialize_value(result)
    
    @staticmethod
    def serialize_value(value):
        """
        Serialize value - same as parent but ensures we can deserialize properly
        """
        if value is None:
            return None
            
        # Let parent handle the actual storage
        return XComObjectStorageBackend.serialize_value(value)
