from airflow.providers.common.io.xcom.backend import XComObjectStorageBackend
from airflow.models.xcom import BaseXCom
from airflow.utils.session import provide_session

class ReadableStorageXComBackend(XComObjectStorageBackend):
    """
    XCom backend that stores large values in GCS but shows content in UI
    """
    
    @staticmethod
    @provide_session
    def get_value(xcom, session=None):
        """
        Override to return actual content when viewed in UI
        """
        result = xcom.value
        if isinstance(result, str) and result.startswith('gs://'):
            # Fetch from GCS
            content = XComObjectStorageBackend.deserialize_value(result)
            # Return in a format that's readable in UI
            try:
                # Try to pretty-print if it's JSON
                return json.loads(content)
            except:
                return content
        return BaseXCom.deserialize_value(result)
    
    @staticmethod
    def serialize_value(value):
        """Handle serialization (storage) the same way as parent"""
        return XComObjectStorageBackend.serialize_value(value)
