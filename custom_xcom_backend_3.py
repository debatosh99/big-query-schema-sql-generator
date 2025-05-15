from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.models.xcom import BaseXCom

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class CustomXCom(BaseXCom):
    """
    Custom XCom backend to deserialize values directly.
    """

    @staticmethod
    def serialize_value(value: Any, **kwargs: Any) -> Any:
        return BaseXCom.serialize_value(value, **kwargs)

    @staticmethod
    def deserialize_value(result: Any, **kwargs: Any) -> Any:
        return BaseXCom.deserialize_value(result, **kwargs)

    @staticmethod
    def orm_deserialize_value(result: Any, session: Session) -> Any:
        """
        Override this method to return the actual value instead of the GCS path.
        """
        if result is None:
            return None
        if isinstance(result, str) and result.startswith("gs://"):
            from google.cloud import storage

            storage_client = storage.Client()
            bucket_name = result.split("/")[2]
            blob_name = "/".join(result.split("/")[3:])
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            try:
                # Download the content and attempt to deserialize
                content = blob.download_as_text()
                deserialized_content = BaseXCom.deserialize_value(content)
                return deserialized_content
            except Exception as e:
                warnings.warn(
                    f"Failed to download and deserialize XCom value from GCS. Returning raw value. Error: {e}",
                    UserWarning,
                )
                return result  # Return the GCS path on failure
        else:
            # If it's not a GCS path, assume it's already deserialized
            return BaseXCom.deserialize_value(result)
