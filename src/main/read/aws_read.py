import traceback

from src.main.utility.logging_config import logger
from botocore.exceptions import ClientError


class S3Reader:

    def list_files(self, bucket_name, folder_path,s3_client):
        try:
            response=s3_client.list_objects_v2(Bucket=bucket_name,Prefix=folder_path)
            if response["Contents"]:
               absolute_path=[ f"s3://{bucket_name}/{key['Key']}" for key in response['Contents'] if not key["Key"].endswith('/')]
               return absolute_path
            else:
                return []

        except Exception as e:
            error_message = f"Error listing files: {e}"
            traceback_message = traceback.format_exc()
            logger.error("Got this error : %s", error_message)
            print(traceback_message)
            raise


