from src.main.upload.upload_to_S3 import *
from src.main.utility.s3_client_object import *
from resources.dev import config
from src.main.utility.encrypt_decrypt import encrypt,decrypt

s3_client_provider=S3ClientProvider(decrypt(config.aws_access_key),decrypt(config.aws_secret_key))
s3_client=s3_client_provider.get_s3_client()

# ,s3_directory,s3_bucket,local_file_path
up=UploadToS3(s3_client)
up.upload_to_s3(config.s3_source_directory,config.bucket_name,config.sale_data_local_directory)