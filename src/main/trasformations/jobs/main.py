import time
from src.main.download.aws_file_download import *
from src.main.read.aws_read import S3Reader
from src.main.utility.logging_config import logger
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.spark_session import *
import os,sys
from src.main.utility import encrypt_decrypt
from resources.dev import config
from src.main.utility.spark_session import spark_session

# **************   Get S3 client   ***************

access_key=config.aws_access_key
secret_key=config.aws_secret_key

s3_client_provider=S3ClientProvider(encrypt_decrypt.decrypt(access_key),encrypt_decrypt.decrypt(secret_key))
s3_client=s3_client_provider.get_s3_client()

response=s3_client.list_buckets()
logger.info('List of Buckets %s',response['Buckets'])

# *********** check if the file is present in local directory
# ******** if already there check same file is present in staging area with status A
# if yes don't delete try to rerun
# else give an error and not process next files

csv_files=[file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
placeholders = ','.join(['%s'] * len(csv_files))
conn=get_mysql_connection()
cursor=conn.cursor()
if csv_files:
    statement=(f"""
                   select distinct file_name from product_staging_table
                       where file_name in ({placeholders}) and status='I'
                   """)
    cursor.execute(statement,csv_files)
    data=cursor.fetchall()
    if data:
        logger.info('Your las run was failed please check')
    else:
        logger.info('No record match')

else:
    logger.info('No csv files found Last run was successfully completed')


# ********************************** Get the absolute file path of the files from s3 ****************

try:
    bucket_name = config.bucket_name
    folder_path = config.s3_source_directory

    s3_reader=S3Reader()
    s3_absolute_file_path=s3_reader.list_files(bucket_name,folder_path,s3_client)
    logger.info('Absolute file path: %s',s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info('No files available at %s',folder_path)
        raise Exception('No data available to process')
except Exception as e:
    logger.info('Exited with error %s',e)


# ************************************ Download the file to local directory *****************************
try:
    bucket_name = config.bucket_name
    folder_path = config.local_directory

    prefix=f's3://{bucket_name}/'
    file_path=[ path[len(prefix):]for path in s3_absolute_file_path]

    download=S3FileDownload(s3_client,bucket_name,folder_path)
    download.download_file(file_path)
except Exception as e:
    logger.info('file downloaded with error %s',e)
    sys.exit()


# ************************************ Get list of all files present in local directory ******************

all_files=os.listdir(config.local_directory)
logger.info('list of files present in local directory %s',all_files)

# ****************************** Filter files with .csv and other ********************************

if all_files:

    csv_files=[]
    error_files=[]

    for file in all_files:
        if file.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(config.local_directory,file)))
        else:
            error_files.append(os.path.abspath(os.path.join(config.local_directory,file)))

    if not csv_files:
        logger.info('No csv files present in local directory to process %s',config.local_directory)
        raise Exception('No csv files present in local directory to process ')
else:
    logger.info('No file are present to process request')
    raise Exception('No file are present to process request')

# ********************* List of csv and error file *****************************
logger.info('csv_files %s',csv_files)
logger.info('error_files %s',error_files)

# ******************** Create Spark Session **************************

spark=spark_session()
logger.info('spark session created %s',spark)