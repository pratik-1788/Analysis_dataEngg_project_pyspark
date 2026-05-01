import time
import shutil
from email import message
from pyspark.resource import information
from src.main.download.aws_file_download import *
from src.main.move.move_files import move_s3_to_s3
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
                       where file_name in ({placeholders}) and status='A'
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
logger.info('****************************** Spark session created *************************')

logger.info("****************************** Checking schema for data loaded in s3 *************************")

# check the required column in the schema of csv files
# if not required column keep it in error list
# else union all the data into one dataframe

correct_files=[]

for data in csv_files:

    data_schema=spark.read.format('csv')\
                .option('header','True')\
                .load(data).columns
    logger.info(f"schema for the {data} is {data_schema} ")
    logger.info(f"Mandatory columns schema is {config.mandatory_columns} ")
    missing_columns=set(config.mandatory_columns) - set(data_schema)
    logger.info(f'Missing columns are {missing_columns} ')
    if missing_columns:
        error_files.append(data)

    else:
        logger.info(f'No missing columns found in {data} ')
        correct_files.append(data)

logger.info(f'********************* List of correct files *****************\\ {correct_files}')
logger.info(f'********************** List of error files ********************** \\{error_files} ')
logger.info('********** Moving error data into error directory ****************')

error_files_local_path=config.error_folder_path_local

if error_files:

    for file in error_files:
        if os.path.exists(file):
            file_name = os.path.basename(file)
            destination_file_path=os.path.join(error_files_local_path,file_name)

            shutil.move(file,destination_file_path)
            logger.info(f"Moved {file_name} to {destination_file_path}")


            source_prefix=config.s3_source_directory
            destination_prefix=config.s3_error_directory

            message=move_s3_to_s3(s3_client,config.bucket_name,source_prefix,destination_prefix,file_name)
            logger.info(message)

        else:
            logger.info(f'{file} does not exist in ')
else:
    logger.info(f'No error file are present')

# Additional column needs to be taken care
# determine extra column

# before running the process
# stage table needs to be update with status as active or inactive

logger.info('**********updating the product _staging_table that we have started the process')

