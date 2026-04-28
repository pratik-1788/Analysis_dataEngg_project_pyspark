from src.main.utility.logging_config import logger
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.spark_session import *
import os,sys
from src.main.utility import encrypt_decrypt
from resources.dev import config

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