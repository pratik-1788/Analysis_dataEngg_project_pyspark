import datetime
import time
import shutil
from email import message
from pyspark.resource import information
from pyspark.sql.functions import concat_ws, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

from src.main.download.aws_file_download import *
from src.main.move.move_files import move_s3_to_s3
from src.main.read.aws_read import S3Reader
from src.main.read.database_reader import DatabaseReader
from src.main.trasformations.jobs.dimentions_tables_join import *
from src.main.upload.upload_to_S3 import UploadToS3
from src.main.utility.logging_config import logger
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.spark_session import *
import os,sys
from src.main.utility import encrypt_decrypt
from resources.dev import config
from src.main.utility.spark_session import spark_session
from src.main.write.writeDf_into_local import WriteDfIntoLocal

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

logger.info('********** Updating the product_staging_table that we have started the process')

db_name=config.database_name
insert_statements=[]
current_date=datetime.datetime.now()
formated_date=current_date.strftime('%Y_%m_%d_%H_%M_%S')

if correct_files:
    for file in correct_files:

        file_name=os.path.basename(file)
        statement=f"""
                   INSERT INTO {db_name}.{config.product_staging_table}
                    (file_name,file_location ,created_date ,status)
                    VALUES ('{file_name}','{file_name
        }','{formated_date}','A')
                   """
        insert_statements.append(statement)
    logger.info('***************** Insert statement created for SQL statement ***************')
    print(insert_statements)
    logger.info('********************* Connecting to MSQL database ************************')
    conn=get_mysql_connection()
    cursor=conn.cursor()
    logger.info('********************* Connected  to MSQL successfully ************************')

    for statement in insert_statements:
        cursor.execute(statement)
        conn.commit()
    cursor.close()
    conn.close()
    logger.info('******************** Staging table updated successfully ************************')
else:
    logger.info('There is no file to process')
    raise Exception('No data available in correct file')



logger.info('************************ Fixing extra columns coming from source ***********************')

# schema=StructType([
#        StructField("customer_id", IntegerType(), True),
#        StructField("store_id", IntegerType(), True),
#        StructField("product_name", StringType(), True),
#        StructField("sales_date", DateType(), True),
#        StructField("sales_person_id", IntegerType(), True),
#        StructField("price", FloatType(), True),
#        StructField("quantity", IntegerType(), True),
#        StructField("total_cost", FloatType(), True),
#        StructField("additional_columns", StringType(), True),
# ])

# file_df_to_process=spark.createDataFrame([],schema=schema)
# # file_df_to_process.show()
# data = file_df_to_process.collect()
# print(data)
logger.info('******************** Creating empty data frame *********************')

database_client=DatabaseReader(config.url,config.properties)
final_df_to_process=database_client.create_dataframe(spark,'empty_df_create_table')
final_df_to_process.show()

for data in correct_files:
    data_df=spark.read.format('csv')\
                .option('header','True')\
                .option('InferSchema','True')\
                .load(data)
    df_schema=data_df.columns

    extra_columns=set(df_schema) - set(config.mandatory_columns)
    logger.info(f" Extra columns present in sources are {extra_columns} ")
    if extra_columns:
        data_df = data_df.withColumn('additional_columns',concat_ws(',',*extra_columns))\
            .select('customer_id','store_id','product_name','sales_date',
                     'sales_person_id','price','quantity','total_cost','additional_columns')
        logger.info(f' Processed {data}  and added additional columns ')
    else:
        data_df = data_df.withColumn('additional_columns', lit(None)) \
            .select('customer_id', 'store_id', 'product_name', 'sales_date', 'sales_person_id', 'price', 'quantity',
                     'total_cost', 'additional_columns')
        logger.info(f' Processed {data}  and added additional columns ')
    final_df_to_process=final_df_to_process.union(data_df)
logger.info('Final dataframe from source which will be going to process')
final_df_to_process.show()


logger.info('****************** Creating dataframe of Customers *****************')
customer_table_df =database_client.create_dataframe(spark,config.customer_table_name)

logger.info('****************** Creating dataframe of Product *****************')
product_table_df =database_client.create_dataframe(spark,config.product_table)


logger.info('****************** Creating dataframe of product_staging_table *****************')
product_staging_table_df =database_client.create_dataframe(spark,config.product_staging_table)


logger.info('****************** Creating dataframe of sales_team_table *****************')
sales_team_table_df =database_client.create_dataframe(spark,config.sales_team_table)


logger.info('****************** Creating dataframe of store_table *****************')
store_table_df =database_client.create_dataframe(spark,config.store_table)


s3_customer_store_sales_df_join= dimensions_table_join(final_df_to_process,customer_table_df,store_table_df,sales_team_table_df)

logger.info('************************ Final enrich data ****************************')
s3_customer_store_sales_df_join.show()

# s3_customer_store_sales_df_join.write \
#     .mode("overwrite") \
#     .option("header", True) \
#     .csv("file:///mnt/d/spark_test_output")
# writeDf=WriteDfIntoLocal('overwrite','csv')
# writeDf.write(s3_customer_store_sales_df_join,config.error_folder_path_local)
# print('done')
# write the data into there respective data marts in parquet formate
# file will be written to local first
# move the raw data to s3 bucket for reporting tool
# write reporting data into mysql table also

logger.info('****************** Writing data into customer data mart *****************')

writeDf=WriteDfIntoLocal('overwrite','parquet')

final_customer_data_mart_df= s3_customer_store_sales_df_join\
                             .select('ct.customer_id','ct.first_name','ct.last_name',
                                     'ct.address','ct.pincode','phone_number',
                                     'sales_date','total_cost')
writeDf.write(final_customer_data_mart_df,config.customer_data_mart_local_file)

logger.info('**************** Final customer datamart ***************')
final_customer_data_mart_df.show()

logger.info(f'**************** customer data written to local disk {config.customer_data_mart_local_file} ***************')
logger.info('****************** Moving  customer data from local to S3 *****************')

s3_upload=UploadToS3(s3_client)
message=s3_upload.upload_to_s3(config.s3_customer_datamart_directory,bucket_name,config.customer_data_mart_local_file)

logger.info(message)

logger.info("*************************** Writing the data into sales team data mart ******************")

final_sale_team_data_mart_df=s3_customer_store_sales_df_join\
                             .select('store_id','sales_person_id','sales_person_first_name','sales_person_last_name',
                                     'store_manager_name','manager_id','is_manager','sales_person_address',
                                     'sales_person_pincode','sales_date','total_cost',
                                     expr('SUBSTRING(sales_date,1,7) as sales_month'))

writeDf.write(final_sale_team_data_mart_df,config.sales_team_data_mart_local_file)


logger.info('**************** Final sales team data mart ***************')
final_sale_team_data_mart_df.show()

logger.info(f'**************** sales team  data written to local disk {config.sales_team_data_mart_local_file} ***************')
logger.info('****************** Moving  sales team  data from local to S3 *****************')

message=s3_upload.upload_to_s3(config.s3_sales_datamart_directory,bucket_name,config.sales_team_data_mart_local_file)

logger.info(message)

# writing data into partition so it can optimize the query

final_sale_team_data_mart_df.write.format('parquet')\
                             .option('header','true')\
                             .mode('overwrite')\
                             .partitionBy('sales_month','store_id')\
                             .save(config.sales_team_data_mart_partitioned_local_file)

# Move data on s3 partition folder

s3_prefix='sales_partitioned_data_mart'
current_epoc=int(datetime.datetime.now().timestamp()) * 1000

for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):

    for file in files:
        local_file_path=os.path.join(root,file)
        relative_file_path=os.path.relpath(local_file_path,config.sales_team_data_mart_partitioned_local_file)
        s3_key=f"{s3_prefix}/{current_epoc}/{relative_file_path}"
        s3_client.upload_file(local_file_path,config.bucket_name,s3_key)




