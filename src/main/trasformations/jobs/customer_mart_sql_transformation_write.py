import mysql

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from resources.dev import config
from src.main.write.database_writer import *
def customer_mart_calculation_table_write(final_customer_data_mart_df):
    window=Window.partitionBy('customer_id','sales_date_month')

    final_customer_data_mart=final_customer_data_mart_df.withColumn('sales_date_month',substring(col('sales_date'),1,7))\
                             .withColumn('total_sales_every_month_by_each_customer',sum('total_cost').over(window))\
                              .select('customer_id',concat(col('first_name'),lit(''),col('last_name')).alias('full_name'),
                                      'address','phone_number','sales_date_month',col('total_sales_every_month_by_each_customer').alias('total_sales'))\
                              .distinct()

    # write the data into mysql table
    final_customer_data_mart.show()

    db_write = DatabaseWriter(url=config.url, properties=config.properties)
    db_write.write_dataframe(final_customer_data_mart,config.customer_data_mart_table)

#
