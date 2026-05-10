from pyspark.sql.functions import *
from pyspark.sql.window import Window
from resources.dev import config
from src.main.write.database_writer import DatabaseWriter


def sales_mart_sql_transformation_write(final_sale_team_data_mart_df):
    window = Window.partitionBy('store_id','sales_person_id','sales_date_month')

    final_sale_team_data_mart=final_sale_team_data_mart_df.withColumn('sales_date_month',substring(col('sales_date'),1,7))\
                              .withColumn('total_sales_every_month',sum('total_cost').over(window))\
                              .select('store_id','sales_person_id',concat(col('sales_person_first_name'),
                                                                          lit(''),col('sales_person_last_name')).alias('full_name'),
                                                        'sales_month','total_sales_every_month').distinct()


    rank_window = Window.partitionBy('store_id','sales_month').orderBy(col('total_sales_every_month').desc())

    final_sale_team_data_mart_table=final_sale_team_data_mart.withColumn('rnk',rank().over(rank_window))\
                                    .withColumn('incentive',when(col('rnk')==1,col('total_sales_every_month')*0.01).otherwise(lit(0)))\
                                    .withColumn('incentive',round(col('incentive'),2))\
                                    .withColumn('total_sales',col('total_sales_every_month'))\
                                    .select('store_id','sales_person_id','full_name','sales_month','total_sales','incentive')

    final_sale_team_data_mart_table.show()

    db_write=DatabaseWriter(url=config.url,properties=config.properties)
    db_write.write_dataframe(final_sale_team_data_mart_table,config.sales_team_data_mart_table)

