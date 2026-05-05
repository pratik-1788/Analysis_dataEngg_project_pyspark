import findspark
findspark.init()
from pyspark.sql import *
from src.main.utility.logging_config import *
import os



def spark_session():
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["hadoop.home.dir"] = "C:\\hadoop"
    spark = SparkSession.builder.master("local[*]") \
        .appName("pratik_spark")\
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.26") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .config("spark.sql.sources.commitProtocolClass",
                "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
        .config("spark.sql.parquet.output.committer.class",
                "org.apache.parquet.hadoop.ParquetOutputCommitter") \
        .getOrCreate()
    logger.info("spark session %s",spark)
    return spark