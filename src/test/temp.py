

from src.main.utility.spark_session import spark_session

spark=spark_session()
print(spark.version)
print(spark.sparkContext._jsc.hadoopConfiguration().get("fs.defaultFS"))