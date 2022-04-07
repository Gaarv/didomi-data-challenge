from pyspark.sql import SparkSession
from didomi_spark.core.configuration import Configuration

spark_session = SparkSession.builder.config(conf=Configuration().spark_conf).enableHiveSupport().getOrCreate()
