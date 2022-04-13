from pyspark.sql import SparkSession
from didomi_spark.core.configuration import Configuration


def build_spark_session(cluster_mode: bool = True) -> SparkSession:
    """Factory function to build a SparkSession.

    Args:
        cluster_mode (bool, optional): Whether to run in clutser mode or local mode. Defaults to True.

    Returns:
        SparkSession
    """
    conf = Configuration()
    spark_conf = conf.spark_conf
    if cluster_mode:
        spark_session = SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()
    else:
        spark_session = SparkSession.builder.config(conf=spark_conf).master("local[*]").enableHiveSupport().getOrCreate()
    return spark_session
