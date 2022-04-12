import os
import shutil

from pyspark.sql import SparkSession


def test_environment():
    """Ensure that Spark is present"""
    assert shutil.which("pyspark") is not None


def test_spark_version(spark_session: SparkSession):
    """Test spark_session fixture with a basic test"""
    assert "3." in spark_session.version
