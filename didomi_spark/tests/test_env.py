import os
import shutil

from pyspark.sql import SparkSession


def test_environment_variables():
    """Ensure that Spark is present in PATH and that mandatory environment variables are set"""
    assert shutil.which("pyspark") is not None
    assert os.getenv("SPARK_HOME") is not None
    assert os.getenv("PYSPARK_PYTHON") is not None


def test_spark_version(spark_session: SparkSession):
    """Test spark_session fixture with a basic test"""
    assert "3." in spark_session.version
