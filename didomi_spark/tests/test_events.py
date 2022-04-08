from pathlib import Path

from didomi_spark.repositories import events
from pyspark.sql import SparkSession


def test_extract_partition_info():
    test_file = Path("/data/test/dt=2021-01-01/file.json")
    p_name, p_value = events.extract_partition_info(test_file)
    assert p_name == "dt"
    assert p_value == "2021-01-01"


def test_read_data_metrics(events_input_path: Path):
    assert list(events_input_path.iterdir()) != 0


def test_hive_metastore(spark_session: SparkSession, events_data):
    df = spark_session.sql("select * from events")
    assert df.count() == 62
