from pathlib import Path

from didomi_spark.jobs.events import check_consent, map_events
from didomi_spark.repositories.events import EventRepository
from didomi_spark.schemas.events import EventSchemas
from pyspark.sql import SparkSession


def test_extract_partition_info():
    test_file = Path("/data/test/dt=2021-01-01/file.json")
    p_name, p_value = EventRepository._extract_partition_info(test_file)
    assert p_name == "dt"
    assert p_value == "2021-01-01"


def test_read_data_metrics(events_input_path: Path):
    assert list(events_input_path.iterdir()) != 0


def test_raw_events_ingestion(spark_session: SparkSession, events_data):
    df = spark_session.sql("select * from raw_events")
    print(df.show(truncate=False))
    assert df.count() == 62


def test_raw_events_schema(spark_session: SparkSession, events_data):
    event_schemas = EventSchemas()
    df = spark_session.sql("select * from raw_events")
    assert df.schema == df.schema.fromJson(event_schemas.raw_event)


def test_map_events(spark_session: SparkSession, events_data):
    raw_events = spark_session.sql("select * from raw_events")
    mapped_events = map_events(raw_events)
    print(mapped_events.show(truncate=False))
    assert mapped_events.count() == 62


def test_events_schema(spark_session: SparkSession, events_data):
    event_schemas = EventSchemas()
    raw_events = spark_session.sql("select * from raw_events")
    mapped_events = map_events(raw_events)
    print(mapped_events.schema.jsonValue())
    # assert mapped_events.schema == mapped_events.schema.fromJson(event_schemas.event)
