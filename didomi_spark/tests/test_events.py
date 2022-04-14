from pathlib import Path

from didomi_spark.jobs import events
from didomi_spark.repositories.events import EventRepository
from didomi_spark.schemas.events import EventSchemas
from pyspark.sql import DataFrame, SparkSession


def test_extract_partition_info():
    test_file = Path("/data/test/dt=2021-01-01/file.json")
    p_name, p_value = EventRepository._extract_partition_info(test_file)
    assert p_name == "dt"
    assert p_value == "2021-01-01"


def test_read_data_metrics(events_input_path: Path):
    assert list(events_input_path.iterdir()) != 0


def test_raw_events_ingestion(spark_session: SparkSession, events_data):
    df = spark_session.sql("select * from raw_events")
    assert df.count() == 62


def test_raw_events_schema(spark_session: SparkSession, events_data):
    event_schemas = EventSchemas()
    df = spark_session.sql("select * from raw_events")
    assert df.schema == df.schema.fromJson(event_schemas.raw_event)


def test_map_events(spark_session: SparkSession, events_data):
    raw_events = spark_session.sql("select * from raw_events")
    mapped_events = events.map_events(raw_events)
    assert mapped_events.count() == 62


def test_events_schema(spark_session: SparkSession, events_data):
    event_schemas = EventSchemas()
    raw_events = spark_session.sql("select * from raw_events")
    mapped_events = events.map_events(raw_events)
    assert mapped_events.schema == mapped_events.schema.fromJson(event_schemas.event)


def test_extract_user_consent(spark_session: SparkSession, events_data):
    raw_events = spark_session.sql("select * from raw_events")
    users_consents = events.extract_user_consent(raw_events)
    assert users_consents.columns == raw_events.columns + ["user_consent"]
    assert sum([row["user_consent"] for row in users_consents.collect()]) == 31


def test_deduplicate_by_event_id(spark_session: SparkSession, events_data):
    event_schemas = EventSchemas()
    raw_events = spark_session.sql("select * from raw_events")
    deduplicated = events.deduplicate_by_event_id(raw_events)
    assert deduplicated.schema == raw_events.schema.fromJson(event_schemas.raw_event)
    assert deduplicated.count() == 57


def test_aggregate_by_event_type(mapped_events_data: DataFrame):
    pageviews = events.aggregate_by_event_type(mapped_events_data, events_types=[events.EventType.pageviews])
    assert pageviews.columns == ["datehour", "domain", "country", "pageviews"]
    assert sum([row["pageviews"] for row in pageviews.collect()]) == 31


def test_aggregate_by_event_type_with_consent(mapped_events_data: DataFrame):
    pageviews_with_consent = events.aggregate_by_event_type(mapped_events_data, events_types=[events.EventType.pageviews], with_consent=True)
    assert pageviews_with_consent.columns == ["datehour", "domain", "country", "pageviews_with_consent"]
    assert sum([row["pageviews_with_consent"] for row in pageviews_with_consent.collect()]) == 16


def test_aggregate_avg_pageviews_per_user(mapped_events_data: DataFrame):
    avg_pageviews_per_user = events.aggregate_avg_pageviews_per_user(mapped_events_data)
    assert avg_pageviews_per_user.columns == ["datehour", "domain", "country", "avg_pageviews_per_user"]
    assert sum([row["avg_pageviews_per_user"] for row in avg_pageviews_per_user.collect()]) == 21.83


def test_aggregate_events_metrics(spark_session: SparkSession, events_data):
    raw_events = spark_session.sql("select * from raw_events")
    event_schemas = EventSchemas()
    events_metrics = events.aggregate_events_metrics(raw_events)
    assert events_metrics.schema == events_metrics.schema.fromJson(event_schemas.events_metrics)
    assert events_metrics.count() == 10
