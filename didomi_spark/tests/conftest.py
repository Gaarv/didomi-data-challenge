import logging
from pathlib import Path

import pytest
from didomi_spark.core.session import build_spark_session
from didomi_spark.jobs.events import deduplicate_by_event_id, map_events
from didomi_spark.repositories.events import EventRepository
from pyspark.sql import SparkSession
from pytest import FixtureRequest


def quiet_py4j():
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session(request: FixtureRequest):
    spark_session = build_spark_session(cluster_mode=False)
    request.addfinalizer(lambda: spark_session.stop())
    quiet_py4j()
    yield spark_session


@pytest.fixture(scope="session")
def events_input_path():
    return EventRepository.extract_from_file(Path("didomi_spark/tests/resources/input.zip"))


@pytest.fixture(scope="session")
def hive_metastore(spark_session: SparkSession):
    event_repository = EventRepository(spark_session)
    event_repository.create_hive_table()


@pytest.fixture(scope="session")
def events_data(spark_session: SparkSession, events_input_path: Path, hive_metastore):
    event_repository = EventRepository(spark_session)
    event_repository.load_into_hive(events_input_path)


@pytest.fixture(scope="session")
def mapped_events_data(spark_session: SparkSession, events_data):
    raw_events = spark_session.sql("select * from raw_events")
    deduplicated_events = deduplicate_by_event_id(raw_events)
    mapped_events = map_events(deduplicated_events)
    return mapped_events
