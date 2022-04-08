import logging
from pathlib import Path

import pytest
from didomi_spark.repositories import events
from pyspark.sql import SparkSession
from pytest import FixtureRequest


def quiet_py4j():
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session(request: FixtureRequest):
    spark_session = SparkSession.builder.master("local[*]").appName("didomi_spark_tests").enableHiveSupport().getOrCreate()
    request.addfinalizer(lambda: spark_session.stop())
    quiet_py4j()
    yield spark_session


@pytest.fixture(scope="session")
def events_input_path():
    return events.extract_from_file(Path("didomi_spark/tests/resources/input.zip"))


@pytest.fixture(scope="session")
def hive_metastore(spark_session: SparkSession):
    events.create_hive_table(spark_session)


@pytest.fixture(scope="session")
def events_data(spark_session: SparkSession, events_input_path: Path, hive_metastore):
    events.load_into_hive(spark_session, events_input_path)
