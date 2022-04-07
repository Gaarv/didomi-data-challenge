import json
import logging
import shutil
from pathlib import Path
import pytest
from pyspark.sql import SparkSession
from pytest import FixtureRequest, TempPathFactory


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
def data_metrics_input_path(tmp_path_factory: TempPathFactory):
    source = Path("didomi_spark/data/input.zip")
    dest = tmp_path_factory.mktemp("data").joinpath("raw")
    shutil.unpack_archive(source, dest)
    print(list(dest.iterdir()))
    yield dest


@pytest.fixture(scope="session")
def hive_metastore(spark_session: SparkSession):
    spark_session.sql(f"add jar didomi_spark/tests/resources/json-serde-1.3.8-jar-with-dependencies.jar")
    spark_session.sql("CREATE DATABASE didomi")
    spark_session.sql(
        f"""
        CREATE TABLE events (
        id string,
        datetime string,
        domain string,
        type string,
        user struct<id:string,country:string,token:string>
        )
        PARTITIONED BY (datehour string)
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        STORED AS TEXTFILE
        """
    )
    # The below line prevents from bad JSON being inserted but insert nulls instead
    spark_session.sql("ALTER TABLE events SET SERDEPROPERTIES ('ignore.malformed.json'='true')")


@pytest.fixture(scope="session")
def events_data(spark_session: SparkSession, data_metrics_input_path: Path, hive_metastore):
    files = list(data_metrics_input_path.iterdir())
    print(files)
    # spark_session.sql("")
