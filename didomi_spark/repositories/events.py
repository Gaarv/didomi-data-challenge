import shutil
import tempfile
from pathlib import Path
from typing import Tuple

from didomi_spark.core.logs import logger
from pyspark.sql import SparkSession


def extract_from_file(zip_file: Path) -> Path:
    dest = Path(tempfile.mkdtemp())
    shutil.unpack_archive(zip_file, dest)
    logger.info(f"Extracted {zip_file} to {dest}.")
    return dest


def create_hive_table(spark_session: SparkSession):
    spark_session.sql(f"add jar didomi_spark/lib/json-serde-1.3.8-jar-with-dependencies.jar")
    spark_session.sql("CREATE DATABASE IF NOT EXISTS didomi")
    spark_session.sql(
        f"""
        CREATE TABLE IF NOT EXISTS events (
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
    # prevents from bad JSON being inserted but insert nulls instead
    spark_session.sql("ALTER TABLE events SET SERDEPROPERTIES ('ignore.malformed.json'='true')")


def load_into_hive(spark_session: SparkSession, input_events: Path):
    """Load event files into Hive

    Args:
        spark_session (SparkSession)
        input_events (Path): absolute Path to files containing events
    """
    files = list(input_events.glob("*/*.json"))  # we assume only one known partition
    partitions = list(set([extract_partition_info(file) for file in files]))
    for partition_name, partition_value in partitions:
        for file in files:
            if f"{partition_name}={partition_value}/{file.name}" in file.as_posix():
                spark_session.sql(
                    f"""
                    LOAD DATA INPATH '{file.as_posix()}' 
                    OVERWRITE INTO TABLE events
                    PARTITION ({partition_name}='{partition_value}')
                    """
                )


def extract_partition_info(filepath: Path) -> Tuple[str, str]:
    """Extract partition name and value from partitionned file Path

    Args:
        filepath (Path): absolute Path to file, ie. path/to/dt=2022-01-01/file

    Returns:
        Tuple[str, str]: partition name and value, ie. ("dt", "2022-01-01)
    """
    partition = filepath.parent.name
    partition_name, partition_value = partition.split("=")
    return partition_name, partition_value
