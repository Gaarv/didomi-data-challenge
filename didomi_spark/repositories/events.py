import shutil
import tempfile
from pathlib import Path
from typing import Tuple

from didomi_spark.core.logs import logger
from pyspark.sql import DataFrame, SparkSession
from s3path import S3Path


class EventRepository:
    """Centralize interaction with data sources"""

    def __init__(self, spark_session: SparkSession) -> None:
        self.spark_session = spark_session

    def fetch_raw_events_from_s3(self, uri: S3Path) -> DataFrame:
        raise NotImplementedError

    def fetch_raw_events_from_hive(self) -> DataFrame:
        return self.spark_session.sql("select * from raw_events")

    def create_hive_table(self):
        """Create Hive table where :attr:`~didomi_spark.schemas.events.EventSchemas.raw_event` data will be loaded.
        In a real life scenario this would be probably handled by a separated job / service outside of Spark for idempotency.
        """
        db_name = "didomi"
        table_name = "raw_events"
        self.spark_session.sql(f"add jar didomi_spark/lib/json-serde-1.3.8-jar-with-dependencies.jar")
        logger.info(f"Creating Hive database", db_name=db_name)
        self.spark_session.sql(f"DROP DATABASE IF EXISTS {db_name}")
        self.spark_session.sql(f"CREATE DATABASE {db_name}")
        logger.info(f"Creating Hive table", table_name=table_name)
        self.spark_session.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
            id string,
            datetime string,
            type string,
            domain string,
            user struct<id:string,country:string,token:string>
            )
            PARTITIONED BY (datehour string)
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            STORED AS TEXTFILE
            """
        )
        # prevents from bad JSON being inserted but insert nulls instead
        self.spark_session.sql(f"ALTER TABLE {table_name} SET SERDEPROPERTIES ('ignore.malformed.json'='true')")

    def load_into_hive(self, input_events: Path) -> None:
        """Load raw event files into Hive

        Args:
            spark_session (SparkSession)
            input_events (Path): absolute Path to files containing raw events
        """
        table_name = "raw_events"
        files = list(input_events.glob("*/*.json"))  # we assume only one partition
        logger.info("Loading files into Hive...", num_files=len(files), table_name=table_name)
        partitions = list(set([self._extract_partition_info(file) for file in files]))
        for partition_name, partition_value in partitions:
            for file in files:
                if f"{partition_name}={partition_value}/{file.name}" in file.as_posix():
                    self.spark_session.sql(
                        f"""
                        LOAD DATA INPATH '{file.as_posix()}' 
                        OVERWRITE INTO TABLE {table_name}
                        PARTITION ({partition_name}='{partition_value}')
                        """
                    )

    @staticmethod
    def extract_from_file(zip_file: Path) -> Path:
        """Extract zip file containing raw events

        Args:
            zip_file (Path): relative path from root package to the zip file, ie. "didomi_spark/data/input.zip"

        Returns:
            Path: path to extracted files
        """
        dest = Path(tempfile.mkdtemp())
        logger.info("Extracting input data...", input_file=zip_file.as_posix(), output_dir=dest.as_posix())
        shutil.unpack_archive(zip_file, dest)
        return dest

    @staticmethod
    def _extract_partition_info(filepath: Path) -> Tuple[str, str]:
        """Extract partition name and value from partitionned file Path. Only meant to be used in local mode or tests.

        Args:
            filepath (Path): absolute Path to file, ie. path/to/dt=2022-01-01/file

        Returns:
            Tuple[str, str]: partition name and value, ie. ("dt", "2022-01-01)
        """
        partition = filepath.parent.name
        partition_name, partition_value = partition.split("=")
        return partition_name, partition_value
