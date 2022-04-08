import shutil
import tempfile
from pathlib import Path
from typing import Tuple

from didomi_spark.core.logs import logger
from pyspark.sql import SparkSession, DataFrame


class EventRepository:
    def __init__(self, spark_session: SparkSession) -> None:
        self.spark_session = spark_session

    def fetch_raw_events(self) -> DataFrame:
        return self.spark_session.sql("select * from raw_events")

    def create_hive_table(self):
        self.spark_session.sql(f"add jar didomi_spark/lib/json-serde-1.3.8-jar-with-dependencies.jar")
        self.spark_session.sql("CREATE DATABASE IF NOT EXISTS didomi")
        self.spark_session.sql(
            f"""
            CREATE TABLE IF NOT EXISTS raw_events (
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
        self.spark_session.sql("ALTER TABLE raw_events SET SERDEPROPERTIES ('ignore.malformed.json'='true')")

    def load_into_hive(self, input_events: Path) -> None:
        """Load event files into Hive

        Args:
            spark_session (SparkSession)
            input_events (Path): absolute Path to files containing raw events
        """
        files = list(input_events.glob("*/*.json"))  # we assume only one known partition
        partitions = list(set([self._extract_partition_info(file) for file in files]))
        for partition_name, partition_value in partitions:
            for file in files:
                if f"{partition_name}={partition_value}/{file.name}" in file.as_posix():
                    self.spark_session.sql(
                        f"""
                        LOAD DATA INPATH '{file.as_posix()}' 
                        OVERWRITE INTO TABLE raw_events
                        PARTITION ({partition_name}='{partition_value}')
                        """
                    )

    @staticmethod
    def extract_from_file(zip_file: Path) -> Path:
        dest = Path(tempfile.mkdtemp())
        shutil.unpack_archive(zip_file, dest)
        logger.info(f"Extracted {zip_file} to {dest}.")
        return dest

    @staticmethod
    def _extract_partition_info(filepath: Path) -> Tuple[str, str]:
        """Extract partition name and value from partitionned file Path

        Args:
            filepath (Path): absolute Path to file, ie. path/to/dt=2022-01-01/file

        Returns:
            Tuple[str, str]: partition name and value, ie. ("dt", "2022-01-01)
        """
        partition = filepath.parent.name
        partition_name, partition_value = partition.split("=")
        return partition_name, partition_value
