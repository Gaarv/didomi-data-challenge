from pathlib import Path

from didomi_spark.core.logs import logger
from didomi_spark.repositories.events import EventRepository
from didomi_spark.schemas.events import EventSchemas
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

event_schemas = EventSchemas()


class EventJob:
    def __init__(self, spark_session: SparkSession) -> None:
        self.spark_session = spark_session
        self.zip_file = Path("didomi_spark/data/input.zip")
        self.repository = EventRepository(self.spark_session)
        self.schemas = EventSchemas()

    def run(self) -> None:
        events_path = self.repository.extract_from_file(self.zip_file)
        self.repository.create_hive_table()
        self.repository.load_into_hive(events_path)
        raw_events = self.repository.fetch_raw_events()
        mapped_events = map_events(raw_events)


def map_events(df: DataFrame) -> DataFrame:
    raw_cols = [F.col(c) for c in ["id", "type", "domain", "datehour"]]
    mapped_cols = [F.col(f"user.{c}").alias(f"user_{c}") for c in ["id", "country", "token"]]
    events = df.dropna(how="any").select(raw_cols + mapped_cols).transform(check_consent)
    return events


def check_consent(df: DataFrame) -> DataFrame:
    return df.withColumn("user_consent", F.from_json("user_token", schema=event_schemas.token))
