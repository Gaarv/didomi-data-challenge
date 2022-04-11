from enum import Enum, unique
from functools import reduce
from pathlib import Path
from typing import Callable, List

from didomi_spark.core.logs import logger
from didomi_spark.repositories.events import EventRepository
from didomi_spark.schemas.events import EventSchemas
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

event_schemas = EventSchemas()
Transformation = Callable[[DataFrame], DataFrame]


@unique
class EventType(Enum):
    pageviews = "pageview"
    consents_asked = "consent.asked"
    consents_given = "consent.given"


class EventJob:
    def __init__(self, spark_session: SparkSession) -> None:
        self.spark_session = spark_session
        self.zip_file = Path("didomi_spark/data/input.zip")
        self.repository = EventRepository(self.spark_session)
        self.schemas = EventSchemas()

    def load(self) -> DataFrame:
        events_path = self.repository.extract_from_file(self.zip_file)
        self.repository.create_hive_table()
        self.repository.load_into_hive(events_path)
        raw_events = self.repository.fetch_raw_events()
        return raw_events

    def preprocess(self, df: DataFrame) -> DataFrame:
        deduplicated_events = deduplicate_by_event_id(df)
        mapped_events = map_events(deduplicated_events)
        return mapped_events

    def transform(self, df: DataFrame) -> DataFrame:
        mapped_events = self.preprocess(df)
        metrics = aggregate_events_metrics(mapped_events)
        return metrics

    def save(self, df: DataFrame) -> None:
        df.write.mode("overwrite").partitionBy("datehour").parquet("output")

    def run(self) -> None:
        self.save(self.transform(self.preprocess(self.load())))


def map_events(raw_events: DataFrame) -> DataFrame:
    event_cols = [F.col(f"{c}").alias(f"event_{c}") for c in ["id", "type"]]
    raw_cols = [F.col(c) for c in ["domain", "datehour"]]
    user_cols = [F.col(f"user.{c}").alias(f"user_{c}") for c in ["id", "country", "token"]]
    events = raw_events.dropna(how="any").select(event_cols + raw_cols + user_cols).transform(extract_user_consent)
    return events


def extract_user_consent(raw_events: DataFrame) -> DataFrame:
    user_consent = (
        raw_events.withColumn("token_data", F.from_json("user_token", schema=event_schemas.token))
        .withColumn("user_consent", F.when(F.size("token_data.purposes.enabled") > 0, 1).otherwise(0))
        .drop("token_data", "user_token")
    )
    return user_consent


def deduplicate_by_event_id(raw_events: DataFrame) -> DataFrame:
    window = Window.partitionBy(F.col("datehour"), F.col("id")).orderBy("id")
    deduplicated = raw_events.withColumn("event_id_row", F.row_number().over(window)).filter(F.col("event_id_row") == 1).drop("event_id_row")
    return deduplicated


def aggregate_events_metrics(events: DataFrame) -> DataFrame:
    events.persist(StorageLevel.MEMORY_AND_DISK)  # cache before successive transformations
    aggregate = aggregate_by_event_type(
        events,
        events_types=[
            EventType.pageviews,
            EventType.consents_asked,
            EventType.consents_given,
        ],
    )
    aggregate_with_consent = aggregate_by_event_type(
        events,
        events_types=[
            EventType.pageviews,
            EventType.consents_asked,
            EventType.consents_given,
        ],
        with_consent=True,
    )
    aggregates = aggregate.join(aggregate_with_consent, on=["datehour", "domain", "country"], how="outer").na.fill(0)
    return aggregates


def aggregate_by_event_type(events: DataFrame, events_types: List[EventType], with_consent: bool = False) -> DataFrame:
    aggregated = (
        events.transform(filter_by_user_consent(with_consent))
        .groupBy(F.col("datehour"), F.col("domain"), F.col("user_country").alias("country"))
        .pivot("event_type", values=[e.value for e in events_types])
        .agg(F.count(F.col("event_type")))
        .na.fill(0)
        .transform(rename_by_user_consent(events_types, with_consent))
    )
    return aggregated


def aggregate_avg_pageviews_per_user(events: DataFrame) -> DataFrame:
    window = Window.partitionBy(F.col("datehour"), F.col("domain"), F.col("user_country"), F.col("user_id"))
    avg_pageviews_per_user = events.filter(F.col("event_type") == EventType.pageviews.value).withColumn(
        "avg_pageviews_per_user", F.round(F.avg(F.col("event_type")).over(window), 2)
    )
    return avg_pageviews_per_user


def filter_by_user_consent(with_consent: bool = False) -> Transformation:
    f: Transformation = lambda df: df.filter(F.col("user_consent") == 1) if with_consent else df
    return f


def rename_by_user_consent(events_types: List[EventType], with_consent: bool = False) -> Transformation:
    events = [e for e in events_types]
    column_alias: Callable[[EventType], str] = lambda e: f"{e.name}_with_consent" if with_consent else e.name
    f: Transformation = lambda df: reduce(lambda _df, event: _df.withColumnRenamed(event.value, column_alias(event)), events, df)
    return f
