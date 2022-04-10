from pathlib import Path
from typing import Callable
from didomi_spark.core.logs import logger
from didomi_spark.repositories.events import EventRepository
from didomi_spark.schemas.events import EventSchemas
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

event_schemas = EventSchemas()
events_metrics_window = Window.partitionBy(F.col("datehour"), F.col("domain"), F.col("user_country"))


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
        deduplicated_events = deduplicate_by_event_id(raw_events)
        mapped_events = map_events(deduplicated_events)
        metrics = aggregate_events_metrics(mapped_events)
        # save as partitionned parquet


def map_events(df: DataFrame) -> DataFrame:
    raw_cols = [F.col(c) for c in ["id", "type", "domain", "datehour"]]
    mapped_cols = [F.col(f"user.{c}").alias(f"user_{c}") for c in ["id", "country", "token"]]
    events = df.dropna(how="any").select(raw_cols + mapped_cols).transform(extract_user_consent)
    return events


def extract_user_consent(df: DataFrame) -> DataFrame:
    df_with_consent = (
        df.withColumn("token_data", F.from_json("user_token", schema=event_schemas.token))
        .withColumn("user_consent", F.when(F.size("token_data.purposes.enabled") > 0, 1).otherwise(0))
        .drop(*[F.col("token_data"), F.col("user_token")])
    )
    return df_with_consent


def deduplicate_by_event_id(df: DataFrame) -> DataFrame:
    window = Window.partitionBy(F.col("datehour"), F.col("id")).orderBy("id")
    deduplicated_df = df.withColumn("event_id_row", F.row_number().over(window)).filter(F.col("event_id_row") == 1).drop(F.col("event_id_row"))
    return deduplicated_df


def aggregate_events_metrics(df: DataFrame) -> DataFrame:
    df.persist(StorageLevel.MEMORY_AND_DISK)
    aggregated_df = (
        df.transform(aggregate_pageviews)
        .transform(aggregate_pageviews_with_consent)
        .transform(aggregate_consents_asked)
        .transform(aggregate_consents_asked_with_consent)
        .transform(aggregate_consents_given_with_consent)
        .transform(aggregate_avg_pageviews_per_user)
    )
    return aggregated_df


def aggregate_by_type_factory(df: DataFrame, input_col: str, output_col: str, with_consent: bool) -> Callable[DataFrame, DataFrame]:
    pass


def aggregate_pageviews(df: DataFrame) -> DataFrame:
    _df = df.filter(F.col("type") == "pageview").withColumn("pageviews", F.count("type").over(events_metrics_window))
    return _df


def aggregate_pageviews_with_consent(df: DataFrame) -> DataFrame:
    _df = (
        df.filter(F.col("user_consent") == 1)
        .filter(F.col("type") == "pageview")
        .withColumn("pageviews_with_consent", F.count("type").over(events_metrics_window))
    )
    return _df


def aggregate_consents_asked(df: DataFrame) -> DataFrame:
    _df = df.filter(F.col("type") == "consent.asked").withColumn("consents_asked", F.count("type").over(events_metrics_window))
    return _df


def aggregate_consents_given(df: DataFrame) -> DataFrame:
    pass


def aggregate_consents_asked_with_consent(df: DataFrame) -> DataFrame:
    pass


def aggregate_consents_given_with_consent(df: DataFrame) -> DataFrame:
    pass


def aggregate_avg_pageviews_per_user(df: DataFrame) -> DataFrame:
    pass
