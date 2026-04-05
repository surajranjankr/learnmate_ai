from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

import pandas as pd

from learnmate_ai.config import AppConfig, get_config
from learnmate_ai.spark_manager import get_spark_session
from learnmate_ai.storage import ensure_data_directories, resolve_storage_uri

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import LinearRegression
except Exception:
    DataFrame = Any
    F = None
    T = None
    KMeans = None
    VectorAssembler = None
    LinearRegression = None


LOG_SCHEMA = T.StructType(
    [
        T.StructField('user_id', T.StringType(), True),
        T.StructField('timestamp', T.StringType(), True),
        T.StructField('topic', T.StringType(), True),
        T.StructField('action_type', T.StringType(), True),
        T.StructField('score', T.DoubleType(), True),
        T.StructField('quiz_id', T.StringType(), True),
        T.StructField('question_count', T.IntegerType(), True),
        T.StructField('question', T.StringType(), True),
        T.StructField('response_preview', T.StringType(), True),
        T.StructField('metadata', T.MapType(T.StringType(), T.StringType()), True),
    ]
) if T is not None else None

BRONZE_TABLES = [
    'users', 'documents', 'summaries', 'study_sessions', 'quiz_results', 'quiz_questions',
    'chat_sessions', 'chat_messages', 'events',
]


def _ensure_spark_ready() -> None:
    if F is None or T is None:
        raise RuntimeError('PySpark is unavailable. Install dependencies before running the Spark pipelines.')


def _sqlite_connection(config: AppConfig) -> sqlite3.Connection:
    connection = sqlite3.connect(config.sqlite_db_path)
    connection.row_factory = sqlite3.Row
    return connection


def _empty_log_frame(spark) -> DataFrame:
    return spark.createDataFrame([], LOG_SCHEMA)


def _read_json_log(spark, path: Path) -> DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return _empty_log_frame(spark)
    return spark.read.schema(LOG_SCHEMA).json(str(path))


def _read_event_lake(spark, config: AppConfig) -> DataFrame:
    raw_events_root = Path(config.raw_events_dir)
    if not raw_events_root.exists():
        return _empty_log_frame(spark)
    if not list(raw_events_root.rglob('*.json')):
        return _empty_log_frame(spark)
    return spark.read.schema(LOG_SCHEMA).json(str(raw_events_root))


def _read_operational_table(spark, config: AppConfig, table_name: str) -> DataFrame:
    with _sqlite_connection(config) as connection:
        frame = pd.read_sql_query(f'SELECT * FROM {table_name}', connection)
    if frame.empty:
        return spark.createDataFrame([], T.StructType([]))
    return spark.createDataFrame(frame)


def load_log_dataframes(spark, config: AppConfig | None = None) -> dict[str, DataFrame]:
    app_config = ensure_data_directories(config or get_config())
    return {
        'quiz': _read_json_log(spark, app_config.logs_dir / 'quiz_logs.json'),
        'chat': _read_json_log(spark, app_config.logs_dir / 'chat_logs.json'),
        'activity': _read_json_log(spark, app_config.logs_dir / 'user_activity.json'),
        'event_lake': _read_event_lake(spark, app_config),
    }


def load_operational_dataframes(spark, config: AppConfig | None = None) -> dict[str, DataFrame]:
    app_config = ensure_data_directories(config or get_config())
    return {table_name: _read_operational_table(spark, app_config, table_name) for table_name in BRONZE_TABLES}


def persist_bronze_zone(logs: dict[str, DataFrame], tables: dict[str, DataFrame], config: AppConfig) -> dict[str, str]:
    outputs: dict[str, str] = {}
    for name, frame in logs.items():
        target = resolve_storage_uri(config.bronze_dir / 'logs' / name)
        frame.write.mode('overwrite').parquet(target)
        outputs[f'log_{name}'] = str(target)
    for name, frame in tables.items():
        target = resolve_storage_uri(config.bronze_dir / 'app_db' / name)
        frame.write.mode('overwrite').parquet(target)
        outputs[f'table_{name}'] = str(target)
    return outputs


def build_topic_metrics(log_df: DataFrame) -> DataFrame:
    return (
        log_df.filter(F.col('topic').isNotNull())
        .withColumn('topic', F.trim(F.col('topic')))
        .groupBy('topic')
        .agg(
            F.round(F.avg('score'), 2).alias('avg_score'),
            F.sum(F.when(F.col('score').isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias('attempts'),
            F.count('*').alias('events'),
            F.countDistinct('user_id').alias('unique_users'),
        )
        .withColumn(
            'difficulty_score',
            F.round((F.lit(100.0) - F.coalesce(F.col('avg_score'), F.lit(0.0))) * F.log(F.col('events') + F.lit(1.0)), 2),
        )
        .orderBy(F.desc('difficulty_score'), F.desc('attempts'), F.desc('events'))
    )


def build_silver_events(logs: dict[str, DataFrame], tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    def normalize_log(frame: DataFrame, event_type: str) -> DataFrame:
        return (
            frame.withColumn('event_timestamp', F.to_timestamp('timestamp'))
            .withColumn('event_type', F.lit(event_type))
            .withColumn('resource_id', F.coalesce(F.col('quiz_id'), F.lit(None).cast('string')))
            .withColumn('document_id', F.lit(None).cast('string'))
            .withColumn('duration_seconds', F.lit(0))
            .withColumn('engagement_score', F.lit(0.0))
            .select('user_id', 'event_timestamp', 'topic', 'action_type', 'event_type', 'score', 'resource_id', 'document_id', 'duration_seconds', 'engagement_score')
        )

    quiz_log_df = normalize_log(logs['quiz'], 'quiz_log')
    chat_log_df = normalize_log(logs['chat'], 'chat_log')
    activity_log_df = normalize_log(logs['activity'], 'activity_log')
    event_lake_df = normalize_log(logs['event_lake'], 'event_lake')

    db_event_df = (
        tables['events']
        .withColumn('event_timestamp', F.to_timestamp('created_at'))
        .withColumn('topic', F.coalesce(F.get_json_object('topics_json', '$[0]'), F.lit('general')))
        .withColumn('score', F.lit(None).cast('double'))
        .withColumn('document_id', F.lit(None).cast('string'))
        .select(
            F.col('user_id').cast('string').alias('user_id'),
            'event_timestamp', 'topic', F.col('activity_type').alias('action_type'), F.col('event_type').alias('event_type'), 'score',
            F.col('resource_id').cast('string').alias('resource_id'), 'document_id',
            F.coalesce(F.col('duration_seconds'), F.lit(0)).alias('duration_seconds'),
            F.coalesce(F.col('engagement_score'), F.lit(0.0)).alias('engagement_score'),
        )
    )

    study_df = (
        tables['study_sessions']
        .withColumn('event_timestamp', F.to_timestamp('created_at'))
        .withColumn('action_type', F.lit('study_session'))
        .withColumn('event_type', F.lit('study_session'))
        .withColumn('score', F.lit(None).cast('double'))
        .select(
            F.col('user_id').cast('string').alias('user_id'), 'event_timestamp', 'topic', 'action_type', 'event_type', 'score',
            F.col('id').cast('string').alias('resource_id'), F.col('document_id').cast('string').alias('document_id'),
            (F.col('time_spent') * F.lit(60)).cast('int').alias('duration_seconds'), F.coalesce(F.col('engagement_score'), F.lit(0.0)).alias('engagement_score'),
        )
    )

    quiz_df = (
        tables['quiz_results']
        .withColumn('event_timestamp', F.to_timestamp('created_at'))
        .withColumn('action_type', F.lit('quiz_result'))
        .withColumn('event_type', F.lit('quiz_result'))
        .select(
            F.col('user_id').cast('string').alias('user_id'), 'event_timestamp', 'topic', 'action_type', 'event_type',
            F.col('score_percent').cast('double').alias('score'), F.col('id').cast('string').alias('resource_id'),
            F.col('document_id').cast('string').alias('document_id'), F.lit(0).alias('duration_seconds'), F.lit(0.0).alias('engagement_score'),
        )
    )

    chat_messages_df = (
        tables['chat_messages']
        .join(
            tables['chat_sessions'].select(
                F.col('id').alias('session_join_id'), F.col('topic').alias('session_topic'), F.col('document_id').alias('session_document_id')
            ),
            tables['chat_messages']['session_id'] == F.col('session_join_id'),
            'left',
        )
        .withColumn('event_timestamp', F.to_timestamp('created_at'))
        .withColumn('action_type', F.lit('chat_message'))
        .withColumn('event_type', F.lit('chat_message'))
        .withColumn('score', F.col('confidence_score').cast('double'))
        .select(
            F.col('user_id').cast('string').alias('user_id'), 'event_timestamp', F.coalesce(F.col('session_topic'), F.lit('general')).alias('topic'),
            'action_type', 'event_type', 'score', F.col('id').cast('string').alias('resource_id'), F.col('session_document_id').cast('string').alias('document_id'),
            F.lit(0).alias('duration_seconds'), F.lit(0.0).alias('engagement_score'),
        )
    )

    unified_events = quiz_log_df.unionByName(chat_log_df).unionByName(activity_log_df).unionByName(event_lake_df).unionByName(db_event_df).unionByName(study_df).unionByName(quiz_df).unionByName(chat_messages_df)

    documents_silver = (
        tables['documents']
        .withColumn('created_ts', F.to_timestamp('created_at'))
        .withColumn('updated_ts', F.to_timestamp('updated_at'))
        .select(
            F.col('id').cast('string').alias('document_id'), F.col('user_id').cast('string').alias('user_id'), 'filename', 'file_type', 'topic', 'language', 'usage_count',
            F.length('text_content').alias('content_characters'), F.size(F.split(F.trim('text_content'), '\\s+')).alias('content_words'), 'created_ts', 'updated_ts',
        )
    )

    return {'events': unified_events, 'documents': documents_silver}


def _cluster_students(recommendation_features: DataFrame) -> DataFrame:
    if KMeans is None or VectorAssembler is None or recommendation_features.rdd.isEmpty():
        return recommendation_features.withColumn('cluster_id', F.lit(-1))

    feature_df = recommendation_features.fillna({'avg_score': 0.0, 'interaction_count': 0, 'time_spent_seconds': 0})
    assembler = VectorAssembler(inputCols=['interaction_count', 'avg_score', 'time_spent_seconds'], outputCol='features')
    assembled = assembler.transform(feature_df)
    try:
        model = KMeans(k=3, seed=42, featuresCol='features', predictionCol='cluster_id').fit(assembled)
        return model.transform(assembled).drop('features')
    except Exception:
        return feature_df.withColumn('cluster_id', F.lit(-1))


def _predict_scores(recommendation_features: DataFrame) -> DataFrame:
    filled = recommendation_features.fillna({'avg_score': 0.0, 'interaction_count': 0, 'time_spent_seconds': 0})
    if LinearRegression is None or VectorAssembler is None or filled.count() < 4:
        return filled.withColumn('predicted_score', F.round(F.col('avg_score') * F.lit(0.75) + F.log(F.col('interaction_count') + F.lit(1.0)) * F.lit(6.0), 2))

    assembler = VectorAssembler(inputCols=['interaction_count', 'time_spent_seconds'], outputCol='features')
    assembled = assembler.transform(filled)
    try:
        model = LinearRegression(featuresCol='features', labelCol='avg_score', predictionCol='predicted_score').fit(assembled)
        return model.transform(assembled).drop('features')
    except Exception:
        return filled.withColumn('predicted_score', F.round(F.col('avg_score') * F.lit(0.75) + F.log(F.col('interaction_count') + F.lit(1.0)) * F.lit(6.0), 2))


def build_gold_tables(silver: dict[str, DataFrame]) -> dict[str, DataFrame]:
    events = silver['events'].filter(F.col('event_timestamp').isNotNull())
    documents = silver['documents']

    topic_metrics = build_topic_metrics(events)
    user_engagement = (
        events.groupBy('user_id')
        .agg(
            F.count('*').alias('total_events'),
            F.round(F.avg('score'), 2).alias('avg_score'),
            F.round(F.avg('engagement_score'), 2).alias('avg_engagement'),
            F.sum('duration_seconds').alias('total_duration_seconds'),
            F.countDistinct('topic').alias('topics_covered'),
        )
        .withColumn('engagement_band', F.when(F.col('total_events') >= 100, F.lit('high')).when(F.col('total_events') >= 25, F.lit('medium')).otherwise(F.lit('emerging')))
        .orderBy(F.desc('total_events'), F.desc('avg_score'))
    )

    daily_activity = (
        events.withColumn('event_date', F.to_date('event_timestamp'))
        .groupBy('event_date')
        .agg(F.count('*').alias('events'), F.countDistinct('user_id').alias('active_users'), F.round(F.avg('score'), 2).alias('avg_score'), F.countDistinct('topic').alias('topics_touched'))
        .orderBy('event_date')
    )

    hourly_activity = (
        events.withColumn('event_hour', F.date_format('event_timestamp', 'yyyy-MM-dd HH:00:00'))
        .groupBy('event_hour', 'event_type')
        .agg(F.count('*').alias('events'), F.countDistinct('user_id').alias('active_users'))
        .orderBy('event_hour')
    )

    document_usage = (
        documents.groupBy('topic')
        .agg(F.countDistinct('document_id').alias('documents'), F.sum('usage_count').alias('total_usage'), F.round(F.avg('content_words'), 2).alias('avg_words'))
        .orderBy(F.desc('total_usage'), F.desc('documents'))
    )

    recommendation_features = (
        events.groupBy('user_id', 'topic')
        .agg(
            F.count('*').alias('interaction_count'),
            F.round(F.avg('score'), 2).alias('avg_score'),
            F.sum('duration_seconds').alias('time_spent_seconds'),
        )
        .withColumn('recommend_revision', F.when((F.col('avg_score') < 60) | (F.col('interaction_count') < 2), F.lit(1)).otherwise(F.lit(0)))
        .withColumn('skill_band', F.when(F.col('avg_score') >= 80, F.lit('strong')).when(F.col('avg_score') >= 60, F.lit('steady')).otherwise(F.lit('weak')))
    )

    student_clusters = _cluster_students(recommendation_features)
    performance_predictions = _predict_scores(recommendation_features)

    learning_recommendations = (
        topic_metrics.withColumn(
            'recommendation',
            F.when(F.col('difficulty_score') >= 90, F.lit('Prioritize revision content and easier follow-up quiz.'))
            .when(F.col('avg_score') < 60, F.lit('Schedule targeted review and a medium quiz.'))
            .otherwise(F.lit('Maintain progress with advanced practice.')),
        )
        .select('topic', 'avg_score', 'attempts', 'difficulty_score', 'recommendation')
    )

    return {
        'topic_metrics': topic_metrics,
        'user_engagement': user_engagement,
        'daily_activity': daily_activity,
        'hourly_activity': hourly_activity,
        'document_usage': document_usage,
        'recommendation_features': recommendation_features,
        'student_clusters': student_clusters,
        'performance_predictions': performance_predictions,
        'learning_recommendations': learning_recommendations,
    }


def _write_zone(zone: dict[str, DataFrame], root_path: Path) -> dict[str, str]:
    outputs: dict[str, str] = {}
    for name, frame in zone.items():
        target = resolve_storage_uri(root_path / name)
        frame.write.mode('overwrite').parquet(target)
        outputs[name] = str(target)
    return outputs


def run_batch_pipeline(config: AppConfig | None = None) -> dict[str, Any]:
    """Run an end-to-end Spark pipeline across the event lake and operational app database."""
    _ensure_spark_ready()
    app_config = ensure_data_directories(config or get_config())
    spark = None
    report_path = app_config.report_dir / 'end_to_end_big_data_report.json'
    started = time.perf_counter()

    try:
        spark = get_spark_session(app_config)
        logs = load_log_dataframes(spark, app_config)
        tables = load_operational_dataframes(spark, app_config)

        bronze_paths = persist_bronze_zone(logs, tables, app_config)
        silver = build_silver_events(logs, tables)
        silver_paths = _write_zone(silver, app_config.silver_dir)
        gold = build_gold_tables(silver)
        gold_paths = _write_zone(gold, app_config.gold_dir)

        total_log_records = sum(frame.count() for frame in logs.values())
        total_table_records = sum(frame.count() for frame in tables.values())
        total_processed = silver['events'].count()
        processing_seconds = round(time.perf_counter() - started, 2)
        records_per_second = round(total_processed / processing_seconds, 2) if processing_seconds > 0 else 0.0

        report = {
            'report_name': 'end_to_end_big_data_pipeline',
            'architecture': {
                'flow': 'User/App Events -> Raw Event Lake -> Spark Bronze -> Spark Silver -> Spark Gold -> Dashboard/Recommendations',
                'storage_backend': app_config.storage_backend,
                'data_lake_uri': app_config.data_lake_uri,
                'streaming_source': 'Kafka' if app_config.kafka_enabled else 'File-microbatch',
                'formats': {'raw': 'json', 'bronze': 'parquet', 'silver': 'parquet', 'gold': 'parquet'},
            },
            'source_paths': {
                'logs': str(app_config.logs_dir),
                'database': str(app_config.sqlite_db_path),
                'raw_event_lake': str(app_config.raw_events_dir),
                'raw_documents': str(app_config.raw_dir),
            },
            'bronze_paths': bronze_paths,
            'silver_paths': silver_paths,
            'gold_paths': gold_paths,
            'records_processed': int(total_processed),
            'log_records_ingested': int(total_log_records),
            'database_records_ingested': int(total_table_records),
            'processing_seconds': processing_seconds,
            'records_per_second': records_per_second,
            'scale_classification': 'large_scale' if total_processed >= 1_000_000 else 'medium_scale',
            'status': 'completed',
            'topic_metrics_preview': [row.asDict() for row in gold['topic_metrics'].limit(15).collect()],
            'user_engagement_preview': [row.asDict() for row in gold['user_engagement'].limit(15).collect()],
            'daily_activity_preview': [row.asDict() for row in gold['daily_activity'].limit(15).collect()],
            'student_clusters_preview': [row.asDict() for row in gold['student_clusters'].limit(15).collect()],
            'performance_predictions_preview': [row.asDict() for row in gold['performance_predictions'].limit(15).collect()],
            'learning_recommendations_preview': [row.asDict() for row in gold['learning_recommendations'].limit(15).collect()],
        }
    except Exception as exc:
        report = {
            'report_name': 'end_to_end_big_data_pipeline',
            'status': 'failed',
            'records_processed': 0,
            'processing_seconds': round(time.perf_counter() - started, 2),
            'error': str(exc),
            'architecture': {'flow': 'User/App Events -> Raw Event Lake -> Spark Bronze -> Spark Silver -> Spark Gold'},
        }
        report_path.write_text(json.dumps(report, indent=2), encoding='utf-8')
        raise RuntimeError(str(exc)) from exc
    finally:
        if spark is not None:
            spark.stop()

    report_path.write_text(json.dumps(report, indent=2), encoding='utf-8')
    return report
