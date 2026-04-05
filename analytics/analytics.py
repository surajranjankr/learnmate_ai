from __future__ import annotations

from datetime import datetime, timedelta
from math import log
from pathlib import Path
from typing import Any

import pandas as pd

from batch_processing.big_data_pipeline import build_topic_metrics, load_log_dataframes
from data_ingestion.data_logger import load_json_records
from learnmate_ai.config import AppConfig, get_config
from learnmate_ai.spark_manager import get_spark_session
from learnmate_ai.storage import ensure_data_directories

try:
    from pyspark.sql import functions as F
except Exception:
    F = None


def _load_gold_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def _load_local_logs(config: AppConfig) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    quiz_df = pd.DataFrame(load_json_records(config.logs_dir / 'quiz_logs.json'))
    chat_df = pd.DataFrame(load_json_records(config.logs_dir / 'chat_logs.json'))
    activity_df = pd.DataFrame(load_json_records(config.logs_dir / 'user_activity.json'))
    available_frames = [frame for frame in [quiz_df, chat_df, activity_df] if not frame.empty]
    combined_df = pd.concat(available_frames, ignore_index=True, sort=False) if available_frames else pd.DataFrame()
    if 'timestamp' in combined_df.columns:
        combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'], errors='coerce')
    return quiz_df, chat_df, activity_df, combined_df


def _fallback_dashboard_metrics(config: AppConfig, limit: int) -> dict[str, list[dict[str, Any]]]:
    quiz_df, _, activity_df, combined_df = _load_local_logs(config)
    if combined_df.empty:
        return {'hardest': [], 'weak': [], 'top': [], 'trend': [], 'recommendations': []}

    combined_df['topic'] = combined_df.get('topic', pd.Series(dtype=str)).fillna('general')
    combined_df['score_numeric'] = pd.to_numeric(combined_df.get('score'), errors='coerce')

    hardest = (
        combined_df.groupby('topic', dropna=False)
        .agg(avg_score=('score_numeric', 'mean'), attempts=('score_numeric', lambda series: int(series.notna().sum())), events=('topic', 'size'))
        .reset_index()
    )
    hardest['avg_score'] = hardest['avg_score'].fillna(0).round(2)
    hardest['difficulty_score'] = ((100 - hardest['avg_score']) * (hardest['events'] + 1).map(log)).round(2)
    hardest = hardest.sort_values(['difficulty_score', 'attempts', 'events'], ascending=False).head(limit)

    if quiz_df.empty:
        weak = pd.DataFrame(columns=['user_id', 'topic', 'avg_score', 'attempts'])
        top = pd.DataFrame(columns=['user_id', 'avg_score', 'attempts', 'best_score'])
    else:
        quiz_df['score_numeric'] = pd.to_numeric(quiz_df.get('score'), errors='coerce')
        weak = (
            quiz_df.groupby(['user_id', 'topic'], dropna=False)
            .agg(avg_score=('score_numeric', 'mean'), attempts=('topic', 'size'))
            .reset_index()
            .sort_values(['user_id', 'avg_score', 'attempts'], ascending=[True, True, False])
            .head(limit * 2)
        )
        top = (
            quiz_df.groupby('user_id', dropna=False)
            .agg(avg_score=('score_numeric', 'mean'), attempts=('topic', 'size'), best_score=('score_numeric', 'max'))
            .reset_index()
            .sort_values(['avg_score', 'best_score', 'attempts'], ascending=False)
            .head(limit)
        )
        weak[['avg_score']] = weak[['avg_score']].round(2)
        top[['avg_score', 'best_score']] = top[['avg_score', 'best_score']].round(2)

    trend = pd.DataFrame(columns=['event_date', 'events', 'active_users', 'avg_score'])
    if not combined_df.empty and 'timestamp' in combined_df.columns:
        temp_df = combined_df.dropna(subset=['timestamp']).copy()
        if not temp_df.empty:
            temp_df['event_date'] = temp_df['timestamp'].dt.date
            trend = (
                temp_df.groupby('event_date', dropna=False)
                .agg(events=('topic', 'size'), active_users=('user_id', pd.Series.nunique), avg_score=('score_numeric', 'mean'))
                .reset_index()
                .sort_values('event_date')
            )
            trend['avg_score'] = trend['avg_score'].fillna(0).round(2)

    recommendations = []
    for row in hardest.head(limit).to_dict(orient='records'):
        recommendations.append({
            'topic': row['topic'],
            'recommendation': 'Revise this topic first and retry a medium quiz.' if row['avg_score'] < 60 else 'Keep practicing with a harder quiz.',
            'difficulty_score': row['difficulty_score'],
        })

    return {
        'hardest': hardest.to_dict(orient='records'),
        'weak': weak.to_dict(orient='records'),
        'top': top.to_dict(orient='records'),
        'trend': trend.to_dict(orient='records'),
        'recommendations': recommendations,
    }


def _gold_dashboard_metrics(config: AppConfig, limit: int) -> dict[str, list[dict[str, Any]]] | None:
    hardest = _load_gold_frame(config.gold_dir / 'topic_metrics')
    weak = _load_gold_frame(config.gold_dir / 'recommendation_features')
    top = _load_gold_frame(config.gold_dir / 'user_engagement')
    trend = _load_gold_frame(config.gold_dir / 'daily_activity')
    recommendations = _load_gold_frame(config.gold_dir / 'learning_recommendations')
    if hardest.empty and weak.empty and top.empty and trend.empty and recommendations.empty:
        return None
    return {
        'hardest': hardest.head(limit).to_dict(orient='records'),
        'weak': weak.head(limit * 2).to_dict(orient='records'),
        'top': top.head(limit).to_dict(orient='records'),
        'trend': trend.to_dict(orient='records'),
        'recommendations': recommendations.head(limit).to_dict(orient='records'),
    }


def dashboard_metrics(config: AppConfig | None = None, limit: int = 10) -> dict[str, list[dict[str, Any]]]:
    """Return analytics sections from gold datasets first, Spark second, local logs last."""
    app_config = ensure_data_directories(config or get_config())
    gold_metrics = _gold_dashboard_metrics(app_config, limit)
    if gold_metrics is not None:
        return gold_metrics
    if F is None:
        return _fallback_dashboard_metrics(app_config, limit)

    spark = None
    try:
        spark = get_spark_session(app_config)
        logs = load_log_dataframes(spark, app_config)
        combined_df = logs['quiz'].unionByName(logs['chat'], allowMissingColumns=True).unionByName(logs['activity'], allowMissingColumns=True).unionByName(logs['event_lake'], allowMissingColumns=True)

        hardest_df = build_topic_metrics(combined_df)
        weak_df = (
            logs['quiz'].filter(F.col('user_id').isNotNull() & F.col('topic').isNotNull())
            .groupBy('user_id', 'topic')
            .agg(F.round(F.avg('score'), 2).alias('avg_score'), F.count('*').alias('attempts'))
            .orderBy('user_id', 'avg_score', F.desc('attempts'))
        )
        top_df = (
            logs['quiz'].filter(F.col('user_id').isNotNull())
            .groupBy('user_id')
            .agg(F.round(F.avg('score'), 2).alias('avg_score'), F.count('*').alias('attempts'), F.round(F.max('score'), 2).alias('best_score'))
            .orderBy(F.desc('avg_score'), F.desc('best_score'), F.desc('attempts'))
        )
        trend_df = (
            combined_df.withColumn('event_timestamp', F.to_timestamp('timestamp'))
            .withColumn('event_date', F.to_date('event_timestamp'))
            .filter(F.col('event_date').isNotNull())
            .groupBy('event_date')
            .agg(F.count('*').alias('events'), F.countDistinct('user_id').alias('active_users'), F.round(F.avg('score'), 2).alias('avg_score'))
            .orderBy('event_date')
        )
        recommendations_df = hardest_df.withColumn(
            'recommendation',
            F.when(F.col('avg_score') < 60, F.lit('Revise this topic first and retry a medium quiz.')).otherwise(F.lit('Keep practicing with a harder quiz.')),
        )
        return {
            'hardest': [row.asDict() for row in hardest_df.limit(limit).collect()],
            'weak': [row.asDict() for row in weak_df.limit(limit * 2).collect()],
            'top': [row.asDict() for row in top_df.limit(limit).collect()],
            'trend': [row.asDict() for row in trend_df.collect()],
            'recommendations': [row.asDict() for row in recommendations_df.limit(limit).collect()],
        }
    except Exception:
        return _fallback_dashboard_metrics(app_config, limit)
    finally:
        if spark is not None:
            spark.stop()


def hardest_topics(config: AppConfig | None = None, limit: int = 10) -> list[dict[str, Any]]:
    return dashboard_metrics(config, limit)['hardest']


def weak_areas_per_user(config: AppConfig | None = None, limit: int = 20) -> list[dict[str, Any]]:
    return dashboard_metrics(config, max(limit, 10))['weak'][:limit]


def top_performing_students(config: AppConfig | None = None, limit: int = 10) -> list[dict[str, Any]]:
    return dashboard_metrics(config, limit)['top']


def trend_analysis(config: AppConfig | None = None) -> list[dict[str, Any]]:
    return dashboard_metrics(config, 10)['trend']


def recent_user_history(user_id: str, config: AppConfig | None = None) -> dict[str, list[dict[str, Any]]]:
    """Return recent activities and quiz scores for the selected user."""
    app_config = ensure_data_directories(config or get_config())
    quiz_df, _, activity_df, combined_df = _load_local_logs(app_config)
    user_id = str(user_id)

    if combined_df.empty:
        return {'recent_activity': [], 'quiz_history': [], 'yesterday_topics': []}

    combined_df['user_id'] = combined_df.get('user_id', pd.Series(dtype=str)).astype(str)
    user_events = combined_df[combined_df['user_id'] == user_id].copy()
    if 'timestamp' in user_events.columns:
        user_events = user_events.sort_values('timestamp', ascending=False)

    recent_activity = user_events[[column for column in ['timestamp', 'topic', 'action_type', 'score'] if column in user_events.columns]].head(10)

    quiz_history = pd.DataFrame()
    if not quiz_df.empty:
        quiz_df['user_id'] = quiz_df.get('user_id', pd.Series(dtype=str)).astype(str)
        quiz_history = quiz_df[quiz_df['user_id'] == user_id].copy()
        if 'timestamp' in quiz_history.columns:
            quiz_history['timestamp'] = pd.to_datetime(quiz_history['timestamp'], errors='coerce')
            quiz_history = quiz_history.sort_values('timestamp', ascending=False)
        quiz_history = quiz_history[[column for column in ['timestamp', 'topic', 'score', 'question_count'] if column in quiz_history.columns]].head(10)

    yesterday_topics: list[dict[str, Any]] = []
    if not activity_df.empty:
        activity_df['user_id'] = activity_df.get('user_id', pd.Series(dtype=str)).astype(str)
        activity_df['timestamp'] = pd.to_datetime(activity_df.get('timestamp'), errors='coerce')
        yesterday = datetime.now().date() - timedelta(days=1)
        yesterday_df = activity_df[(activity_df['user_id'] == user_id) & (activity_df['timestamp'].dt.date == yesterday)]
        if not yesterday_df.empty:
            yesterday_topics = yesterday_df[[column for column in ['timestamp', 'topic', 'action_type', 'score'] if column in yesterday_df.columns]].to_dict(orient='records')

    return {
        'recent_activity': recent_activity.to_dict(orient='records'),
        'quiz_history': quiz_history.to_dict(orient='records'),
        'yesterday_topics': yesterday_topics,
    }
