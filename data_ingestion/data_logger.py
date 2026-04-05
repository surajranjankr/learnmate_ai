from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

from data_ingestion.kafka_ingestion import publish_event
from learnmate_ai.config import AppConfig, get_config
from learnmate_ai.storage import append_event_to_lake, ensure_data_directories, timestamped_name


LOG_SCHEMAS = {
    'quiz_logs.json': ['user_id', 'timestamp', 'topic', 'action_type', 'score', 'quiz_id', 'question_count'],
    'chat_logs.json': ['user_id', 'timestamp', 'topic', 'action_type', 'score', 'question', 'response_preview'],
    'user_activity.json': ['user_id', 'timestamp', 'topic', 'action_type', 'score', 'metadata'],
}


def ensure_log_files(config: AppConfig | None = None) -> dict[str, Path]:
    """Create the log directory and required JSON log files."""
    app_config = ensure_data_directories(config or get_config())
    app_config.logs_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    for filename in LOG_SCHEMAS:
        path = app_config.logs_dir / filename
        if not path.exists():
            path.write_text('', encoding='utf-8')
        paths[filename] = path
    return paths


def _append_log(filename: str, payload: dict[str, Any], config: AppConfig | None = None) -> Path:
    app_config = ensure_data_directories(config or get_config())
    paths = ensure_log_files(app_config)
    path = paths[filename]
    with path.open('a', encoding='utf-8') as file:
        file.write(json.dumps(payload, ensure_ascii=True) + '\n')

    stream_event_path = app_config.streaming_input_dir / timestamped_name(filename)
    stream_event_path.write_text(json.dumps(payload, ensure_ascii=True), encoding='utf-8')

    event_type = filename.replace('.json', '').replace('_logs', '').replace('user_activity', 'activity')
    append_event_to_lake(app_config, event_type, payload)
    kafka_topic_map = {
        'quiz_logs.json': app_config.kafka_topic_quiz,
        'chat_logs.json': app_config.kafka_topic_chat,
        'user_activity.json': app_config.kafka_topic_user_activity,
    }
    publish_event(kafka_topic_map.get(filename, app_config.kafka_topic_user_activity), payload, app_config)
    return path


def _base_payload(user_id: int | str, topic: str, action_type: str, score: float | int | None = None) -> dict[str, Any]:
    return {
        'user_id': str(user_id),
        'timestamp': datetime.now(UTC).isoformat(timespec='seconds'),
        'topic': topic.strip() or 'general',
        'action_type': action_type.strip(),
        'score': None if score is None else float(score),
    }


def log_quiz_attempt(
    user_id: int | str,
    topic: str,
    score: float,
    question_count: int,
    quiz_id: str,
    config: AppConfig | None = None,
) -> Path:
    """Append a quiz attempt record to the quiz log."""
    payload = _base_payload(user_id, topic, 'quiz_attempt', score)
    payload['quiz_id'] = quiz_id
    payload['question_count'] = int(question_count)
    return _append_log('quiz_logs.json', payload, config)


def log_chat_event(
    user_id: int | str,
    topic: str,
    question: str,
    response_preview: str,
    config: AppConfig | None = None,
) -> Path:
    """Append a chatbot interaction record to the chat log."""
    payload = _base_payload(user_id, topic, 'chat_message')
    payload['question'] = question.strip()
    payload['response_preview'] = response_preview[:300]
    return _append_log('chat_logs.json', payload, config)


def log_user_activity(
    user_id: int | str,
    topic: str,
    action_type: str,
    metadata: dict[str, Any] | None = None,
    score: float | int | None = None,
    config: AppConfig | None = None,
) -> Path:
    """Append a generic user activity record to the activity log."""
    payload = _base_payload(user_id, topic, action_type, score)
    payload['metadata'] = metadata or {}
    return _append_log('user_activity.json', payload, config)


def load_json_records(path: Path) -> list[dict[str, Any]]:
    """Load newline-delimited JSON records from a log file."""
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if not line:
            continue
        records.append(json.loads(line))
    return records
