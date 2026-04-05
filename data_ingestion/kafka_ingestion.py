from __future__ import annotations

import json
from typing import Any

from learnmate_ai.config import AppConfig, get_config

try:
    from kafka import KafkaProducer
except Exception:
    KafkaProducer = None


_producer_cache: dict[str, Any] = {}


def kafka_available() -> bool:
    return KafkaProducer is not None


def _get_producer(config: AppConfig) -> Any | None:
    if not config.kafka_enabled or KafkaProducer is None:
        return None
    cache_key = config.kafka_bootstrap_servers
    if cache_key in _producer_cache:
        return _producer_cache[cache_key]
    try:
        producer = KafkaProducer(
            bootstrap_servers=config.kafka_bootstrap_servers,
            value_serializer=lambda payload: json.dumps(payload, ensure_ascii=False).encode('utf-8'),
            linger_ms=25,
            acks='all',
        )
    except Exception:
        return None
    _producer_cache[cache_key] = producer
    return producer


def publish_event(topic: str, payload: dict[str, Any], config: AppConfig | None = None) -> bool:
    app_config = config or get_config()
    producer = _get_producer(app_config)
    if producer is None:
        return False
    try:
        producer.send(topic, payload)
        producer.flush(timeout=2)
        return True
    except Exception:
        return False
