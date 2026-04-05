from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import json
import random
from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from learnmate_ai.config import get_config
from learnmate_ai.storage import append_event_to_lake, ensure_data_directories


TOPICS = [
    'Operating Systems', 'DBMS', 'Computer Networks', 'Data Structures', 'Algorithms',
    'Machine Learning', 'Big Data', 'Cloud Computing', 'Software Engineering', 'Python',
]
ACTIONS = ['document_uploaded', 'summary_requested', 'quiz_attempt', 'chat_message', 'study_session']


def _payload(user_id: int, timestamp: datetime, topic: str, action_type: str) -> dict[str, object]:
    score = None
    if action_type == 'quiz_attempt':
        score = round(random.uniform(35, 98), 2)
    return {
        'user_id': str(user_id),
        'timestamp': timestamp.isoformat(timespec='seconds'),
        'topic': topic,
        'action_type': action_type,
        'score': score,
        'metadata': {
            'duration_seconds': random.randint(30, 1800),
            'engagement_score': round(random.uniform(0.2, 1.0), 2),
            'skill_level': random.choice(['beginner', 'intermediate', 'advanced']),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description='Generate a large-scale event lake dataset for Spark processing.')
    parser.add_argument('--records', type=int, default=1000000, help='Number of event records to generate')
    parser.add_argument('--users', type=int, default=5000, help='Number of unique users to simulate')
    parser.add_argument('--chunk-size', type=int, default=50000, help='Parquet write chunk size')
    args = parser.parse_args()

    config = ensure_data_directories(get_config())
    start = datetime.now(UTC) - timedelta(days=120)
    parquet_dir = config.curated_events_dir / 'synthetic_scale_dataset'
    parquet_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, object]] = []
    parquet_index = 0
    total_written = 0

    for index in range(args.records):
        event_time = start + timedelta(seconds=index * 7)
        row = _payload(
            user_id=random.randint(1, args.users),
            timestamp=event_time,
            topic=random.choice(TOPICS),
            action_type=random.choice(ACTIONS),
        )
        append_event_to_lake(config, 'synthetic_activity', row)
        rows.append(row)

        if len(rows) >= args.chunk_size:
            pd.DataFrame(rows).to_parquet(parquet_dir / f'part-{parquet_index:05d}.parquet', index=False)
            total_written += len(rows)
            rows = []
            parquet_index += 1

    if rows:
        pd.DataFrame(rows).to_parquet(parquet_dir / f'part-{parquet_index:05d}.parquet', index=False)
        total_written += len(rows)

    summary = {
        'records_written': total_written,
        'users': args.users,
        'raw_event_lake': str(config.raw_events_dir),
        'parquet_dataset': str(parquet_dir),
    }
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
