from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from learnmate_ai.config import get_config
from learnmate_ai.storage import append_event_to_lake, ensure_data_directories


def main() -> None:
    parser = argparse.ArgumentParser(description='Backfill historical database events into the raw event lake.')
    parser.add_argument('--limit', type=int, default=5000, help='Maximum number of historical event rows to backfill')
    args = parser.parse_args()

    config = ensure_data_directories(get_config())
    connection = sqlite3.connect(config.sqlite_db_path)
    connection.row_factory = sqlite3.Row
    rows = connection.execute('SELECT * FROM events ORDER BY id DESC LIMIT ?', (args.limit,)).fetchall()
    written = 0
    for row in rows:
        payload = {
            'user_id': str(row['user_id'] or ''),
            'timestamp': row['created_at'],
            'topic': json.loads(row['topics_json'] or '[]')[0] if row['topics_json'] else 'general',
            'action_type': row['activity_type'],
            'score': None,
            'metadata': json.loads(row['event_data'] or '{}') if row['event_data'] else {},
        }
        append_event_to_lake(config, 'historical_activity', payload)
        written += 1
    print({'written': written, 'raw_events_dir': str(config.raw_events_dir)})


if __name__ == '__main__':
    main()
