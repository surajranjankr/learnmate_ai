from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from batch_processing.big_data_pipeline import run_batch_pipeline
from database.database_manager import initialize_database_schema, persist_pipeline_report
from learnmate_ai.config import get_config


def main() -> None:
    """Run the end-to-end Spark pipeline over the event lake plus the operational app database."""
    parser = argparse.ArgumentParser(description='Run the LearnMate end-to-end Spark big data pipeline.')
    parser.add_argument('--persist-database', action='store_true', help='Persist pipeline metadata to the application database')
    parser.add_argument('--show-report', action='store_true', help='Print the generated pipeline report')
    parser.add_argument('--backfill-event-lake', action='store_true', help='Backfill historical DB events into the raw event lake before processing')
    args = parser.parse_args()

    if args.backfill_event_lake:
        subprocess.run([sys.executable, str(PROJECT_ROOT / 'scripts' / 'backfill_event_lake.py')], check=False)

    config = get_config()
    report = run_batch_pipeline(config)

    if args.show_report:
        print(report)

    if args.persist_database:
        initialize_database_schema(config)
        persisted = persist_pipeline_report(report, config)
        print(persisted)


if __name__ == '__main__':
    main()
