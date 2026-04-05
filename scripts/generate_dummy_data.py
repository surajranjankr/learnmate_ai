from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import json
import random
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from learnmate_ai.config import get_config
from learnmate_ai.storage import ensure_data_directories


TOPICS = [
    "machine learning",
    "data engineering",
    "apache spark",
    "sql analytics",
    "statistics",
    "python",
]
ACTIONS = ["page_view", "summary_requested", "dataset_uploaded", "quiz_attempt", "chat_message"]


def _write_records(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record) + "\n")


def _timestamp(days_back: int) -> str:
    dt = datetime.now(UTC) - timedelta(days=random.randint(0, days_back), minutes=random.randint(0, 1439))
    return dt.isoformat(timespec="seconds")


def generate_dummy_data(users: int, quiz_attempts: int, chats: int, activities: int) -> dict[str, Path]:
    config = ensure_data_directories(get_config())
    quiz_records = []
    chat_records = []
    activity_records = []

    for index in range(quiz_attempts):
        topic = random.choice(TOPICS)
        quiz_records.append(
            {
                "user_id": str(random.randint(1, users)),
                "timestamp": _timestamp(20),
                "topic": topic,
                "action_type": "quiz_attempt",
                "score": round(random.uniform(35, 98), 2),
                "quiz_id": f"quiz-{index + 1}",
                "question_count": random.randint(3, 10),
            }
        )

    for index in range(chats):
        topic = random.choice(TOPICS)
        chat_records.append(
            {
                "user_id": str(random.randint(1, users)),
                "timestamp": _timestamp(20),
                "topic": topic,
                "action_type": "chat_message",
                "score": None,
                "question": f"How does {topic} work in practice?",
                "response_preview": f"A generated explanation for {topic} interaction {index + 1}.",
            }
        )

    for _ in range(activities):
        topic = random.choice(TOPICS)
        activity_records.append(
            {
                "user_id": str(random.randint(1, users)),
                "timestamp": _timestamp(20),
                "topic": topic,
                "action_type": random.choice(ACTIONS),
                "score": round(random.uniform(50, 100), 2) if random.random() > 0.6 else None,
                "metadata": {"source": "dummy_generator", "device": random.choice(["web", "mobile"])} ,
            }
        )

    quiz_path = config.logs_dir / "quiz_logs.json"
    chat_path = config.logs_dir / "chat_logs.json"
    activity_path = config.logs_dir / "user_activity.json"
    _write_records(quiz_path, quiz_records)
    _write_records(chat_path, chat_records)
    _write_records(activity_path, activity_records)
    return {
        "quiz_logs": quiz_path,
        "chat_logs": chat_path,
        "user_activity": activity_path,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate dummy LearnMate logs for Spark analytics.")
    parser.add_argument("--users", type=int, default=25)
    parser.add_argument("--quiz-attempts", type=int, default=120)
    parser.add_argument("--chats", type=int, default=80)
    parser.add_argument("--activities", type=int, default=160)
    args = parser.parse_args()
    print(generate_dummy_data(args.users, args.quiz_attempts, args.chats, args.activities))


if __name__ == "__main__":
    main()
