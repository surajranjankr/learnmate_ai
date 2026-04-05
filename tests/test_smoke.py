from __future__ import annotations

import json
from pathlib import Path
import shutil
import unittest

from data_ingestion.data_logger import ensure_log_files, load_json_records, log_chat_event, log_quiz_attempt, log_user_activity
from database.database_manager import (
    add_chat_message,
    authenticate_user,
    create_chat_session,
    database_status,
    get_documents_df,
    get_events_df,
    get_quiz_df,
    get_study_df,
    get_summary_df,
    get_users_df,
    get_or_create_document,
    initialize_database_schema,
    list_chat_messages,
    log_event,
    log_study_session,
    register_user,
    save_quiz_result,
    store_summary,
)
from learnmate_ai.config import AppConfig
from learnmate_ai.storage import ensure_data_directories
from modules import analytics, chatbot_rag, vectorstore


class UploadStub:
    def __init__(self, name: str, data: bytes):
        self.name = name
        self._data = data

    def getvalue(self) -> bytes:
        return self._data


class LearnMateSmokeTests(unittest.TestCase):
    def setUp(self):
        self.temp_root = Path('tests/.tmp_runtime')
        if self.temp_root.exists():
            shutil.rmtree(self.temp_root, ignore_errors=True)
        self.config = AppConfig(
            base_dir=self.temp_root,
            data_dir=self.temp_root / 'data',
            raw_dir=self.temp_root / 'data' / 'raw',
            bronze_dir=self.temp_root / 'data' / 'bronze',
            silver_dir=self.temp_root / 'data' / 'silver',
            gold_dir=self.temp_root / 'data' / 'gold',
            report_dir=self.temp_root / 'data' / 'reports',
            logs_dir=self.temp_root / 'data' / 'logs',
            streaming_input_dir=self.temp_root / 'data' / 'stream_input',
            streaming_output_dir=self.temp_root / 'data' / 'stream_output',
            checkpoint_dir=self.temp_root / 'data' / 'checkpoints',
            lakehouse_dir=self.temp_root / 'data' / 'lakehouse',
            raw_events_dir=self.temp_root / 'data' / 'lakehouse' / 'raw_events',
            curated_events_dir=self.temp_root / 'data' / 'lakehouse' / 'curated_events',
            model_features_dir=self.temp_root / 'data' / 'lakehouse' / 'model_features',
            kafka_checkpoint_dir=self.temp_root / 'data' / 'checkpoints' / 'kafka_streaming',
            sqlite_db_path=self.temp_root / 'data' / 'learnmate_test.db',
        )
        ensure_data_directories(self.config)
        initialize_database_schema(self.config)

    def tearDown(self):
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def test_load_structured_data_valid_csv(self):
        upload = UploadStub('sample.csv', b'category,value\nA,1\nB,2\n')
        df = analytics.load_structured_data(upload)
        self.assertEqual(len(df), 2)

    def test_blank_chat_question(self):
        response = chatbot_rag.chatbot_respond('   ')
        self.assertIn('Please enter', response['answer'])
        self.assertEqual(response['confidence'], 0.0)

    def test_vectorstore_writes_json_text_file(self):
        temp_dir = self.temp_root / 'vectorstore'
        temp_dir.mkdir(parents=True, exist_ok=True)
        index_path = str(temp_dir / 'vectordb')
        vectorstore.build_vectorstore(['alpha beta', 'gamma delta'], index_path=index_path)
        text_path = Path(f'{index_path}_texts.json')
        self.assertTrue(text_path.exists())
        with text_path.open('r', encoding='utf-8') as file:
            payload = json.load(file)
        self.assertEqual(payload, ['alpha beta', 'gamma delta'])

    def test_loggers_write_json_lines(self):
        ensure_log_files(self.config)
        log_quiz_attempt('7', 'python', 86.5, 5, 'quiz-1', self.config)
        log_chat_event('7', 'python', 'What is Spark?', 'Spark is a distributed engine.', self.config)
        log_user_activity('7', 'python', 'summary_requested', {'mode': 'brief'}, config=self.config)

        quiz_records = load_json_records(self.config.logs_dir / 'quiz_logs.json')
        chat_records = load_json_records(self.config.logs_dir / 'chat_logs.json')
        activity_records = load_json_records(self.config.logs_dir / 'user_activity.json')
        raw_event_files = list(self.config.raw_events_dir.rglob('*.json'))

        self.assertEqual(quiz_records[0]['topic'], 'python')
        self.assertEqual(chat_records[0]['action_type'], 'chat_message')
        self.assertEqual(activity_records[0]['metadata']['mode'], 'brief')
        self.assertGreaterEqual(len(raw_event_files), 3)

    def test_local_database_tracks_user_activity(self):
        user = register_user('Test User', 'test@example.com', 'password123', self.config)
        auth = authenticate_user('test@example.com', 'password123', self.config)
        document = get_or_create_document(user['user_id'], 'demo.pdf', '.pdf', 'B Tree', 'B Trees and B+ Trees', 'en', self.config)
        log_study_session(user['user_id'], 'Document Study', 'B Tree', 12, self.config, document_id=document['id'])
        save_quiz_result(user['user_id'], 'Document Study', 'B Tree', 4, 5, self.config, document_id=document['id'], difficulty_level='medium')
        store_summary(user['user_id'], document['id'], 'tfidf', 'brief', 'en', '- summary', [{'type': 'concept', 'text': 'B tree'}], {'document_level': ['B tree']}, self.config)
        session_id = create_chat_session(user['user_id'], 'Demo Chat', 'B Tree', self.config, document_id=document['id'])
        add_chat_message(session_id, user['user_id'], 'user', 'Explain B tree', self.config)
        add_chat_message(session_id, user['user_id'], 'assistant', 'B tree explanation', self.config, confidence_score=0.82)
        log_event(user['user_id'], 'summary_requested', {'topic': 'B Tree'}, self.config)

        users_df = get_users_df(self.config)
        documents_df = get_documents_df(self.config)
        study_df = get_study_df(self.config)
        quiz_df = get_quiz_df(self.config)
        summary_df = get_summary_df(self.config)
        events_df = get_events_df(config=self.config)
        messages = list_chat_messages(session_id, self.config)
        status = database_status(self.config)

        self.assertTrue(auth['signed_in'])
        self.assertEqual(len(users_df), 1)
        self.assertEqual(len(documents_df), 1)
        self.assertEqual(len(study_df), 1)
        self.assertEqual(len(quiz_df), 1)
        self.assertEqual(len(summary_df), 1)
        self.assertEqual(len(messages), 2)
        self.assertGreaterEqual(len(events_df), 3)
        self.assertTrue(status['connected'])
        self.assertTrue(status['database_configured'])


if __name__ == '__main__':
    unittest.main()
