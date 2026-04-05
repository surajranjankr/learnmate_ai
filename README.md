# 🧠 LearnMate AI – AI Study Assistant

LearnMate is now structured as an end-to-end local big-data learning analytics platform with Spark batch processing, Spark structured streaming, a partitioned event lake, and AI study features.

An offline AI Study Assistant that helps you learn better using:
✅ Summarizer (brief/detailed)
✅ Quiz generator (interactive with answers)
✅ RAG-based chatbot for document Q&A


# Project Creator:
Suraj Ranjan kumar (24BDS082)
Mohit (24BDS042)
Dharmendra Yadav (24BDS019)
Ravi Ranjan Bharti (24BDS065)
Rupesh Kumar (24BDS069)


### 🚀 Features:
- Powered by Mistral 7B GGUF (local LLM)
- Built with Streamlit + Python
- Everything runs locally without internet
- RAG chatbot using FAISS + sentence-transformers


## Architecture
`App/User Events -> Logs + Raw Event Lake (+ optional Kafka) -> Spark Bronze -> Spark Silver -> Spark Gold -> Analytics Dashboard + Recommendations`


## Core Big-Data Components

- Partitioned raw event lake under `data/lakehouse/raw_events`
- Spark batch pipeline over logs, event lake, and operational database
- Spark structured streaming over file micro-batches or Kafka topics
- Bronze / Silver / Gold outputs for metrics, engagement, and recommendation features
- Gold-layer analytics surfaced in the application analytics flow

## Main Folders

- `data_ingestion/data_logger.py`
- `data_ingestion/kafka_ingestion.py`
- `batch_processing/big_data_pipeline.py`
- `stream_processing/streaming_pipeline.py`
- `analytics/analytics.py`
- `database/database_manager.py`
- `scripts/backfill_event_lake.py`
- `scripts/run_big_data_pipeline.py`
- `app.py`


## Streaming Pipeline

The streaming pipeline reads from:
- Kafka when `KAFKA_ENABLED=true`
- file micro-batches in `data/stream_input` otherwise

## Notes

This repository is still a local/single-machine implementation, but the architecture now supports:
- large event ingestion patterns
- Spark distributed-style processing
- streaming ingestion
- partitioned event-lake storage
- analytics and recommendation outputs


## 📦 Model Instructions

This app uses Mistral-7B GGUF model, which is not included in the repo.
Download it manually from:

👉 [https://huggingface.co/TheBloke/Mistral-7B-Instruct-v0.1-GGUF](https://huggingface.co/TheBloke/Mistral-7B-Instruct-v0.1-GGUF)

### Recommended file:
- `mistral-7b-instruct-v0.1.Q4_K_M.gguf`

Save the file to the `models/` directory like so:


### 📦 How to Run
```bash
pip install -r requirements.txt
streamlit run app.py

## Backfill Historical Events

```bash
python scripts/backfill_event_lake.py --limit 5000
```

## Run Batch Pipeline

```bash
python scripts/run_big_data_pipeline.py --backfill-event-lake --show-report
```



PPT Presentation Link(Version 1):
https://drive.google.com/file/d/14v8V_ELjbGtw8SIi1k8RyGSB2G7Z2ni4/view?usp=sharing

PPT Presentation Link(Big Data - Version 2):
https://docs.google.com/presentation/d/15j2-XU0nFJFnNFFAwYk2wlh2sVMOqunX/edit?usp=sharing&ouid=109262647802171461229&rtpof=true&sd=true


Report Link(Version 1):
https://drive.google.com/file/d/1XqeNYPe2E7v_cm-u8WcW5zXGeBVw-EqU/view?usp=sharing

Report Link(Big Data - Version 2):
https://drive.google.com/file/d/11dNo_OAY1v6TZPpp0N2b4NTCgjUjCpgU/view?usp=sharing