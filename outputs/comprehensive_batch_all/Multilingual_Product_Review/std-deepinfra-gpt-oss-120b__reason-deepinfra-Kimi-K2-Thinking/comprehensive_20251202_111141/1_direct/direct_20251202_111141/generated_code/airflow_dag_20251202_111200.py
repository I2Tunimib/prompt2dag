import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

# Directory where data files are stored; can be overridden by environment variable
DATA_DIR = os.getenv("DATA_DIR", "/tmp/data")

# Common Docker mount configuration
DATA_MOUNT = Mount(source=DATA_DIR, target="/data", type="bind")

with DAG(
    dag_id="multilingual_product_review_analysis",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["review", "nlp", "multilingual"],
    doc_md=__doc__,
) as dag:
    # Wait for the source CSV file to be present
    wait_for_reviews_csv = FileSensor(
        task_id="wait_for_reviews_csv",
        filepath=os.path.join(DATA_DIR, "reviews.csv"),
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=600,
        mode="poke",
    )

    # 1. Load & Modify Data
    load_and_modify = DockerOperator(
        task_id="load_and_modify",
        image="i2t-backendwithintertwino6-load-and-modify:latest",
        command="python /app/load_modify.py",
        mounts=[DATA_MOUNT],
        environment={
            "DATASET_ID": "2",
            "DATE_COLUMN": "submission_date",
            "TABLE_NAME_PREFIX": "JOT_",
            "INPUT_FILE": "/data/reviews.csv",
            "OUTPUT_FILE": "/data/table_data_2.json",
        },
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="app_network",
    )

    # 2. Language Detection
    language_detection = DockerOperator(
        task_id="language_detection",
        image="jmockit/language-detection:latest",
        command="python /app/language_detect.py",
        mounts=[DATA_MOUNT],
        environment={
            "INPUT_FILE": "/data/table_data_2.json",
            "OUTPUT_FILE": "/data/lang_detected_2.json",
            "TEXT_COLUMN": "review_text",
            "LANG_CODE_COLUMN": "language_code",
        },
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="app_network",
    )

    # 3. Sentiment Analysis
    sentiment_analysis = DockerOperator(
        task_id="sentiment_analysis",
        image="huggingface/transformers-inference:latest",
        command="python /app/sentiment.py",
        mounts=[DATA_MOUNT],
        environment={
            "INPUT_FILE": "/data/lang_detected_2.json",
            "OUTPUT_FILE": "/data/sentiment_analyzed_2.json",
            "MODEL_NAME": "distilbert-base-uncased-finetuned-sst-2-english",
            "TEXT_COLUMN": "review_text",
            "OUTPUT_COLUMN": "sentiment_score",
        },
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="app_network",
    )

    # 4. Category Extraction
    category_extraction = DockerOperator(
        task_id="category_extraction",
        image="i2t-backendwithintertwino6-column-extension:latest",
        command="python /app/column_extend.py",
        mounts=[DATA_MOUNT],
        environment={
            "INPUT_FILE": "/data/sentiment_analyzed_2.json",
            "OUTPUT_FILE": "/data/column_extended_2.json",
            "EXTENDER_ID": "featureExtractor",
            "TEXT_COLUMN": "review_text",
            "OUTPUT_COLUMN": "mentioned_features",
        },
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="app_network",
    )

    # 5. Save Final Data
    save_final = DockerOperator(
        task_id="save_final",
        image="i2t-backendwithintertwino6-save:latest",
        command="python /app/save.py",
        mounts=[DATA_MOUNT],
        environment={
            "INPUT_FILE": "/data/column_extended_2.json",
            "OUTPUT_FILE": "/data/enriched_data_2.csv",
            "DATASET_ID": "2",
        },
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="app_network",
    )

    # Define task dependencies
    wait_for_reviews_csv >> load_and_modify >> language_detection >> sentiment_analysis >> category_extraction >> save_final