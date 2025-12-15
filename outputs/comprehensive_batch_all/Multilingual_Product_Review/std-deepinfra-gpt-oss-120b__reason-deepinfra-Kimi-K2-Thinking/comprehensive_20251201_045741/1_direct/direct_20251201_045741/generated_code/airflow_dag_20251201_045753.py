import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DATA_DIR = os.getenv("DATA_DIR", "/tmp/data")
VOLUME_MOUNT = f"{DATA_DIR}:/data"

with DAG(
    dag_id="multilingual_product_review_analysis",
    description="Enrich product reviews with language detection, sentiment, and feature extraction",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["review", "nlp", "llm"],
) as dag:

    load_and_modify = DockerOperator(
        task_id="load_and_modify_data",
        image="i2t-backendwithintertwino6-load-and-modify:latest",
        api_version="auto",
        auto_remove=True,
        command="",
        environment={
            "DATASET_ID": "2",
            "DATE_COLUMN": "submission_date",
            "TABLE_NAME_PREFIX": "JOT_",
        },
        volumes=[VOLUME_MOUNT],
        network_mode="app_network",
        docker_url="unix://var/run/docker.sock",
        tty=True,
    )

    language_detection = DockerOperator(
        task_id="language_detection",
        image="jmockit/language-detection:latest",
        api_version="auto",
        auto_remove=True,
        command="",
        environment={
            "TEXT_COLUMN": "review_text",
            "LANG_CODE_COLUMN": "language_code",
            "OUTPUT_FILE": "lang_detected_2.json",
        },
        volumes=[VOLUME_MOUNT],
        network_mode="app_network",
        docker_url="unix://var/run/docker.sock",
        tty=True,
    )

    sentiment_analysis = DockerOperator(
        task_id="sentiment_analysis",
        image="huggingface/transformers-inference:latest",
        api_version="auto",
        auto_remove=True,
        command="",
        environment={
            "MODEL_NAME": "distilbert-base-uncased-finetuned-sst-2-english",
            "TEXT_COLUMN": "review_text",
            "OUTPUT_COLUMN": "sentiment_score",
        },
        volumes=[VOLUME_MOUNT],
        network_mode="app_network",
        docker_url="unix://var/run/docker.sock",
        tty=True,
    )

    category_extraction = DockerOperator(
        task_id="category_extraction",
        image="i2t-backendwithintertwino6-column-extension:latest",
        api_version="auto",
        auto_remove=True,
        command="",
        environment={
            "EXTENDER_ID": "featureExtractor",
            "TEXT_COLUMN": "review_text",
            "OUTPUT_COLUMN": "mentioned_features",
        },
        volumes=[VOLUME_MOUNT],
        network_mode="app_network",
        docker_url="unix://var/run/docker.sock",
        tty=True,
    )

    save_final_data = DockerOperator(
        task_id="save_final_data",
        image="i2t-backendwithintertwino6-save:latest",
        api_version="auto",
        auto_remove=True,
        command="",
        environment={
            "DATASET_ID": "2",
        },
        volumes=[VOLUME_MOUNT],
        network_mode="app_network",
        docker_url="unix://var/run/docker.sock",
        tty=True,
    )

    load_and_modify >> language_detection >> sentiment_analysis >> category_extraction >> save_final_data