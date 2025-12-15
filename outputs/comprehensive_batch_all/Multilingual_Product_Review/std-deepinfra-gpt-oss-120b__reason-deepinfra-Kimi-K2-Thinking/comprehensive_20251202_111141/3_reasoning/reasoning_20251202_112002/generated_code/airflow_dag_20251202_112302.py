import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

DATA_DIR = os.getenv('DATA_DIR', '/opt/airflow/data')

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='multilingual_product_review_analysis',
    default_args=default_args,
    description='Enriches product reviews with language verification, sentiment, and feature extraction',
    schedule_interval=None,
    catchup=False,
    tags=['product_reviews', 'nlp', 'llm', 'multilingual'],
) as dag:
    
    load_modify_data = DockerOperator(
        task_id='load_modify_data',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        api_version='auto',
        auto_remove=True,
        network_mode='app_network',
        mounts=[{'type': 'bind', 'source': DATA_DIR, 'target': '/data', 'read_only': False}],
        mount_tmp_dir=False,
        environment={
            'DATASET_ID': '2',
            'DATE_COLUMN': 'submission_date',
            'TABLE_NAME_PREFIX': 'JOT_',
            'INPUT_FILE': '/data/reviews.csv',
            'OUTPUT_FILE': '/data/table_data_2.json',
        },
        docker_url='unix://var/run/docker.sock',
        tty=True,
        force_pull=False,
    )
    
    language_detection = DockerOperator(
        task_id='language_detection',
        image='jmockit/language-detection:latest',
        api_version='auto',
        auto_remove=True,
        network_mode='app_network',
        mounts=[{'type': 'bind', 'source': DATA_DIR, 'target': '/data', 'read_only': False}],
        mount_tmp_dir=False,
        environment={
            'TEXT_COLUMN': 'review_text',
            'LANG_CODE_COLUMN': 'language_code',
            'INPUT_FILE': '/data/table_data_2.json',
            'OUTPUT_FILE': '/data/lang_detected_2.json',
        },
        docker_url='unix://var/run/docker.sock',
        tty=True,
        force_pull=False,
    )
    
    sentiment_analysis = DockerOperator(
        task_id='sentiment_analysis',
        image='huggingface/transformers-inference:latest',
        api_version='auto',
        auto_remove=True,
        network_mode='app_network',
        mounts=[{'type': 'bind', 'source': DATA_DIR, 'target': '/data', 'read_only': False}],
        mount_tmp_dir=False,
        environment={
            'MODEL_NAME': 'distilbert-base-uncased-finetuned-sst-2-english',
            'TEXT_COLUMN': 'review_text',
            'OUTPUT_COLUMN': 'sentiment_score',
            'INPUT_FILE': '/data/lang_detected_2.json',
            'OUTPUT_FILE': '/data/sentiment_analyzed_2.json',
        },
        docker_url='unix://var/run/docker.sock',
        tty=True,
        force_pull=False,
    )
    
    category_extraction = DockerOperator(
        task_id='category_extraction',
        image='i2t-backendwithintertwino6-column-extension:latest',
        api_version='auto',
        auto_remove=True,
        network_mode='app_network',
        mounts=[{'type': 'bind', 'source': DATA_DIR, 'target': '/data', 'read_only': False}],
        mount_tmp_dir=False,
        environment={
            'EXTENDER_ID': 'featureExtractor',
            'TEXT_COLUMN': 'review_text',
            'OUTPUT_COLUMN': 'mentioned_features',
            'INPUT_FILE': '/data/sentiment_analyzed_2.json',
            'OUTPUT_FILE': '/data/column_extended_2.json',
        },
        docker_url='unix://var/run/docker.sock',
        tty=True,
        force_pull=False,
    )
    
    save_final_data = DockerOperator(
        task_id='save_final_data',
        image='i2t-backendwithintertwino6-save:latest',
        api_version='auto',
        auto_remove=True,
        network_mode='app_network',
        mounts=[{'type': 'bind', 'source': DATA_DIR, 'target': '/data', 'read_only': False}],
        mount_tmp_dir=False,
        environment={
            'DATASET_ID': '2',
            'INPUT_FILE': '/data/column_extended_2.json',
            'OUTPUT_FILE': '/data/enriched_data_2.csv',
        },
        docker_url='unix://var/run/docker.sock',
        tty=True,
        force_pull=False,
    )
    
    load_modify_data >> language_detection >> sentiment_analysis >> category_extraction >> save_final_data