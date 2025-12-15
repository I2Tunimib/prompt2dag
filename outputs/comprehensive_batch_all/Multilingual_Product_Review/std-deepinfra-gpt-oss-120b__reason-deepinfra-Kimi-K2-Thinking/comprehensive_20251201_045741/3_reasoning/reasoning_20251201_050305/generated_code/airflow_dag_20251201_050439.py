from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
import os

"""
Multilingual Product Review Analysis Pipeline

Enriches product reviews with language verification, sentiment analysis,
and key feature extraction using LLM capabilities for deeper customer insight.
"""

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='multilingual_product_review_analysis',
    default_args=default_args,
    description='Enriches product reviews with language verification, sentiment, and key feature extraction',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['reviews', 'nlp', 'multilingual'],
) as dag:
    
    DATA_DIR = os.getenv('DATA_DIR', '/opt/airflow/data')
    
    docker_defaults = {
        'api_version': 'auto',
        'auto_remove': True,
        'volumes': [f'{DATA_DIR}:{DATA_DIR}'],
        'network_mode': 'app_network',
        'working_dir': DATA_DIR,
    }
    
    load_and_modify = DockerOperator(
        task_id='load_and_modify_data',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        environment={
            'DATASET_ID': '2',
            'DATE_COLUMN': 'submission_date',
            'TABLE_NAME_PREFIX': 'JOT_',
            'INPUT_FILE': 'reviews.csv',
            'OUTPUT_FILE': 'table_data_2.json',
        },
        **docker_defaults,
    )
    
    language_detection = DockerOperator(
        task_id='detect_language',
        image='jmockit/language-detection',
        environment={
            'TEXT_COLUMN': 'review_text',
            'LANG_CODE_COLUMN': 'language_code',
            'INPUT_FILE': 'table_data_2.json',
            'OUTPUT_FILE': 'lang_detected_2.json',
        },
        **docker_defaults,
    )
    
    sentiment_analysis = DockerOperator(
        task_id='analyze_sentiment',
        image='huggingface/transformers-inference',
        environment={
            'MODEL_NAME': 'distilbert-base-uncased-finetuned-sst-2-english',
            'TEXT_COLUMN': 'review_text',
            'OUTPUT_COLUMN': 'sentiment_score',
            'INPUT_FILE': 'lang_detected_2.json',
            'OUTPUT_FILE': 'sentiment_analyzed_2.json',
        },
        **docker_defaults,
    )
    
    category_extraction = DockerOperator(
        task_id='extract_categories',
        image='i2t-backendwithintertwino6-column-extension:latest',
        environment={
            'EXTENDER_ID': 'featureExtractor',
            'TEXT_COLUMN': 'review_text',
            'OUTPUT_COLUMN': 'mentioned_features',
            'INPUT_FILE': 'sentiment_analyzed_2.json',
            'OUTPUT_FILE': 'column_extended_2.json',
        },
        **docker_defaults,
    )
    
    save_final_data = DockerOperator(
        task_id='save_enriched_data',
        image='i2t-backendwithintertwino6-save:latest',
        environment={
            'DATASET_ID': '2',
            'INPUT_FILE': 'column_extended_2.json',
            'OUTPUT_FILE': 'enriched_data_2.csv',
        },
        **docker_defaults,
    )
    
    load_and_modify >> language_detection >> sentiment_analysis >> category_extraction >> save_final_data