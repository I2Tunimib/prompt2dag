from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DATA_DIR = os.environ.get('DATA_DIR', '/tmp/data')

with DAG(
    'multilingual_product_review_analysis',
    default_args=default_args,
    description='Pipeline for multilingual product review analysis',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def load_and_modify_data():
        # Placeholder for actual data loading and modification logic
        pass

    load_and_modify = PythonOperator(
        task_id='load_and_modify_data',
        python_callable=load_and_modify_data,
        op_kwargs={
            'input_file': f'{DATA_DIR}/reviews.csv',
            'output_file': f'{DATA_DIR}/table_data_2.json',
            'dataset_id': 2,
            'date_column': 'submission_date',
            'table_name_prefix': 'JOT_',
        },
    )

    language_detection = DockerOperator(
        task_id='language_detection',
        image='jmockit/language-detection',
        api_version='auto',
        auto_remove=True,
        environment={
            'TEXT_COLUMN': 'review_text',
            'LANG_CODE_COLUMN': 'language_code',
            'OUTPUT_FILE': 'lang_detected_2.json',
        },
        volumes=[f'{DATA_DIR}:/data'],
        command='python detect_language.py --input /data/table_data_2.json --output /data/lang_detected_2.json',
    )

    sentiment_analysis = DockerOperator(
        task_id='sentiment_analysis',
        image='huggingface/transformers-inference',
        api_version='auto',
        auto_remove=True,
        environment={
            'MODEL_NAME': 'distilbert-base-uncased-finetuned-sst-2-english',
            'TEXT_COLUMN': 'review_text',
            'OUTPUT_COLUMN': 'sentiment_score',
        },
        volumes=[f'{DATA_DIR}:/data'],
        command='python sentiment_analysis.py --input /data/lang_detected_2.json --output /data/sentiment_analyzed_2.json',
    )

    category_extraction = DockerOperator(
        task_id='category_extraction',
        image='i2t-backendwithintertwino6-column-extension:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'EXTENDER_ID': 'featureExtractor',
            'TEXT_COLUMN': 'review_text',
            'OUTPUT_COLUMN': 'mentioned_features',
        },
        volumes=[f'{DATA_DIR}:/data'],
        command='python category_extraction.py --input /data/sentiment_analyzed_2.json --output /data/column_extended_2.json',
    )

    save_final_data = DockerOperator(
        task_id='save_final_data',
        image='i2t-backendwithintertwino6-save:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'DATASET_ID': 2,
        },
        volumes=[f'{DATA_DIR}:/data'],
        command='python save_data.py --input /data/column_extended_2.json --output /data/enriched_data_2.csv',
    )

    load_and_modify >> language_detection >> sentiment_analysis >> category_extraction >> save_final_data