from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='multilingual_product_review_analysis',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def load_and_modify_data(**kwargs):
        import pandas as pd
        from datetime import datetime

        data_dir = kwargs['data_dir']
        dataset_id = kwargs['dataset_id']
        date_column = kwargs['date_column']
        table_name_prefix = kwargs['table_name_prefix']

        df = pd.read_csv(f"{data_dir}/reviews.csv")
        df[date_column] = pd.to_datetime(df[date_column], format='%Y%m%d')
        df[date_column] = df[date_column].dt.strftime('%Y-%m-%d')
        df.to_json(f"{data_dir}/{table_name_prefix}data_{dataset_id}.json", orient='records')

    load_and_modify = PythonOperator(
        task_id='load_and_modify_data',
        python_callable=load_and_modify_data,
        op_kwargs={
            'data_dir': '/data',
            'dataset_id': 2,
            'date_column': 'submission_date',
            'table_name_prefix': 'JOT_'
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
        volumes=['/data:/data'],
        network_mode='app_network',
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
        volumes=['/data:/data'],
        network_mode='app_network',
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
        volumes=['/data:/data'],
        network_mode='app_network',
    )

    save_final_data = DockerOperator(
        task_id='save_final_data',
        image='i2t-backendwithintertwino6-save:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'DATASET_ID': 2,
        },
        volumes=['/data:/data'],
        network_mode='app_network',
    )

    load_and_modify >> language_detection >> sentiment_analysis >> category_extraction >> save_final_data