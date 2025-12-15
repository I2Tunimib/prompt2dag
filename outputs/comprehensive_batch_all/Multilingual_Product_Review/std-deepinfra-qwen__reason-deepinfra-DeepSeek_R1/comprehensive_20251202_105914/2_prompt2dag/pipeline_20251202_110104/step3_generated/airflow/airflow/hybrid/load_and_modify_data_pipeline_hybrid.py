from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='load_and_modify_data_pipeline',
    description='No description provided.',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    is_paused_upon_creation=True
) as dag:

    # Task definitions
    load_and_modify_data = DockerOperator(
        task_id='load_and_modify_data',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        environment={
            "DATASET_ID": "2",
            "DATE_COLUMN": "submission_date",
            "TABLE_NAME_PREFIX": "JOT_"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    language_detection = DockerOperator(
        task_id='language_detection',
        image='jmockit/language-detection',
        environment={
            "TEXT_COLUMN": "review_text",
            "LANG_CODE_COLUMN": "language_code",
            "OUTPUT_FILE": "lang_detected_2.json"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    sentiment_analysis = DockerOperator(
        task_id='sentiment_analysis',
        image='huggingface/transformers-inference',
        environment={
            "MODEL_NAME": "distilbert-base-uncased-finetuned-sst-2-english",
            "TEXT_COLUMN": "review_text",
            "OUTPUT_COLUMN": "sentiment_score"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    category_extraction = DockerOperator(
        task_id='category_extraction',
        image='i2t-backendwithintertwino6-column-extension:latest',
        environment={
            "EXTENDER_ID": "featureExtractor",
            "TEXT_COLUMN": "review_text",
            "OUTPUT_COLUMN": "mentioned_features"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    save_final_data = DockerOperator(
        task_id='save_final_data',
        image='i2t-backendwithintertwino6-save:latest',
        environment={
            "DATASET_ID": "2"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task dependencies
    load_and_modify_data >> language_detection >> sentiment_analysis >> category_extraction >> save_final_data