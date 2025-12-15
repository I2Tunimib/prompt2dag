from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="multilingual_product_review_analysis_pipeline",
    description="Enriches product reviews with language verification, sentiment, and key feature extraction using LLM capabilities for deeper customer insight.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["multilingual", "review", "analysis"],
) as dag:

    
    load_and_modify_reviews = DockerOperator(
        task_id='load_and_modify_reviews',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        environment={},
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )
    

    
    detect_review_language = DockerOperator(
        task_id='detect_review_language',
        image='jmockit/language-detection',
        environment={},
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )
    

    
    analyze_sentiment = DockerOperator(
        task_id='analyze_sentiment',
        image='huggingface/transformers-inference',
        environment={},
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )
    

    
    extract_review_features = DockerOperator(
        task_id='extract_review_features',
        image='i2t-backendwithintertwino6-column-extension:latest',
        environment={},
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )
    

    
    save_enriched_reviews = DockerOperator(
        task_id='save_enriched_reviews',
        image='i2t-backendwithintertwino6-save:latest',
        environment={},
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )
    

    # Set sequential dependencies
    load_and_modify_reviews >> detect_review_language >> analyze_sentiment >> extract_review_features >> save_enriched_reviews