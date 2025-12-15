from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner

@task(name='load_and_modify_data', retries=1)
def load_and_modify_data():
    """Task: Load and Modify Data"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-load-and-modify:latest
    pass

@task(name='language_detection', retries=1)
def language_detection():
    """Task: Language Detection"""
    # Docker execution via infrastructure
    # Image: jmockit/language-detection
    pass

@task(name='sentiment_analysis', retries=1)
def sentiment_analysis():
    """Task: Sentiment Analysis"""
    # Docker execution via infrastructure
    # Image: huggingface/transformers-inference
    pass

@task(name='category_extraction', retries=1)
def category_extraction():
    """Task: Category Extraction"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-column-extension:latest
    pass

@task(name='save_final_data', retries=1)
def save_final_data():
    """Task: Save Final Data"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-save:latest
    pass

@flow(name="load_and_modify_data_pipeline", task_runner=SequentialTaskRunner)
def load_and_modify_data_pipeline():
    logger = get_run_logger()
    logger.info("Starting load_and_modify_data_pipeline")

    load_data_result = load_and_modify_data()
    language_detection_result = language_detection(wait_for=[load_data_result])
    sentiment_analysis_result = sentiment_analysis(wait_for=[language_detection_result])
    category_extraction_result = category_extraction(wait_for=[sentiment_analysis_result])
    save_final_data_result = save_final_data(wait_for=[category_extraction_result])

    logger.info("load_and_modify_data_pipeline completed successfully")

if __name__ == "__main__":
    load_and_modify_data_pipeline()