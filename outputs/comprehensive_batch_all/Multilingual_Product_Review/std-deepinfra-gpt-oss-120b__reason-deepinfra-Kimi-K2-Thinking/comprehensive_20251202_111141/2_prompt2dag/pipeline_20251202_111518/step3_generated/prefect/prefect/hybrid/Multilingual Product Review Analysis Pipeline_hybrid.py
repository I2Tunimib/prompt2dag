from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name="load_and_modify_reviews", retries=1)
def load_and_modify_reviews():
    """Task: Load and Modify Reviews"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-load-and-modify:latest
    pass


@task(name="detect_review_language", retries=1)
def detect_review_language():
    """Task: Detect Review Language"""
    # Docker execution via infrastructure
    # Image: jmockit/language-detection
    pass


@task(name="analyze_sentiment", retries=1)
def analyze_sentiment():
    """Task: Sentiment Analysis"""
    # Docker execution via infrastructure
    # Image: huggingface/transformers-inference
    pass


@task(name="extract_review_features", retries=1)
def extract_review_features():
    """Task: Extract Review Features"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-column-extension:latest
    pass


@task(name="save_enriched_reviews", retries=1)
def save_enriched_reviews():
    """Task: Save Enriched Reviews"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-save:latest
    pass


@flow(
    name="multilingual_product_review_analysis_pipeline",
    task_runner=SequentialTaskRunner(),
)
def multilingual_product_review_analysis_pipeline():
    """Main flow orchestrating the multilingual product review analysis pipeline."""
    # Entry point
    load_and_modify_reviews()
    # Subsequent steps respecting dependencies
    detect_review_language()
    analyze_sentiment()
    extract_review_features()
    save_enriched_reviews()


if __name__ == "__main__":
    multilingual_product_review_analysis_pipeline()