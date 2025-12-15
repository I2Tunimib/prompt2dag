from dagster import op, job, fs_io_manager, docker_executor, ResourceDefinition

# ----------------------------------------------------------------------
# Task Definitions (provided exactly as given)
# ----------------------------------------------------------------------

@op(
    name='load_and_modify_reviews',
    description='Load and Modify Reviews',
)
def load_and_modify_reviews(context):
    """Op: Load and Modify Reviews"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-load-and-modify:latest
    pass


@op(
    name='detect_review_language',
    description='Detect Review Language',
)
def detect_review_language(context):
    """Op: Detect Review Language"""
    # Docker execution
    # Image: jmockit/language-detection
    pass


@op(
    name='analyze_sentiment',
    description='Sentiment Analysis',
)
def analyze_sentiment(context):
    """Op: Sentiment Analysis"""
    # Docker execution
    # Image: huggingface/transformers-inference
    pass


@op(
    name='extract_review_features',
    description='Extract Review Features',
)
def extract_review_features(context):
    """Op: Extract Review Features"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-column-extension:latest
    pass


@op(
    name='save_enriched_reviews',
    description='Save Enriched Reviews',
)
def save_enriched_reviews(context):
    """Op: Save Enriched Reviews"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-save:latest
    pass


# ----------------------------------------------------------------------
# Resource Definitions
# ----------------------------------------------------------------------
# Placeholder resource for data directory; replace with actual implementation as needed.
data_dir_fs = ResourceDefinition.hardcoded_resource("/tmp/data_dir")


# ----------------------------------------------------------------------
# Job Definition
# ----------------------------------------------------------------------
@job(
    name="multilingual_product_review_analysis_pipeline",
    description="Enriches product reviews with language verification, sentiment, and key feature extraction using LLM capabilities for deeper customer insight.",
    executor_def=docker_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "data_dir_fs": data_dir_fs,
    },
)
def multilingual_product_review_analysis_pipeline():
    # Sequential wiring of ops
    (
        load_and_modify_reviews()
        >> detect_review_language()
        >> analyze_sentiment()
        >> extract_review_features()
        >> save_enriched_reviews()
    )