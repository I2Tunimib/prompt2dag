from dagster import job, op, ResourceDefinition, fs_io_manager, dagster_type_loader, dagster_type_materializer

# Task Definitions
@op(
    name='load_and_modify_data',
    description='Load and Modify Data',
)
def load_and_modify_data(context):
    """Op: Load and Modify Data"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-load-and-modify:latest
    pass

@op(
    name='language_detection',
    description='Language Detection',
)
def language_detection(context):
    """Op: Language Detection"""
    # Docker execution
    # Image: jmockit/language-detection
    pass

@op(
    name='sentiment_analysis',
    description='Sentiment Analysis',
)
def sentiment_analysis(context):
    """Op: Sentiment Analysis"""
    # Docker execution
    # Image: huggingface/transformers-inference
    pass

@op(
    name='category_extraction',
    description='Category Extraction',
)
def category_extraction(context):
    """Op: Category Extraction"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-column-extension:latest
    pass

@op(
    name='save_final_data',
    description='Save Final Data',
)
def save_final_data(context):
    """Op: Save Final Data"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-save:latest
    pass

# Job Definition
@job(
    name="load_and_modify_data_pipeline",
    description="No description provided.",
    executor_def=ResourceDefinition.docker_executor(),
    resource_defs={"io_manager": fs_io_manager, "data_dir": ResourceDefinition.string_resource()},
)
def load_and_modify_data_pipeline():
    load_data = load_and_modify_data()
    detect_language = language_detection(load_data)
    analyze_sentiment = sentiment_analysis(detect_language)
    extract_category = category_extraction(analyze_sentiment)
    save_final_data(extract_category)