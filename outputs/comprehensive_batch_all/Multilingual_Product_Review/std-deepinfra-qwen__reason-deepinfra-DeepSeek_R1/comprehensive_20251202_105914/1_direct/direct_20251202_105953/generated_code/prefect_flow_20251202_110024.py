from prefect import flow, task, get_run_logger
import pandas as pd
import json
from datetime import datetime

# Task 1: Load & Modify Data
@task
def load_and_modify_data(dataset_id: int, date_column: str, table_name_prefix: str) -> str:
    logger = get_run_logger()
    data_dir = "/path/to/DATA_DIR"  # Replace with actual DATA_DIR
    input_file = f"{data_dir}/reviews.csv"
    output_file = f"{data_dir}/table_data_{dataset_id}.json"

    logger.info(f"Loading data from {input_file}")
    df = pd.read_csv(input_file)

    # Standardize date formats
    df[date_column] = pd.to_datetime(df[date_column], format='%Y%m%d').dt.strftime('%Y-%m-%d')

    # Convert to JSON
    df.to_json(output_file, orient='records', lines=True)
    logger.info(f"Data saved to {output_file}")

    return output_file

# Task 2: Language Detection
@task
def language_detection(input_file: str, text_column: str, lang_code_column: str, output_file: str) -> str:
    logger = get_run_logger()
    logger.info(f"Detecting language in {input_file}")

    with open(input_file, 'r') as f:
        data = [json.loads(line) for line in f]

    for record in data:
        # Placeholder for language detection logic
        detected_lang = "en"  # Replace with actual language detection
        record[lang_code_column] = detected_lang

    with open(output_file, 'w') as f:
        for record in data:
            f.write(json.dumps(record) + '\n')

    logger.info(f"Language detection results saved to {output_file}")

    return output_file

# Task 3: Sentiment Analysis
@task
def sentiment_analysis(input_file: str, model_name: str, text_column: str, output_column: str, output_file: str) -> str:
    logger = get_run_logger()
    logger.info(f"Performing sentiment analysis on {input_file}")

    with open(input_file, 'r') as f:
        data = [json.loads(line) for line in f]

    for record in data:
        # Placeholder for sentiment analysis logic
        sentiment_score = 0.8  # Replace with actual sentiment score
        record[output_column] = sentiment_score

    with open(output_file, 'w') as f:
        for record in data:
            f.write(json.dumps(record) + '\n')

    logger.info(f"Sentiment analysis results saved to {output_file}")

    return output_file

# Task 4: Category Extraction
@task
def category_extraction(input_file: str, extender_id: str, text_column: str, output_column: str, output_file: str) -> str:
    logger = get_run_logger()
    logger.info(f"Extracting categories from {input_file}")

    with open(input_file, 'r') as f:
        data = [json.loads(line) for line in f]

    for record in data:
        # Placeholder for category extraction logic
        mentioned_features = ["feature1", "feature2"]  # Replace with actual features
        record[output_column] = mentioned_features

    with open(output_file, 'w') as f:
        for record in data:
            f.write(json.dumps(record) + '\n')

    logger.info(f"Category extraction results saved to {output_file}")

    return output_file

# Task 5: Save Final Data
@task
def save_final_data(input_file: str, dataset_id: int) -> str:
    logger = get_run_logger()
    data_dir = "/path/to/DATA_DIR"  # Replace with actual DATA_DIR
    output_file = f"{data_dir}/enriched_data_{dataset_id}.csv"

    logger.info(f"Saving final data to {output_file}")

    with open(input_file, 'r') as f:
        data = [json.loads(line) for line in f]

    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False)

    logger.info(f"Final data saved to {output_file}")

    return output_file

# Flow: Multilingual Product Review Analysis Pipeline
@flow
def multilingual_product_review_analysis_pipeline():
    logger = get_run_logger()
    logger.info("Starting Multilingual Product Review Analysis Pipeline")

    # Step 1: Load & Modify Data
    table_data_file = load_and_modify_data(dataset_id=2, date_column="submission_date", table_name_prefix="JOT_")

    # Step 2: Language Detection
    lang_detected_file = language_detection(
        input_file=table_data_file,
        text_column="review_text",
        lang_code_column="language_code",
        output_file="lang_detected_2.json"
    )

    # Step 3: Sentiment Analysis
    sentiment_analyzed_file = sentiment_analysis(
        input_file=lang_detected_file,
        model_name="distilbert-base-uncased-finetuned-sst-2-english",
        text_column="review_text",
        output_column="sentiment_score",
        output_file="sentiment_analyzed_2.json"
    )

    # Step 4: Category Extraction
    column_extended_file = category_extraction(
        input_file=sentiment_analyzed_file,
        extender_id="featureExtractor",
        text_column="review_text",
        output_column="mentioned_features",
        output_file="column_extended_2.json"
    )

    # Step 5: Save Final Data
    save_final_data(input_file=column_extended_file, dataset_id=2)

    logger.info("Multilingual Product Review Analysis Pipeline completed successfully")

if __name__ == '__main__':
    multilingual_product_review_analysis_pipeline()