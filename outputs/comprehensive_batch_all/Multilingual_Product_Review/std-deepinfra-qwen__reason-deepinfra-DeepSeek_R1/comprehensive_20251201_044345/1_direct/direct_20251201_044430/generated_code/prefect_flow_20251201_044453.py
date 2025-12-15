from prefect import flow, task
import pandas as pd
import json
from datetime import datetime

# Constants
DATA_DIR = "/path/to/data"
DATASET_ID = 2
DATE_COLUMN = "submission_date"
TABLE_NAME_PREFIX = "JOT_"
TEXT_COLUMN = "review_text"
LANG_CODE_COLUMN = "language_code"
MODEL_NAME = "distilbert-base-uncased-finetuned-sst-2-english"
OUTPUT_COLUMN = "sentiment_score"
EXTENDER_ID = "featureExtractor"
OUTPUT_FEATURE_COLUMN = "mentioned_features"

# Task 1: Load & Modify Data
@task
def load_and_modify_data(input_csv: str, output_json: str):
    df = pd.read_csv(input_csv)
    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN], format="%Y%m%d")
    df.to_json(output_json, orient="records", lines=True)

# Task 2: Language Detection
@task
def language_detection(input_json: str, output_json: str):
    with open(input_json, "r") as f:
        data = json.load(f)
    
    for record in data:
        # Placeholder for language detection logic
        detected_lang = record[LANG_CODE_COLUMN]  # Assume correct for simplicity
        record[LANG_CODE_COLUMN] = detected_lang
    
    with open(output_json, "w") as f:
        json.dump(data, f, indent=4)

# Task 3: Sentiment Analysis
@task
def sentiment_analysis(input_json: str, output_json: str):
    with open(input_json, "r") as f:
        data = json.load(f)
    
    for record in data:
        # Placeholder for sentiment analysis logic
        sentiment_score = 0.8  # Assume positive sentiment for simplicity
        record[OUTPUT_COLUMN] = sentiment_score
    
    with open(output_json, "w") as f:
        json.dump(data, f, indent=4)

# Task 4: Category Extraction
@task
def category_extraction(input_json: str, output_json: str):
    with open(input_json, "r") as f:
        data = json.load(f)
    
    for record in data:
        # Placeholder for category extraction logic
        mentioned_features = ["quality", "price"]  # Example features
        record[OUTPUT_FEATURE_COLUMN] = mentioned_features
    
    with open(output_json, "w") as f:
        json.dump(data, f, indent=4)

# Task 5: Save Final Data
@task
def save_final_data(input_json: str, output_csv: str):
    with open(input_json, "r") as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    df.to_csv(output_csv, index=False)

# Flow: Multilingual Product Review Analysis Pipeline
@flow
def multilingual_product_review_analysis_pipeline():
    input_csv = f"{DATA_DIR}/reviews.csv"
    table_data_json = f"{DATA_DIR}/table_data_2.json"
    lang_detected_json = f"{DATA_DIR}/lang_detected_2.json"
    sentiment_analyzed_json = f"{DATA_DIR}/sentiment_analyzed_2.json"
    column_extended_json = f"{DATA_DIR}/column_extended_2.json"
    final_csv = f"{DATA_DIR}/enriched_data_2.csv"

    load_and_modify_data.submit(input_csv, table_data_json)
    language_detection.submit(table_data_json, lang_detected_json)
    sentiment_analysis.submit(lang_detected_json, sentiment_analyzed_json)
    category_extraction.submit(sentiment_analyzed_json, column_extended_json)
    save_final_data.submit(column_extended_json, final_csv)

if __name__ == "__main__":
    multilingual_product_review_analysis_pipeline()