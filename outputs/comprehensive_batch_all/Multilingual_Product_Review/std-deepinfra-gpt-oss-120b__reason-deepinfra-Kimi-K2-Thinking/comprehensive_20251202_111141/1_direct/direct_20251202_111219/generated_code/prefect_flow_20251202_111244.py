import os
import json
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
from langdetect import detect, LangDetectException
from prefect import flow, task
from transformers import pipeline, Pipeline


DATASET_ID = 2
DATE_COLUMN = "submission_date"
TABLE_NAME_PREFIX = "JOT_"
TEXT_COLUMN = "review_text"
LANG_CODE_COLUMN = "language_code"
OUTPUT_FILE_LANG = "lang_detected_2.json"
OUTPUT_FILE_SENTIMENT = "sentiment_analyzed_2.json"
OUTPUT_FILE_FEATURES = "column_extended_2.json"
FINAL_CSV = "enriched_data_2.csv"
MODEL_NAME = "distilbert-base-uncased-finetuned-sst-2-english"


def _get_data_dir() -> Path:
    """Resolve the data directory from the environment."""
    data_dir = os.getenv("DATA_DIR", ".")
    return Path(data_dir).resolve()


@task
def load_and_modify_data() -> Path:
    """
    Load the CSV review file, standardize the date format, and write JSON output.

    Returns:
        Path to the generated JSON file.
    """
    data_dir = _get_data_dir()
    csv_path = data_dir / "reviews.csv"
    json_path = data_dir / "table_data_2.json"

    if not csv_path.is_file():
        raise FileNotFoundError(f"Input CSV not found at {csv_path}")

    df = pd.read_csv(csv_path, dtype=str)

    # Standardize date format to ISO (YYYY-MM-DD)
    if DATE_COLUMN in df.columns:
        def _parse_date(val: str) -> str:
            try:
                dt = datetime.strptime(val, "%Y%m%d")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                return val  # keep original if parsing fails

        df[DATE_COLUMN] = df[DATE_COLUMN].apply(_parse_date)

    # Convert to list of dicts and write JSON
    records: List[dict] = df.to_dict(orient="records")
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return json_path


@task
def language_detection(input_json: Path) -> Path:
    """
    Detect language of the review text and update the language_code column.

    Args:
        input_json: Path to the JSON file from the previous step.

    Returns:
        Path to the JSON file with detected language codes.
    """
    output_path = input_json.parent / OUTPUT_FILE_LANG

    with input_json.open("r", encoding="utf-8") as f:
        records = json.load(f)

    for rec in records:
        text = rec.get(TEXT_COLUMN, "")
        try:
            detected = detect(text)
        except LangDetectException:
            detected = rec.get(LANG_CODE_COLUMN, "")
        rec[LANG_CODE_COLUMN] = detected

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_path


def _load_sentiment_pipeline() -> Pipeline:
    """Create a HuggingFace sentiment analysis pipeline."""
    return pipeline("sentiment-analysis", model=MODEL_NAME, tokenizer=MODEL_NAME)


@task
def sentiment_analysis(input_json: Path) -> Path:
    """
    Perform sentiment analysis on review text.

    Args:
        input_json: Path to the JSON file with language detection results.

    Returns:
        Path to the JSON file enriched with sentiment scores.
    """
    output_path = input_json.parent / OUTPUT_FILE_SENTIMENT
    sentiment_pipe = _load_sentiment_pipeline()

    with input_json.open("r", encoding="utf-8") as f:
        records = json.load(f)

    for rec in records:
        text = rec.get(TEXT_COLUMN, "")
        if not text:
            rec["sentiment_score"] = None
            continue
        result = sentiment_pipe(text[:512])  # truncate for speed
        # result is a list like [{'label': 'POSITIVE', 'score': 0.99}]
        sentiment = result[0]
        rec["sentiment_score"] = {
            "label": sentiment["label"],
            "score": sentiment["score"],
        }

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_path


@task
def category_extraction(input_json: Path) -> Path:
    """
    Extract simple product features from review text.

    This placeholder implementation extracts nouns using a very naive rule:
    words following the word "is" or "are" are considered features.

    Args:
        input_json: Path to the JSON file with sentiment analysis results.

    Returns:
        Path to the JSON file with an added 'mentioned_features' column.
    """
    output_path = input_json.parent / OUTPUT_FILE_FEATURES

    with input_json.open("r", encoding="utf-8") as f:
        records = json.load(f)

    for rec in records:
        text = rec.get(TEXT_COLUMN, "")
        features: List[str] = []

        tokens = text.split()
        for i, token in enumerate(tokens):
            lowered = token.lower().strip(".,!?:;")
            if lowered in {"is", "are"} and i + 1 < len(tokens):
                candidate = tokens[i + 1].strip(".,!?:;")
                features.append(candidate)

        rec["mentioned_features"] = features

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_path


@task
def save_final_data(input_json: Path) -> Path:
    """
    Convert the enriched JSON data back to CSV.

    Args:
        input_json: Path to the final JSON file.

    Returns:
        Path to the generated CSV file.
    """
    data_dir = _get_data_dir()
    csv_path = data_dir / FINAL_CSV

    with input_json.open("r", encoding="utf-8") as f:
        records = json.load(f)

    df = pd.DataFrame(records)
    df.to_csv(csv_path, index=False, encoding="utf-8")

    return csv_path


@flow
def multilingual_review_analysis_flow() -> None:
    """
    Orchestrates the multilingual product review analysis pipeline.
    """
    json_1 = load_and_modify_data()
    json_2 = language_detection(json_1)
    json_3 = sentiment_analysis(json_2)
    json_4 = category_extraction(json_3)
    _ = save_final_data(json_4)


if __name__ == "__main__":
    multilingual_review_analysis_flow()