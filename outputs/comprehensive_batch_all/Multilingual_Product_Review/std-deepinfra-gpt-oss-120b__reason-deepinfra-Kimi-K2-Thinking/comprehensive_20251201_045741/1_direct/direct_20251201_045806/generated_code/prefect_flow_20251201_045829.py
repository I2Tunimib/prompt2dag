import os
import json
from datetime import datetime
from typing import List, Dict

import pandas as pd
from langdetect import detect, LangDetectException
from prefect import flow, task, get_run_logger


@task
def load_and_modify_data(data_dir: str) -> str:
    """Load CSV reviews, standardize dates, and write JSON."""
    logger = get_run_logger()
    input_path = os.path.join(data_dir, "reviews.csv")
    output_path = os.path.join(data_dir, "table_data_2.json")

    logger.info("Reading CSV from %s", input_path)
    df = pd.read_csv(input_path, dtype=str)

    # Standardize date format (ISO 8601)
    if "submission_date" in df.columns:
        logger.info("Standardizing submission_date column")
        df["submission_date"] = pd.to_datetime(
            df["submission_date"], format="%Y%m%d", errors="coerce"
        ).dt.strftime("%Y-%m-%d")

    # Convert DataFrame to list of dicts and write JSON
    records: List[Dict] = df.to_dict(orient="records")
    logger.info("Writing %d records to JSON %s", len(records), output_path)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_path


@task
def language_detection(input_path: str) -> str:
    """Detect or verify language_code for each review."""
    logger = get_run_logger()
    output_path = os.path.join(os.path.dirname(input_path), "lang_detected_2.json")

    logger.info("Reading JSON from %s", input_path)
    with open(input_path, "r", encoding="utf-8") as f:
        records: List[Dict] = json.load(f)

    for rec in records:
        text = rec.get("review_text", "")
        try:
            detected_lang = detect(text)
            rec["detected_language"] = detected_lang
            # Optionally correct the original column
            if rec.get("language_code") != detected_lang:
                logger.debug(
                    "Correcting language_code from %s to %s for review_id %s",
                    rec.get("language_code"),
                    detected_lang,
                    rec.get("review_id"),
                )
                rec["language_code"] = detected_lang
        except LangDetectException:
            logger.warning("Could not detect language for review_id %s", rec.get("review_id"))
            rec["detected_language"] = None

    logger.info("Writing language‑detected data to %s", output_path)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_path


@task
def sentiment_analysis(input_path: str) -> str:
    """Perform a simple sentiment analysis on review_text."""
    logger = get_run_logger()
    output_path = os.path.join(os.path.dirname(input_path), "sentiment_analyzed_2.json")

    logger.info("Reading JSON from %s", input_path)
    with open(input_path, "r", encoding="utf-8") as f:
        records: List[Dict] = json.load(f)

    positive_keywords = {"good", "great", "fantastic", "excellent", "amazing", "ottima"}
    negative_keywords = {"bad", "poor", "small", "terrible", "horrible", "alto"}

    for rec in records:
        text = rec.get("review_text", "").lower()
        if any(word in text for word in positive_keywords):
            sentiment = "positive"
        elif any(word in text for word in negative_keywords):
            sentiment = "negative"
        else:
            sentiment = "neutral"
        rec["sentiment_score"] = sentiment

    logger.info("Writing sentiment‑analyzed data to %s", output_path)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_path


@task
def category_extraction(input_path: str) -> str:
    """Extract mentioned product features from review_text."""
    logger = get_run_logger()
    output_path = os.path.join(os.path.dirname(input_path), "column_extended_2.json")

    logger.info("Reading JSON from %s", input_path)
    with open(input_path, "r", encoding="utf-8") as f:
        records: List[Dict] = json.load(f)

    for rec in records:
        text = rec.get("review_text", "")
        # Very naive feature extraction: words longer than 4 characters
        features = [word for word in text.split() if len(word) > 4]
        rec["mentioned_features"] = features

    logger.info("Writing category‑extracted data to %s", output_path)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_path


@task
def save_final_data(input_path: str) -> str:
    """Export enriched data to CSV."""
    logger = get_run_logger()
    output_path = os.path.join(os.path.dirname(input_path), "enriched_data_2.csv")

    logger.info("Reading JSON from %s", input_path)
    with open(input_path, "r", encoding="utf-8") as f:
        records: List[Dict] = json.load(f)

    df = pd.DataFrame(records)
    logger.info("Writing %d rows to CSV %s", len(df), output_path)
    df.to_csv(output_path, index=False, encoding="utf-8")

    return output_path


@flow
def multilingual_review_analysis_flow():
    """
    Orchestrates the multilingual product review analysis pipeline.
    """
    data_dir = os.getenv("DATA_DIR", ".")

    json_path_1 = load_and_modify_data(data_dir)
    json_path_2 = language_detection(json_path_1)
    json_path_3 = sentiment_analysis(json_path_2)
    json_path_4 = category_extraction(json_path_3)
    csv_path = save_final_data(json_path_4)

    get_run_logger().info("Pipeline completed. Enriched CSV at %s", csv_path)


if __name__ == "__main__":
    # For local execution; in production, configure a Prefect deployment with a schedule if needed.
    multilingual_review_analysis_flow()