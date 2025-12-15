import os
import json
from datetime import datetime
from typing import Dict, List

import pandas as pd
from dagster import OpExecutionContext, job, op, ConfigurableResource, resource, String, Int, Bool


try:
    from langdetect import detect  # type: ignore
except Exception:  # pragma: no cover
    # Simple fallback if langdetect is unavailable
    def detect(text: str) -> str:  # pylint: disable=function-redefined
        return "en"


def _ensure_dir(path: str) -> None:
    """Create directory if it does not exist."""
    os.makedirs(path, exist_ok=True)


@op(
    config_schema={
        "data_dir": String,
        "dataset_id": Int,
        "date_column": String,
        "table_name_prefix": String,
    },
    description="Load CSV, standardize dates, and write JSON.",
)
def load_and_modify(context: OpExecutionContext) -> str:
    cfg = context.op_config
    data_dir = cfg["data_dir"]
    dataset_id = cfg["dataset_id"]
    date_col = cfg["date_column"]
    prefix = cfg["table_name_prefix"]

    csv_path = os.path.join(data_dir, "reviews.csv")
    json_path = os.path.join(data_dir, f"{prefix}table_data_{dataset_id}.json")
    _ensure_dir(data_dir)

    df = pd.read_csv(csv_path, dtype=str)
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], format="%Y%m%d", errors="coerce").dt.strftime(
            "%Y-%m-%d"
        )
    records = df.to_dict(orient="records")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    context.log.info(f"Wrote JSON to {json_path}")
    return json_path


@op(
    config_schema={
        "text_column": String,
        "lang_code_column": String,
        "output_file": String,
    },
    description="Detect language for each review and write enriched JSON.",
)
def language_detection(context: OpExecutionContext, input_path: str) -> str:
    cfg = context.op_config
    text_col = cfg["text_column"]
    lang_col = cfg["lang_code_column"]
    output_file = cfg["output_file"]

    with open(input_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    for rec in records:
        text = rec.get(text_col, "")
        try:
            detected = detect(text)
        except Exception:  # pragma: no cover
            detected = rec.get(lang_col, "en")
        rec[lang_col] = detected

    output_path = os.path.join(os.path.dirname(input_path), output_file)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    context.log.info(f"Language detection written to {output_path}")
    return output_path


@op(
    config_schema={
        "model_name": String,
        "text_column": String,
        "output_column": String,
    },
    description="Perform sentiment analysis (placeholder) and write JSON.",
)
def sentiment_analysis(context: OpExecutionContext, input_path: str) -> str:
    cfg = context.op_config
    text_col = cfg["text_column"]
    out_col = cfg["output_column"]

    with open(input_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    # Very naive sentiment scoring for demonstration purposes
    positive_keywords = {"good", "great", "fantastic", "excellent", "love", "awesome"}
    negative_keywords = {"bad", "poor", "terrible", "small", "hate", "worst"}

    for rec in records:
        text = rec.get(text_col, "").lower()
        pos = any(word in text for word in positive_keywords)
        neg = any(word in text for word in negative_keywords)
        if pos and not neg:
            score = 1.0
        elif neg and not pos:
            score = -1.0
        else:
            score = 0.0
        rec[out_col] = score

    output_path = os.path.join(
        os.path.dirname(input_path), f"sentiment_analyzed_{os.path.basename(input_path)}"
    )
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    context.log.info(f"Sentiment analysis written to {output_path}")
    return output_path


@op(
    config_schema={
        "extender_id": String,
        "text_column": String,
        "output_column": String,
    },
    description="Extract product features (placeholder) and write JSON.",
)
def category_extraction(context: OpExecutionContext, input_path: str) -> str:
    cfg = context.op_config
    text_col = cfg["text_column"]
    out_col = cfg["output_column"]

    with open(input_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    # Simple placeholder: extract capitalized words as "features"
    for rec in records:
        text = rec.get(text_col, "")
        features = [word for word in text.split() if word.istitle()]
        rec[out_col] = features

    output_path = os.path.join(
        os.path.dirname(input_path), f"column_extended_{os.path.basename(input_path)}"
    )
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    context.log.info(f"Category extraction written to {output_path}")
    return output_path


@op(
    config_schema={"dataset_id": Int},
    description="Save enriched JSON data to CSV.",
)
def save_final_data(context: OpExecutionContext, input_path: str) -> str:
    cfg = context.op_config
    dataset_id = cfg["dataset_id"]
    df = pd.read_json(input_path)

    csv_path = os.path.join(os.path.dirname(input_path), f"enriched_data_{dataset_id}.csv")
    df.to_csv(csv_path, index=False)
    context.log.info(f"Final CSV saved to {csv_path}")
    return csv_path


@job
def multilingual_review_analysis():
    """Orchestrates the multilingual product review analysis pipeline."""
    load_path = load_and_modify()
    lang_path = language_detection(load_path)
    sentiment_path = sentiment_analysis(lang_path)
    category_path = category_extraction(sentiment_path)
    _ = save_final_data(category_path)


if __name__ == "__main__":
    # Example execution with default configuration values.
    # Adjust the DATA_DIR environment variable or pass a config dict as needed.
    data_dir = os.getenv("DATA_DIR", "/tmp/data")
    result = multilingual_review_analysis.execute_in_process(
        run_config={
            "ops": {
                "load_and_modify": {
                    "config": {
                        "data_dir": data_dir,
                        "dataset_id": 2,
                        "date_column": "submission_date",
                        "table_name_prefix": "JOT_",
                    }
                },
                "language_detection": {
                    "config": {
                        "text_column": "review_text",
                        "lang_code_column": "language_code",
                        "output_file": "lang_detected_2.json",
                    }
                },
                "sentiment_analysis": {
                    "config": {
                        "model_name": "distilbert-base-uncased-finetuned-sst-2-english",
                        "text_column": "review_text",
                        "output_column": "sentiment_score",
                    }
                },
                "category_extraction": {
                    "config": {
                        "extender_id": "featureExtractor",
                        "text_column": "review_text",
                        "output_column": "mentioned_features",
                    }
                },
                "save_final_data": {
                    "config": {
                        "dataset_id": 2,
                    }
                },
            }
        }
    )
    if result.success:
        print("Pipeline executed successfully.")
    else:
        print("Pipeline failed.")