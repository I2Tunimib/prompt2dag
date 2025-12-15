import os
import json
import csv
import datetime
from dagster import op, job, Field, String, Int


@op(
    config_schema={
        "data_dir": Field(String, default_value="./data"),
        "dataset_id": Field(Int, default_value=2),
        "date_column": Field(String, default_value="submission_date"),
        "table_name_prefix": Field(String, default_value="JOT_"),
    }
)
def load_and_modify(context) -> str:
    """Load CSV reviews, standardize dates, and write JSON."""
    cfg = context.op_config
    data_dir = cfg["data_dir"]
    dataset_id = cfg["dataset_id"]
    date_column = cfg["date_column"]

    input_path = os.path.join(data_dir, "reviews.csv")
    output_path = os.path.join(data_dir, f"table_data_{dataset_id}.json")

    rows = []
    with open(input_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            raw_date = row.get(date_column)
            if raw_date:
                try:
                    dt = datetime.datetime.strptime(raw_date, "%Y%m%d")
                    row[date_column] = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass
            rows.append(row)

    os.makedirs(data_dir, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as jsonfile:
        json.dump(rows, jsonfile, ensure_ascii=False, indent=2)

    context.log.info(f"Wrote JSON to {output_path}")
    return output_path


@op(
    config_schema={
        "data_dir": Field(String, default_value="./data"),
        "text_column": Field(String, default_value="review_text"),
        "lang_code_column": Field(String, default_value="language_code"),
        "output_file": Field(String, default_value="lang_detected_2.json"),
    }
)
def language_detection(context, input_path: str) -> str:
    """Detect or verify language codes and write JSON."""
    cfg = context.op_config
    data_dir = cfg["data_dir"]
    text_column = cfg["text_column"]
    lang_code_column = cfg["lang_code_column"]
    output_file = cfg["output_file"]

    output_path = os.path.join(data_dir, output_file)

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    for row in data:
        # Placeholder: keep existing language code
        row[lang_code_column] = row.get(lang_code_column, "unknown")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    context.log.info(f"Wrote language-detected JSON to {output_path}")
    return output_path


@op(
    config_schema={
        "data_dir": Field(String, default_value="./data"),
        "model_name": Field(String, default_value="distilbert-base-uncased-finetuned-sst-2-english"),
        "text_column": Field(String, default_value="review_text"),
        "output_column": Field(String, default_value="sentiment_score"),
    }
)
def sentiment_analysis(context, input_path: str) -> str:
    """Add a dummy sentiment score to each review and write JSON."""
    cfg = context.op_config
    data_dir = cfg["data_dir"]
    output_column = cfg["output_column"]

    output_path = os.path.join(data_dir, "sentiment_analyzed_2.json")

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    for row in data:
        # Placeholder sentiment score
        row[output_column] = 0.5

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    context.log.info(f"Wrote sentiment-analyzed JSON to {output_path}")
    return output_path


@op(
    config_schema={
        "data_dir": Field(String, default_value="./data"),
        "extender_id": Field(String, default_value="featureExtractor"),
        "text_column": Field(String, default_value="review_text"),
        "output_column": Field(String, default_value="mentioned_features"),
    }
)
def category_extraction(context, input_path: str) -> str:
    """Extract dummy product features and write JSON."""
    cfg = context.op_config
    data_dir = cfg["data_dir"]
    output_column = cfg["output_column"]

    output_path = os.path.join(data_dir, "column_extended_2.json")

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    for row in data:
        # Placeholder feature extraction
        row[output_column] = ["feature_a", "feature_b"]

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    context.log.info(f"Wrote category-extracted JSON to {output_path}")
    return output_path


@op(
    config_schema={
        "data_dir": Field(String, default_value="./data"),
        "dataset_id": Field(Int, default_value=2),
    }
)
def save_final_data(context, input_path: str) -> str:
    """Save enriched data to CSV."""
    cfg = context.op_config
    data_dir = cfg["data_dir"]
    dataset_id = cfg["dataset_id"]

    output_path = os.path.join(data_dir, f"enriched_data_{dataset_id}.csv")

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not data:
        context.log.warning("No data to write.")
        return output_path

    fieldnames = list(data[0].keys())
    os.makedirs(data_dir, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)

    context.log.info(f"Wrote enriched CSV to {output_path}")
    return output_path


@job
def product_review_analysis_job():
    """Orchestrates the multilingual product review analysis pipeline."""
    loaded_path = load_and_modify()
    lang_path = language_detection(loaded_path)
    sentiment_path = sentiment_analysis(lang_path)
    category_path = category_extraction(sentiment_path)
    save_final_data(category_path)


if __name__ == "__main__":
    result = product_review_analysis_job.execute_in_process(
        run_config={
            "ops": {
                "load_and_modify": {
                    "config": {
                        "data_dir": "./data",
                        "dataset_id": 2,
                        "date_column": "submission_date",
                        "table_name_prefix": "JOT_",
                    }
                },
                "language_detection": {
                    "config": {
                        "data_dir": "./data",
                        "text_column": "review_text",
                        "lang_code_column": "language_code",
                        "output_file": "lang_detected_2.json",
                    }
                },
                "sentiment_analysis": {
                    "config": {
                        "data_dir": "./data",
                        "model_name": "distilbert-base-uncased-finetuned-sst-2-english",
                        "text_column": "review_text",
                        "output_column": "sentiment_score",
                    }
                },
                "category_extraction": {
                    "config": {
                        "data_dir": "./data",
                        "extender_id": "featureExtractor",
                        "text_column": "review_text",
                        "output_column": "mentioned_features",
                    }
                },
                "save_final_data": {
                    "config": {
                        "data_dir": "./data",
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