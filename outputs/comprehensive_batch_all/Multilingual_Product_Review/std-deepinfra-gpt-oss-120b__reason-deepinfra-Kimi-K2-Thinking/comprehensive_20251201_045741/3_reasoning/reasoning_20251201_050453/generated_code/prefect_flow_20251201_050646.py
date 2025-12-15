import os
import json
import csv
from datetime import datetime
from typing import List, Dict, Any, Optional

from prefect import flow, task

try:
    from langdetect import detect
except ImportError:
    def detect(text: str) -> str:
        return "unknown"

try:
    from transformers import pipeline
except ImportError:
    pipeline = None


@task(retries=1)
def load_and_modify_data(
    data_dir: str,
    dataset_id: int,
    date_column: str,
    table_name_prefix: str
) -> str:
    """Load CSV, standardize dates, and convert to JSON."""
    input_path = os.path.join(data_dir, "reviews.csv")
    output_path = os.path.join(data_dir, f"table_data_{dataset_id}.json")
    
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    data = []
    with open(input_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if date_column in row and row[date_column]:
                try:
                    date_obj = datetime.strptime(row[date_column], "%Y%m%d")
                    row[date_column] = date_obj.isoformat()
                except ValueError:
                    pass
            row["table_name"] = f"{table_name_prefix}reviews"
            data.append(row)
    
    with open(output_path, 'w', encoding='utf-8') as jsonfile:
        json.dump(data, jsonfile, ensure_ascii=False, indent=2)
    
    return output_path


@task(retries=1)
def detect_language(
    input_file: str,
    text_column: str,
    lang_code_column: str,
    output_file: str
) -> str:
    """Verify or correct language codes using language detection."""
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    for record in data:
        text = record.get(text_column, "")
        if text:
            try:
                record[lang_code_column] = detect(text)
            except Exception:
                pass
    
    output_path = os.path.join(os.path.dirname(input_file), output_file)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    return output_path


@task(retries=1)
def analyze_sentiment(
    input_file: str,
    model_name: str,
    text_column: str,
    output_column: str
) -> str:
    """Analyze sentiment using a transformer model."""
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if pipeline is not None:
        sentiment_pipeline = pipeline("sentiment-analysis", model=model_name)
    else:
        def mock_sentiment(text: str) -> Dict[str, Any]:
            positive_words = ["good", "great", "fantastic", "excellent", "amazing", "love", "perfect", "ottima"]
            negative_words = ["bad", "terrible", "awful", "hate", "poor", "worst", "small", "alto"]
            text_lower = text.lower()
            pos_count = sum(1 for word in positive_words if word in text_lower)
            neg_count = sum(1 for word in negative_words if word in text_lower)
            if pos_count > neg_count:
                return {"label": "POSITIVE", "score": 0.8}
            elif neg_count > pos_count:
                return {"label": "NEGATIVE", "score": 0.8}
            return {"label": "NEUTRAL", "score": 0.5}
        sentiment_pipeline = mock_sentiment
    
    for record in data:
        text = record.get(text_column, "")
        if text:
            try:
                if callable(sentiment_pipeline):
                    result = sentiment_pipeline(text)
                else:
                    result = sentiment_pipeline(text)[0]
                record[output_column] = result
            except Exception:
                record[output_column] = {"label": "UNKNOWN", "score": 0.0}
    
    output_path = os.path.join(os.path.dirname(input_file), "sentiment_analyzed_2.json")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    return output_path


@task(retries=1)
def extract_categories(
    input_file: str,
    extender_id: str,
    text_column: str,
    output_column: str
) -> str:
    """Extract product features/categories from reviews."""
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    feature_keywords = {
        "quality": ["quality", "qualità", "qualität", "ottima"],
        "price": ["price", "prezzo", "preis", "cost", "expensive", "cheap", "alto"],
        "size": ["size", "größe", "taglia", "small", "large", "fit", "pequeño"],
        "color": ["color", "farbe", "colore"],
        "material": ["material", "material", "materiale", "fabric", "stoff"],
        "comfort": ["comfort", "komfort", "comodo", "comfortable"],
        "warmth": ["warm", "winter", "cold", "heat", "warmth", "wärme"]
    }
    
    for record in data:
        text = record.get(text_column, "").lower()
        features = []
        for feature, keywords in feature_keywords.items():
            if any(keyword in text for keyword in keywords):
                features.append(feature)
        record[output_column] = features
    
    output_path = os.path.join(os.path.dirname(input_file), "column_extended_2.json")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    return output_path


@task(retries=1)
def save_final_data(
    input_file: str,
    data_dir: str,
    dataset_id: int
) -> str:
    """Save enriched data to CSV."""
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if not data:
        raise ValueError("No data to save")
    
    output_path = os.path.join(data_dir, f"enriched_data_{dataset_id}.csv")
    fieldnames = list(data[0].keys())
    
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    return output_path


@flow(name="multilingual-product-review-analysis")
def multilingual_product_review_analysis_flow(
    data_dir: Optional[str] = None,
    dataset_id: int = 2
):
    """Main flow for multilingual product review analysis."""
    if data_dir is None:
        data_dir = os.getenv("DATA_DIR", "./data")
    
    os.makedirs(data_dir, exist_ok=True)
    
    table_data_file = load_and_modify_data(
        data_dir=data_dir