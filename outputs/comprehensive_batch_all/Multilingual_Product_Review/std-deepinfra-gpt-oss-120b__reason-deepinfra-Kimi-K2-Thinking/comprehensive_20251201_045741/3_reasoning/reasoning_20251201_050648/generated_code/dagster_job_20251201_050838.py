from dagster import op, job, resource, Field, Out, Output, In
import pandas as pd
import json
import os
from datetime import datetime
import logging

# Resources
@resource(config_schema={"data_dir": Field(str, default_value="./data")})
def data_directory_resource(init_context):
    return init_context.resource_config["data_dir"]

# Ops
@op(
    required_resource_keys={"data_dir"},
    config_schema={
        "dataset_id": Field(int, default_value=2),
        "date_column": Field(str, default_value="submission_date"),
        "table_name_prefix": Field(str, default_value="JOT_"),
    },
    out={"json_path": Out(str)},
)
def load_and_modify_data(context):
    """
    Ingest review CSV, standardize date formats, convert to JSON.
    """
    data_dir = context.resources.data_dir
    dataset_id = context.op_config["dataset_id"]
    date_column = context.op_config["date_column"]
    table_name_prefix = context.op_config["table_name_prefix"]
    
    csv_path = os.path.join(data_dir, "reviews.csv")
    json_path = os.path.join(data_dir, f"table_data_{dataset_id}.json")
    
    # Check if file exists
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found at {csv_path}")
    
    # Read CSV
    df = pd.read_csv(csv_path)
    
    # Standardize date format
    df[date_column] = pd.to_datetime(df[date_column], format="%Y%m%d").dt.strftime("%Y-%m-%d")
    
    # Add table name prefix if needed
    df["table_name"] = f"{table_name_prefix}reviews"
    
    # Convert to JSON
    df.to_json(json_path, orient="records", indent=2)
    
    context.log.info(f"Data loaded and saved to {json_path}")
    return json_path

@op(
    required_resource_keys={"data_dir"},
    config_schema={
        "text_column": Field(str, default_value="review_text"),
        "lang_code_column": Field(str, default_value="language_code"),
        "output_file": Field(str, default_value="lang_detected_2.json"),
    },
    ins={"input_json_path": In(str)},
    out={"output_json_path": Out(str)},
)
def detect_language(context, input_json_path):
    """
    Verify or correct the language_code using language detection.
    """
    data_dir = context.resources.data_dir
    text_column = context.op_config["text_column"]
    lang_code_column = context.op_config["lang_code_column"]
    output_file = context.op_config["output_file"]
    
    output_path = os.path.join(data_dir, output_file)
    
    # Read JSON
    with open(input_json_path, 'r') as f:
        data = json.load(f)
    
    # Simple language detection (in production, use a proper library)
    # For demo purposes, we'll use a heuristic based on common words
    def detect_lang(text):
        text_lower = text.lower()
        if any(word in text_lower for word in ['the', 'is', 'was', 'this']):
            return 'en'
        elif any(word in text_lower for word in ['der', 'die', 'das', 'ist', 'für']):
            return 'de'
        elif any(word in text_lower for word in ['il', 'la', 'è', 'ma']):
            return 'it'
        return 'unknown'
    
    # Update language codes
    for record in data:
        detected_lang = detect_lang(record[text_column])
        # Only update if we're confident or if original is missing
        if detected_lang != 'unknown':
            record[lang_code_column] = detected_lang
    
    # Save updated data
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    context.log.info(f"Language detection complete, saved to {output_path}")
    return output_path

@op(
    required_resource_keys={"data_dir"},
    config_schema={
        "model_name": Field(str, default_value="distilbert-base-uncased-finetuned-sst-2-english"),
        "text_column": Field(str, default_value="review_text"),
        "output_column": Field(str, default_value="sentiment_score"),
    },
    ins={"input_json_path": In(str)},
    out={"output_json_path": Out(str)},
)
def analyze_sentiment(context, input_json_path):
    """
    Determine sentiment of reviews using LLM.
    """
    data_dir = context.resources.data_dir
    text_column = context.op_config["text_column"]
    output_column = context.op_config["output_column"]
    
    output_path = os.path.join(data_dir, "sentiment_analyzed_2.json")
    
    # Read JSON
    with open(input_json_path, 'r') as f:
        data = json.load(f)
    
    # Simple sentiment analysis (in production, use transformers)
    # For demo: heuristic based on positive/negative words
    positive_words = ['fantastic', 'great', 'good', 'excellent', 'amazing', 'love', 'ottima']
    negative_words = ['bad', 'terrible', 'awful', 'hate', 'small', 'alto']  # alto = high price (negative in context)
    
    for record in data:
        text_lower = record[text_column].lower()
        pos_count = sum(1 for word in positive_words if word in text_lower)
        neg_count = sum(1 for word in negative_words if word in text_lower)
        
        if pos_count > neg_count:
            record[output_column] = "positive"
        elif neg_count > pos_count:
            record[output_column] = "negative"
        else:
            record[output_column] = "neutral"
    
    # Save updated data
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    context.log.info(f"Sentiment analysis complete, saved to {output_path}")
    return output_path

@op(
    required_resource_keys={"data_dir"},
    config_schema={
        "extender_id": Field(str, default_value="featureExtractor"),
        "text_column": Field(str, default_value="review_text"),
        "output_column": Field(str, default_value="mentioned_features"),
    },
    ins={"input_json_path": In(str)},
    out={"output_json_path": Out(str)},
)
def extract_categories(context, input_json_path):
    """
    Extract product features or categories from reviews.
    """
    data_dir = context.resources.data_dir
    text_column = context.op_config["text_column"]
    output_column = context.op_config["output_column"]
    
    output_path = os.path.join(data_dir, "column_extended_2.json")
    
    # Read JSON
    with open(input_json_path, 'r') as f:
        data = json.load(f)
    
    # Simple feature extraction (in production, use LLM)
    # For demo: keyword matching
    feature_keywords = {
        'size': ['small', 'large', 'size', 'fit', 'fits'],
        'quality': ['quality', 'qualità', 'ottima'],
        'price': ['price', 'prezzo', 'alto', 'cost'],
        'warmth': ['winter', 'warm', 'jacke'],
        'style': ['style', 'look', 'fashion']
    }
    
    for record in data:
        text_lower = record[text_column].lower()
        features = []
        for feature, keywords in feature_keywords.items():
            if any(keyword in text_lower for keyword in keywords):
                features.append(feature)
        record[output_column] = features
    
    # Save updated data
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    context.log.info(f"Category extraction complete, saved to {output_path}")
    return output_path

@op(
    required_resource_keys={"data_dir"},
    config_schema={
        "dataset_id": Field(int, default_value=2),
    },
    ins={"input_json_path": In(str)},
    out={"output_csv_path": Out(str)},
)
def save_final_data(context, input_json_path):
    """
    Export the fully enriched review data to CSV.
    """
    data_dir = context.resources.data_dir
    dataset_id = context.op_config["dataset_id"]
    
    output_path = os.path.join(data_dir, f"enriched_data_{dataset_id}.csv")
    
    # Read JSON
    with open(input_json_path, 'r') as f:
        data = json.load(f)
    
    # Convert to DataFrame and save as CSV
    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)
    
    context.log.info(f"Final data saved to {output_path}")
    return output_path

# Job definition
@job(
    resource_defs={"data_dir": data_directory_resource},
    config={
        "resources": {
            "data_dir": {"config": {"data_dir": "./data"}}
        },
        "ops": {
            "load_and_modify_data": {
                "config": {
                    "dataset_id": 2,
                    "date_column": "submission_date",
                    "table_name_prefix": "JOT_"
                }
            },
            "detect_language": {
                "config": {
                    "text_column": "review_text",
                    "lang_code_column": "language_code",
                    "output_file": "lang_detected_2.json"
                }
            },
            "analyze_sentiment": {
                "config": {
                    "model_name": "distilbert-base-uncased-finetuned-sst-2-english",
                    "text_column": "review_text",
                    "output_column": "sentiment_score"
                }
            },
            "extract_categories": {
                "config": {
                    "extender_id": "featureExtractor",
                    "text_column": "review_text",
                    "output_column": "mentioned_features"
                }
            },
            "save_final_data": {
                "config": {
                    "dataset_id": 2
                }
            }
        }
    }
)
def multilingual_review_analysis_pipeline():
    """
    Multilingual Product Review Analysis Pipeline
    """
    # Step 1: Load & Modify Data
    json_path = load_and_modify_data()
    
    # Step 2: Language Detection
    lang_path = detect_language(json_path)
    
    # Step 3: Sentiment Analysis
    sentiment_path = analyze_sentiment(lang_path)
    
    # Step 4: Category Extraction
    categories_path = extract_categories(sentiment_path)
    
    # Step 5: Save Final Data
    save_final_data(categories_path)

# Launch pattern
if __name__ == '__main__':
    # Ensure data directory exists
    os.makedirs("./data", exist_ok=True)
    
    # Create sample data if it doesn't exist
    sample_csv = "./data/reviews.csv"
    if not os.path.exists(sample_csv):
        sample_data = """review_id,product_id,review_text,language_code,submission_date
F1001,P5436,"Diese Jacke ist fantastisch für den Winter!",de,20230110
F1002,P5436,"This jacket was too small for me.",en,20230115
F1003,P5436,"La qualità è ottima ma il prezzo è alto.",it,20230118
"""
        with open(sample_csv, 'w') as f:
            f.write(sample_data)
    
    # Execute the pipeline
    result = multilingual_review_analysis_pipeline.execute_in_process()
    if result.success:
        print("Pipeline executed successfully!")
    else:
        print("Pipeline execution failed!")
        for event in result.all_events:
            if event.event_type_value == 'STEP_FAILURE':
                print(f"Step failed: {event.step_key}")