from dagster import op, job, get_dagster_logger, RetryPolicy

# Define the ops for each step in the pipeline

@op
def load_and_modify_data(context):
    """
    Ingest review CSV, standardize date formats, convert to JSON.
    """
    import pandas as pd
    from datetime import datetime

    data_dir = context.op_config.get("DATA_DIR", "./data")
    input_file = f"{data_dir}/reviews.csv"
    output_file = f"{data_dir}/table_data_2.json"

    df = pd.read_csv(input_file)
    df['submission_date'] = pd.to_datetime(df['submission_date'], format='%Y%m%d')
    df.to_json(output_file, orient='records', lines=True)

    return output_file

@op
def detect_language(context, input_file):
    """
    Verify or correct the language_code using language detection algorithms.
    """
    import json
    from langdetect import detect

    output_file = f"{context.op_config['DATA_DIR']}/lang_detected_2.json"

    with open(input_file, 'r') as f:
        data = [json.loads(line) for line in f]

    for record in data:
        record['language_code'] = detect(record['review_text'])

    with open(output_file, 'w') as f:
        for record in data:
            json.dump(record, f)
            f.write('\n')

    return output_file

@op
def analyze_sentiment(context, input_file):
    """
    Determine sentiment of reviews using LLM.
    """
    import json
    from transformers import pipeline

    model_name = context.op_config.get("MODEL_NAME", "distilbert-base-uncased-finetuned-sst-2-english")
    sentiment_pipeline = pipeline("sentiment-analysis", model=model_name)

    output_file = f"{context.op_config['DATA_DIR']}/sentiment_analyzed_2.json"

    with open(input_file, 'r') as f:
        data = [json.loads(line) for line in f]

    for record in data:
        result = sentiment_pipeline(record['review_text'])[0]
        record['sentiment_score'] = result['score']

    with open(output_file, 'w') as f:
        for record in data:
            json.dump(record, f)
            f.write('\n')

    return output_file

@op
def extract_categories(context, input_file):
    """
    Extract product features or categories from reviews.
    """
    import json
    from transformers import pipeline

    extender_id = context.op_config.get("EXTENDER_ID", "featureExtractor")
    feature_extractor = pipeline("feature-extraction", model=extender_id)

    output_file = f"{context.op_config['DATA_DIR']}/column_extended_2.json"

    with open(input_file, 'r') as f:
        data = [json.loads(line) for line in f]

    for record in data:
        features = feature_extractor(record['review_text'])
        record['mentioned_features'] = features[0]

    with open(output_file, 'w') as f:
        for record in data:
            json.dump(record, f)
            f.write('\n')

    return output_file

@op
def save_final_data(context, input_file):
    """
    Export the fully enriched review data to CSV.
    """
    import pandas as pd
    import json

    data_dir = context.op_config.get("DATA_DIR", "./data")
    output_file = f"{data_dir}/enriched_data_2.csv"

    with open(input_file, 'r') as f:
        data = [json.loads(line) for line in f]

    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False)

    return output_file

# Define the job that sets dependencies

@job(
    resource_defs={
        "io_manager": dagster_file_manager.configured({"base_dir": "./data"})
    },
    retry_policy=RetryPolicy(max_retries=1)
)
def multilingual_product_review_analysis():
    data_file = load_and_modify_data()
    lang_detected_file = detect_language(data_file)
    sentiment_analyzed_file = analyze_sentiment(lang_detected_file)
    category_extracted_file = extract_categories(sentiment_analyzed_file)
    save_final_data(category_extracted_file)

# Launch pattern

if __name__ == '__main__':
    result = multilingual_product_review_analysis.execute_in_process(
        run_config={
            "ops": {
                "load_and_modify_data": {
                    "config": {
                        "DATA_DIR": "./data"
                    }
                },
                "analyze_sentiment": {
                    "config": {
                        "MODEL_NAME": "distilbert-base-uncased-finetuned-sst-2-english",
                        "DATA_DIR": "./data"
                    }
                },
                "extract_categories": {
                    "config": {
                        "EXTENDER_ID": "featureExtractor",
                        "DATA_DIR": "./data"
                    }
                },
                "save_final_data": {
                    "config": {
                        "DATA_DIR": "./data"
                    }
                }
            }
        }
    )