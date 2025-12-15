from dagster import op, job, resource, RetryPolicy, Field, String, get_dagster_logger
import os
import requests

logger = get_dagster_logger()

@resource(config_schema={"data_dir": Field(String, default_value="/data")})
def data_dir_resource(context):
    return context.resource_config["data_dir"]

@op(required_resource_keys={"data_dir"})
def load_and_modify_data(context):
    data_dir = context.resources.data_dir
    input_file = os.path.join(data_dir, "suppliers.csv")
    output_file = os.path.join(data_dir, "table_data_2.json")
    
    response = requests.post(
        "http://localhost:3003/load-and-modify",
        json={
            "dataset_id": 2,
            "table_name_prefix": "JOT_",
            "input_file": input_file,
            "output_file": output_file
        }
    )
    response.raise_for_status()
    logger.info(f"Data loaded and modified: {output_file}")

@op(required_resource_keys={"data_dir"})
def reconcile_entities(context):
    data_dir = context.resources.data_dir
    input_file = os.path.join(data_dir, "table_data_2.json")
    output_file = os.path.join(data_dir, "reconciled_table_2.json")
    
    response = requests.post(
        "http://localhost:3003/reconcile",
        json={
            "primary_column": "supplier_name",
            "reconciliator_id": "wikidataEntity",
            "dataset_id": 2,
            "input_file": input_file,
            "output_file": output_file
        }
    )
    response.raise_for_status()
    logger.info(f"Entities reconciled: {output_file}")

@op(required_resource_keys={"data_dir"})
def save_final_data(context):
    data_dir = context.resources.data_dir
    input_file = os.path.join(data_dir, "reconciled_table_2.json")
    output_file = os.path.join(data_dir, "enriched_data_2.csv")
    
    response = requests.post(
        "http://localhost:3003/save",
        json={
            "dataset_id": 2,
            "input_file": input_file,
            "output_file": output_file
        }
    )
    response.raise_for_status()
    logger.info(f"Final data saved: {output_file}")

@job(
    resource_defs={"data_dir": data_dir_resource},
    retry_policy=RetryPolicy(max_retries=1)
)
def supplier_validation_job():
    load_and_modify_data().then(reconcile_entities).then(save_final_data)

if __name__ == "__main__":
    result = supplier_validation_job.execute_in_process()