from dagster import job, op, resource, Field, String, execute_in_process

# Resource definitions
@resource(config_schema={"ftp_url": Field(String, default_value="ftp.ensembl.org", is_required=False),
                         "s3_bucket": Field(String, default_value="your-s3-bucket", is_required=False),
                         "s3_prefix": Field(String, default_value="raw/landing/ensembl/", is_required=False),
                         "spark_config": Field(dict, default_value={}, is_required=False)})
def ensembl_resources(context):
    return {
        "ftp_url": context.resource_config["ftp_url"],
        "s3_bucket": context.resource_config["s3_bucket"],
        "s3_prefix": context.resource_config["s3_prefix"],
        "spark_config": context.resource_config["spark_config"]
    }

# Op definitions
@op(required_resource_keys={"ensembl_resources"})
def check_and_download_files(context):
    """Checks for new data versions on the Ensembl FTP server and downloads them to S3 if necessary."""
    resources = context.resources.ensembl_resources
    data_types = ["canonical", "ena", "entrez", "refseq", "uniprot"]
    updated = False

    for data_type in data_types:
        # Check for new version
        latest_version = get_latest_version_from_ftp(resources["ftp_url"], data_type)
        current_version = get_current_version_from_s3(resources["s3_bucket"], resources["s3_prefix"], data_type)

        if latest_version > current_version:
            # Download and validate file
            file_path = download_file_from_ftp(resources["ftp_url"], data_type, latest_version)
            validate_file_integrity(file_path)
            upload_file_to_s3(file_path, resources["s3_bucket"], resources["s3_prefix"], data_type)
            updated = True

    if not updated:
        context.log.info("No new data versions found. Skipping pipeline.")
        raise Exception("No new data versions found. Pipeline skipped.")

def get_latest_version_from_ftp(ftp_url, data_type):
    # Placeholder for actual FTP version check logic
    return "1.0"

def get_current_version_from_s3(s3_bucket, s3_prefix, data_type):
    # Placeholder for actual S3 version check logic
    return "0.0"

def download_file_from_ftp(ftp_url, data_type, version):
    # Placeholder for actual FTP download logic
    return f"/path/to/{data_type}_{version}.tsv"

def validate_file_integrity(file_path):
    # Placeholder for actual file validation logic
    pass

def upload_file_to_s3(file_path, s3_bucket, s3_prefix, data_type):
    # Placeholder for actual S3 upload logic
    pass

@op(required_resource_keys={"ensembl_resources"})
def process_files_with_spark(context):
    """Processes the newly landed Ensembl data files from S3 using Spark."""
    resources = context.resources.ensembl_resources
    data_types = ["canonical", "ena", "entrez", "refseq", "uniprot"]

    spark = initialize_spark(resources["spark_config"])

    for data_type in data_types:
        file_path = f"s3a://{resources['s3_bucket']}/{resources['s3_prefix']}/{data_type}.tsv"
        df = spark.read.csv(file_path, header=True, sep="\t")
        transformed_df = transform_data(df)
        transformed_df.write.parquet(f"s3a://{resources['s3_bucket']}/processed/ensembl/{data_type}", mode="overwrite")

def initialize_spark(spark_config):
    # Placeholder for actual Spark initialization logic
    return None

def transform_data(df):
    # Placeholder for actual data transformation logic
    return df

# Job definition
@job(resource_defs={"ensembl_resources": ensembl_resources})
def ensembl_etl_pipeline():
    check_and_download_files().then(process_files_with_spark())

# Launch pattern
if __name__ == '__main__':
    result = ensembl_etl_pipeline.execute_in_process()