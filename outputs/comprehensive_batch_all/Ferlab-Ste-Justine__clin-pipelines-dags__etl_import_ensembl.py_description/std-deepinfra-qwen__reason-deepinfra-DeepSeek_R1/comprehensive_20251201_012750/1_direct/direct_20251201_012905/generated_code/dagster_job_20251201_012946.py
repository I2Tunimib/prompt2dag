from dagster import job, op, resource, execute_in_process

# Simplified resource stubs
@resource
def s3_resource():
    return {
        'bucket': 'your-s3-bucket',
        'prefix': 'raw/landing/ensembl/'
    }

@resource
def spark_resource():
    return {
        'master': 'k8s://https://your-k8s-cluster',
        'app_name': 'ensembl-etl'
    }

@resource
def ftp_resource():
    return {
        'host': 'ftp.ensembl.org',
        'path': '/pub/current_homo_sapiens/'
    }

@resource
def slack_resource():
    return {
        'webhook_url': 'https://your-slack-webhook-url'
    }

# Ops
@op(required_resource_keys={'ftp', 's3', 'slack'})
def check_and_upload_files(context):
    """
    Checks for new data versions on the Ensembl FTP server and uploads them to S3 if updated.
    """
    ftp = context.resources.ftp
    s3 = context.resources.s3
    slack = context.resources.slack

    data_types = ['canonical', 'ena', 'entrez', 'refseq', 'uniprot']
    updated = False

    for data_type in data_types:
        ftp_file_path = f"{ftp['path']}/{data_type}.tsv.gz"
        s3_file_path = f"{s3['prefix']}/{data_type}.tsv.gz"
        
        # Check if new version is available
        latest_version = context.resources.ftp.get_latest_version(ftp_file_path)
        current_version = context.resources.s3.get_current_version(s3_file_path)
        
        if latest_version > current_version:
            # Download and validate file
            file_content, md5_checksum = context.resources.ftp.download_file(ftp_file_path)
            if context.resources.ftp.validate_md5(file_content, md5_checksum):
                # Upload to S3
                context.resources.s3.upload_file(file_content, s3_file_path)
                updated = True
            else:
                context.log.error(f"MD5 checksum validation failed for {data_type}")
                slack['webhook_url'].send_message(f"MD5 checksum validation failed for {data_type}")
                return

    if not updated:
        context.log.info("No new data versions found. Skipping pipeline.")
        slack['webhook_url'].send_message("No new data versions found. Skipping pipeline.")
        return

@op(required_resource_keys={'s3', 'spark', 'slack'})
def process_files_with_spark(context):
    """
    Processes the newly landed Ensembl data files from S3 using Spark.
    """
    s3 = context.resources.s3
    spark = context.resources.spark
    slack = context.resources.slack

    data_types = ['canonical', 'ena', 'entrez', 'refseq', 'uniprot']
    for data_type in data_types:
        s3_file_path = f"{s3['prefix']}/{data_type}.tsv.gz"
        
        # Read TSV file from S3
        df = context.resources.spark.read.format('csv').option('header', 'true').option('delimiter', '\t').load(s3_file_path)
        
        # Apply transformations
        transformed_df = df.transform(lambda df: df.withColumnRenamed('old_column', 'new_column'))
        
        # Load into structured table
        transformed_df.write.format('parquet').mode('overwrite').save(f"{s3['prefix']}/tables/ensembl_mapping/{data_type}")

    slack['webhook_url'].send_message("Pipeline completed successfully.")

# Job
@job(resource_defs={'ftp': ftp_resource, 's3': s3_resource, 'spark': spark_resource, 'slack': slack_resource})
def ensembl_etl_job():
    process_files_with_spark(check_and_upload_files())

if __name__ == '__main__':
    result = ensembl_etl_job.execute_in_process()