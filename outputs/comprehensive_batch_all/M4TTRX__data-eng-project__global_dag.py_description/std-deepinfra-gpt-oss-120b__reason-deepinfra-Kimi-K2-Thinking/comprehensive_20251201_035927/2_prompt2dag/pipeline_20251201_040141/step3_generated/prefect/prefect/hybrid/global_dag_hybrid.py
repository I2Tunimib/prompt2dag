from prefect import flow

@flow(name="global_dag")
def global_dag():
    """Placeholder for the ETL pipeline."""
    pass

if __name__ == "__main__":
    global_dag()