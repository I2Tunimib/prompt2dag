from prefect import flow

@flow(name="global_dag")
def global_dag():
    """Empty ETL pipeline."""
    pass

if __name__ == "__main__":
    global_dag()