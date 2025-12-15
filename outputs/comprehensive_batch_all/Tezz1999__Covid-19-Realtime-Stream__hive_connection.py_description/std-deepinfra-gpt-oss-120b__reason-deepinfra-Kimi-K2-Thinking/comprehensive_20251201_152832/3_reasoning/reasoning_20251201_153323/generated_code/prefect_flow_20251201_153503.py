from prefect import flow, task
import subprocess
import os

try:
    from pyhive import hive
except ImportError:
    hive = None


def get_hive_connection_config(connection_name: str) -> dict:
    """Retrieve Hive connection configuration.
    
    In Airflow, this would come from the Connections DB.
    In Prefect, use Blocks or environment variables for secrets.
    """
    return {
        "host": os.environ.get("HIVE_HOST", "localhost"),
        "port": int(os.environ.get("HIVE_PORT", 10000)),
        "username": os.environ.get("HIVE_USER", "hive"),
        "database": "default"
    }


@task
def run_after_loop() -> str:
    """Execute bash command to echo current username."""
    result = subprocess.run(
        ["whoami"],
        capture_output=True,
        text=True,
        check=True
    )
    username = result.stdout.strip()
    print(f"Current username: {username}")
    return username


@task
def hive_script_task() -> None:
    """Execute Hive script to create database, table, and insert record."""
    if hive is None:
        raise ImportError(
            "pyhive package is required for Hive connectivity. "
            "Install with: pip install pyhive"
        )
    
    try:
        conn_config = get_hive_connection_config("hive_local")
        conn = hive.connect(**conn_config)
        cursor = conn.cursor()
        
        cursor.execute("CREATE DATABASE IF NOT EXISTS mydb")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mydb.test_af (
                test INT
            )
        """)
        cursor.execute("INSERT INTO mydb.test_af VALUES (2)")
        print("Successfully executed Hive operations")
        
    except Exception as e:
        print(f"Error executing Hive operations: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


@flow(name="hive-script")
def hive_script_flow() -> None:
    """Flow that executes bash command followed by Hive operations.
    
    Schedule: Daily at 1:00 AM (configure via deployment)
    Start Date: One day prior to deployment date (configure via deployment)
    """
    # Execute tasks sequentially: bash task then hive task
    run_after_loop()
    hive_script_task()


if __name__ == "__main__":
    hive_script_flow()