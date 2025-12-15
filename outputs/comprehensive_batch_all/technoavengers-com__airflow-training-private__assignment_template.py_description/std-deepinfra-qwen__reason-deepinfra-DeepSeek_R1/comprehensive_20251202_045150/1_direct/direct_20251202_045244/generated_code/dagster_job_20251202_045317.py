from dagster import op, job, get_dagster_logger, In, Out, Nothing
import requests
import psycopg2
from datetime import datetime, timedelta

# Resources
class ReqresResource:
    def __init__(self, base_url):
        self.base_url = base_url

class PostgresResource:
    def __init__(self, connection_string):
        self.connection_string = connection_string

    def execute_query(self, query, params=None):
        with psycopg2.connect(self.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                conn.commit()

# Ops
@op(required_resource_keys={"reqres"}, out=Out(dict))
def get_user(context):
    """
    Executes an HTTP GET request to the reqres API endpoint to retrieve user data in JSON format.
    """
    response = requests.get(f"{context.resources.reqres.base_url}/api/users/2")
    response.raise_for_status()
    return response.json()

@op(ins={"user_data": In(dict)}, out=Out(dict))
def process_user(context, user_data):
    """
    Processes the API response by parsing JSON data and extracting first name, last name, and email fields.
    """
    processed_data = {
        "firstname": user_data["data"]["first_name"],
        "lastname": user_data["data"]["last_name"],
        "email": user_data["data"]["email"]
    }
    return processed_data

@op(required_resource_keys={"postgres"}, out=Out(Nothing))
def create_table(context):
    """
    Creates a PostgreSQL users table with columns for firstname, lastname, and email.
    """
    query = """
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        firstname VARCHAR(255),
        lastname VARCHAR(255),
        email VARCHAR(255)
    );
    """
    context.resources.postgres.execute_query(query)

@op(ins={"processed_data": In(dict)}, required_resource_keys={"postgres"}, out=Out(Nothing))
def insert_data(context, processed_data):
    """
    Inserts the processed user data into the PostgreSQL users table.
    """
    query = """
    INSERT INTO users (firstname, lastname, email) VALUES (%s, %s, %s);
    """
    params = (processed_data["firstname"], processed_data["lastname"], processed_data["email"])
    context.resources.postgres.execute_query(query, params)

# Job
@job(
    resource_defs={
        "reqres": ReqresResource("https://reqres.in"),
        "postgres": PostgresResource("dbname=yourdb user=youruser password=yourpassword host=localhost port=5432")
    }
)
def user_pipeline():
    """
    A Dagster job that fetches user data from an external API, processes the extracted information,
    creates a PostgreSQL table, and inserts the processed data.
    """
    processed_data = process_user(get_user())
    create_table()
    insert_data(processed_data)

# Launch pattern
if __name__ == '__main__':
    result = user_pipeline.execute_in_process()