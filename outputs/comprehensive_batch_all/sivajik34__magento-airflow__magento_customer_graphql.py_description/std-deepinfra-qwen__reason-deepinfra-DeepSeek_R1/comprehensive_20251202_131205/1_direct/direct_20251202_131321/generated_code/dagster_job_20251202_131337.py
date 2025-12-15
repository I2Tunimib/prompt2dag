from dagster import op, job, RetryPolicy, resource, Field, String, execute_in_process

# Simplified resource for Magento GraphQL API connection
@resource(config_schema={"base_url": Field(String, default_value="https://magento.example.com/graphql")})
def magento_graphql_resource(context):
    class MagentoGraphQLClient:
        def __init__(self, base_url):
            self.base_url = base_url

        def execute_query(self, query, variables=None):
            # Simplified GraphQL execution
            import requests
            response = requests.post(self.base_url, json={"query": query, "variables": variables})
            response.raise_for_status()
            return response.json()

    return MagentoGraphQLClient(context.resource_config["base_url"])

@op(required_resource_keys={"magento_graphql"})
def create_customer(context):
    """Creates a new customer account and returns the customer's email address."""
    query = """
    mutation CreateCustomer($input: CustomerInput!) {
        createCustomer(input: $input) {
            customer {
                email
            }
        }
    }
    """
    variables = {
        "input": {
            "email": "new.customer@example.com",
            "firstname": "John",
            "lastname": "Doe",
            "password": "securepassword123"
        }
    }
    response = context.resources.magento_graphql.execute_query(query, variables)
    customer_email = response["data"]["createCustomer"]["customer"]["email"]
    return customer_email

@op(required_resource_keys={"magento_graphql"})
def generate_customer_token(context, customer_email):
    """Generates an authentication token for the customer and returns the token."""
    query = """
    mutation GenerateCustomerToken($email: String!, $password: String!) {
        generateCustomerToken(email: $email, password: $password) {
            token
        }
    }
    """
    variables = {
        "email": customer_email,
        "password": "securepassword123"
    }
    response = context.resources.magento_graphql.execute_query(query, variables)
    token = response["data"]["generateCustomerToken"]["token"]
    return token

@op(required_resource_keys={"magento_graphql"})
def get_customer_information(context, token):
    """Retrieves comprehensive customer profile data using the authentication token."""
    query = """
    query GetCustomerInformation {
        customer {
            firstname
            lastname
            email
            addresses {
                street
                city
                region
                postcode
                country_code
            }
        }
    }
    """
    headers = {"Authorization": f"Bearer {token}"}
    response = context.resources.magento_graphql.execute_query(query, headers=headers)
    customer_info = response["data"]["customer"]
    return customer_info

@job(
    resource_defs={"magento_graphql": magento_graphql_resource},
    retry_policy=RetryPolicy(max_retries=1, delay=300),
)
def customer_onboarding_job():
    customer_email = create_customer()
    token = generate_customer_token(customer_email)
    get_customer_information(token)

if __name__ == '__main__':
    result = customer_onboarding_job.execute_in_process()