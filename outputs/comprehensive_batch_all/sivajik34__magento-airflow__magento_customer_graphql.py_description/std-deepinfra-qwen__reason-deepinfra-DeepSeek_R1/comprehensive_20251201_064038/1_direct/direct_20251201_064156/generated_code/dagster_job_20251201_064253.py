from dagster import op, job, RetryPolicy, resource, Field, String, In, Out

# Simplified resource for Magento GraphQL API connection
@resource(config_schema={"url": Field(String, default_value="https://magento.example.com/graphql")})
def magento_graphql_resource(context):
    import requests

    class MagentoGraphQLClient:
        def __init__(self, url):
            self.url = url

        def execute(self, query, variables=None, headers=None):
            response = requests.post(self.url, json={"query": query, "variables": variables}, headers=headers)
            response.raise_for_status()
            return response.json()

    return MagentoGraphQLClient(context.resource_config["url"])


@op(required_resource_keys={"magento_graphql"}, out=Out(String, description="Customer email"))
def create_customer(context):
    """
    Executes a GraphQL mutation to create a new customer account.
    Returns the created customer's email address.
    """
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
    result = context.resources.magento_graphql.execute(query, variables)
    customer_email = result["data"]["createCustomer"]["customer"]["email"]
    return customer_email


@op(required_resource_keys={"magento_graphql"}, ins={"customer_email": In(String)}, out=Out(String, description="Bearer token"))
def generate_customer_token(context, customer_email):
    """
    Uses the customer email to execute a GraphQL mutation that generates an authentication token.
    Returns the generated bearer token.
    """
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
    result = context.resources.magento_graphql.execute(query, variables)
    token = result["data"]["generateCustomerToken"]["token"]
    return token


@op(required_resource_keys={"magento_graphql"}, ins={"token": In(String)})
def get_customer_information(context, token):
    """
    Uses the authentication token to execute a GraphQL query that retrieves comprehensive customer profile data.
    """
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
    result = context.resources.magento_graphql.execute(query, headers=headers)
    customer_info = result["data"]["customer"]
    context.log.info(f"Customer Information: {customer_info}")


@job(
    resource_defs={"magento_graphql": magento_graphql_resource},
    retry_policy=RetryPolicy(max_retries=1, delay=300),
)
def customer_management_job():
    customer_email = create_customer()
    token = generate_customer_token(customer_email)
    get_customer_information(token)


if __name__ == "__main__":
    result = customer_management_job.execute_in_process()