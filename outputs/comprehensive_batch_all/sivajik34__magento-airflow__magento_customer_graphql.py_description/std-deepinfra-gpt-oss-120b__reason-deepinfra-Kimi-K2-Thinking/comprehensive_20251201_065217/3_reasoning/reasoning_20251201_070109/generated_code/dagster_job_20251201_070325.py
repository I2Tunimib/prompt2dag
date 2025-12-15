from dagster import (
    op,
    job,
    resource,
    Field,
    String,
    RetryPolicy,
    OpExecutionContext,
    Out,
    In,
    Dict,
    Any,
)
import logging
import time
from typing import Dict as TypingDict, Any as TypingAny

# Minimal Magento GraphQL client resource stub
@resource(
    config_schema={
        "graphql_endpoint": Field(String, description="Magento GraphQL endpoint URL"),
        "api_key": Field(String, is_required=False, description="Optional API key"),
    }
)
def magento_graphql_client(context):
    """Stub for Magento GraphQL client - replace with actual implementation."""
    
    class MagentoClient:
        def __init__(self, endpoint: str, api_key: str = None):
            self.endpoint = endpoint
            self.api_key = api_key
            self.log = context.log
        
        def execute_mutation(self, query: str, variables: TypingDict[str, TypingAny]) -> TypingDict[str, TypingAny]:
            """Execute GraphQL mutation."""
            self.log.info(f"Executing mutation: {query.split()[0]}...")
            time.sleep(0.1)
            return {"data": {}}
        
        def execute_query(self, query: str, variables: TypingDict[str, TypingAny]) -> TypingDict[str, TypingAny]:
            """Execute GraphQL query."""
            self.log.info(f"Executing query: {query.split()[0]}...")
            time.sleep(0.1)
            return {"data": {}}
    
    return MagentoClient(
        endpoint=context.resource_config["graphql_endpoint"],
        api_key=context.resource_config.get("api_key"),
    )


# Retry policy: 1 retry with 5-minute delay
magento_retry_policy = RetryPolicy(max_retries=1, delay=300)


@op(
    out={"customer_email": Out(String)},
    required_resource_keys={"magento_default"},
    retry_policy=magento_retry_policy,
)
def create_customer(context: OpExecutionContext) -> str:
    """Create a new Magento customer account via GraphQL mutation."""
    try:
        context.log.info("Creating new customer account")
        
        # In production, load from config or secrets
        customer_data = {
            "firstname": "John",
            "lastname": "Doe",
            "email": "john.doe@example.com",
            "password": "SecurePassword123!",
        }
        
        mutation = """
        mutation createCustomer($input: CustomerInput!) {
            createCustomer(input: $input) {
                customer { email }
            }
        }
        """
        
        context.resources.magento_default.execute_mutation(mutation, {"input": customer_data})
        
        context.log.info(f"Customer created: {customer_data['email']}")
        return customer_data["email"]
        
    except Exception as e:
        context.log.error(f"Customer creation failed: {e}")
        raise


@op(
    ins={"customer_email": In(String)},
    out={"bearer_token": Out(String)},
    required_resource_keys={"magento_default"},
    retry_policy=magento_retry_policy,
)
def generate_customer_token(context: OpExecutionContext, customer_email: str) -> str:
    """Generate authentication token for the customer."""
    try:
        context.log.info(f"Generating token for {customer_email}")
        
        # In production, retrieve password securely
        password = "SecurePassword123!"
        
        mutation = """
        mutation generateCustomerToken($email: String!, $password: String!) {
            generateCustomerToken(email: $email, password: $password) { token }
        }
        """
        
        context.resources.magento_default.execute_mutation(
            mutation, {"email": customer_email, "password": password}
        )
        
        token = "fake-bearer-token-12345"  # Parse from response in production
        context.log.info("Token generated successfully")
        return token
        
    except Exception as e:
        context.log.error(f"Token generation failed for {customer_email}: {e}")
        raise


@op(
    ins={"bearer_token": In(String)},
    out={"customer_profile": Out(Dict[String, Any])},
    required_resource_keys={"magento_default"},
    retry_policy=magento_retry_policy,
)
def get_customer_information(context: OpExecutionContext, bearer_token: str) -> TypingDict[str, TypingAny]:
    """Retrieve comprehensive customer profile data."""
    try:
        context.log.info("Retrieving customer profile")
        
        query = """
        query getCustomer {
            customer {
                firstname
                lastname
                email
                addresses {
                    firstname
                    lastname
                    street
                    city
                    region
                    postcode
                    country_code
                }
            }
        }
        """
        
        context.resources.magento_default.execute_query(query, {})
        
        # Mock profile - parse from GraphQL response in production
        profile = {
            "firstname": "John",
            "lastname": "Doe",
            "email": "john.doe@example.com",
            "addresses": [
                {
                    "street": ["123 Main St"],
                    "city": "New York",
                    "region": "NY",
                    "postcode": "10001",
                    "country_code": "US",
                }
            ],
        }
        
        context.log.info("Customer profile retrieved successfully")
        return profile
        
    except Exception as e:
        context.log.error(f"Failed to retrieve customer profile: {e}")
        raise


@job(
    resource_defs={"magento_default": magento_graphql_client},
    config={
        "resources": {
            "magento_default": {
                "config": {
                    "graphql_endpoint": "https://your-magento-instance.com/graphql",
                    "api_key": "your-api-key",
                }
            }
        }
    },
)
def magento_customer_onboarding_job():
    """
    Linear pipeline for Magento customer onboarding:
    1. Create customer → 2. Generate token → 3. Retrieve profile
    """
    customer_email = create_customer()
    bearer_token = generate_customer_token(customer_email)
    get_customer_information(bearer_token)


if __name__ == "__main__":
    # Manual execution only (no schedule, catchup implicitly disabled)
    result = magento_customer_onboarding_job.execute_in_process()
    
    if result.success:
        print("✅ Pipeline executed successfully!")
    else:
        print("❌ Pipeline execution failed!")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                print(f"   Failed step: {event.step_key}")