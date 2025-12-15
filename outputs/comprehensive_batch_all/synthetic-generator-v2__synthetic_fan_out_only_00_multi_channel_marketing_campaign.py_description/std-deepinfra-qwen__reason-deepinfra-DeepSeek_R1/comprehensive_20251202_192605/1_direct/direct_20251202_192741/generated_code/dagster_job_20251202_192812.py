from dagster import op, job, In, Out, RetryPolicy, daily_schedule, ResourceDefinition, String

# Simplified resource example for sending campaigns
class CampaignResource:
    def send_email(self, customer_segment):
        print(f"Sending email campaign to {customer_segment}")

    def send_sms(self, customer_segment):
        print(f"Sending SMS campaign to {customer_segment}")

    def send_push_notification(self, customer_segment):
        print(f"Sending push notification to {customer_segment}")

campaign_resource = ResourceDefinition.hardcoded_resource(CampaignResource())

@op(required_resource_keys={"campaign_resource"}, out=Out(String))
def load_customer_segment_csv(context):
    """Loads customer segment data from a CSV file."""
    # Simulate loading data from a CSV file
    customer_segment = "premium_segment"
    context.log.info(f"Loaded customer segment: {customer_segment}")
    return customer_segment

@op(required_resource_keys={"campaign_resource"})
def send_email_campaign(context, customer_segment):
    """Sends email campaign to the premium customer segment."""
    context.resources.campaign_resource.send_email(customer_segment)

@op(required_resource_keys={"campaign_resource"})
def send_sms_campaign(context, customer_segment):
    """Sends SMS campaign to the customer segment."""
    context.resources.campaign_resource.send_sms(customer_segment)

@op(required_resource_keys={"campaign_resource"})
def send_push_notification(context, customer_segment):
    """Sends push notification campaign to mobile app users."""
    context.resources.campaign_resource.send_push_notification(customer_segment)

@job(
    resource_defs={"campaign_resource": campaign_resource},
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def marketing_campaign_job():
    """Multi-channel marketing campaign job with a fan-out pattern."""
    customer_segment = load_customer_segment_csv()
    send_email_campaign(customer_segment)
    send_sms_campaign(customer_segment)
    send_push_notification(customer_segment)

@daily_schedule(
    pipeline_name="marketing_campaign_job",
    start_date="2024-01-01",
    execution_time="00:00",
    execution_timezone="UTC",
    catchup=False
)
def marketing_campaign_daily_schedule():
    return {}

if __name__ == '__main__':
    result = marketing_campaign_job.execute_in_process()