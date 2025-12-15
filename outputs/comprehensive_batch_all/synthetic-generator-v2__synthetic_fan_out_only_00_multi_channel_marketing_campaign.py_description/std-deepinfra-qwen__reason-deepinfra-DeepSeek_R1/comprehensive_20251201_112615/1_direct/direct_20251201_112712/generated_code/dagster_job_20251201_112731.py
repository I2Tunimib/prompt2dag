from dagster import op, job, RetryPolicy, In, Out, graph, daily_schedule, ResourceDefinition, String

# Simplified resource stubs
class EmailClient:
    def send_campaign(self, customer_segment):
        print(f"Sending email campaign to {customer_segment}")

class SMSClient:
    def send_campaign(self, customer_segment):
        print(f"Sending SMS campaign to {customer_segment}")

class PushNotificationClient:
    def send_campaign(self, customer_segment):
        print(f"Sending push notification campaign to {customer_segment}")

email_client = ResourceDefinition.hardcoded_resource(EmailClient())
sms_client = ResourceDefinition.hardcoded_resource(SMSClient())
push_notification_client = ResourceDefinition.hardcoded_resource(PushNotificationClient())

@op(required_resource_keys={"email_client"}, retry_policy=RetryPolicy(max_retries=2, delay=300))
def send_email_campaign(context, customer_segment: str):
    """Send email campaign to the specified customer segment."""
    context.resources.email_client.send_campaign(customer_segment)

@op(required_resource_keys={"sms_client"}, retry_policy=RetryPolicy(max_retries=2, delay=300))
def send_sms_campaign(context, customer_segment: str):
    """Send SMS campaign to the specified customer segment."""
    context.resources.sms_client.send_campaign(customer_segment)

@op(required_resource_keys={"push_notification_client"}, retry_policy=RetryPolicy(max_retries=2, delay=300))
def send_push_notification(context, customer_segment: str):
    """Send push notification campaign to the specified customer segment."""
    context.resources.push_notification_client.send_campaign(customer_segment)

@op(out={"customer_segment": Out(dagster_type=str)})
def load_customer_segment_csv(context):
    """Load customer segment data from a CSV file."""
    # Simplified data loading logic
    customer_segment = "premium_customers"
    context.log.info(f"Loaded customer segment: {customer_segment}")
    return customer_segment

@graph
def marketing_campaign_graph():
    customer_segment = load_customer_segment_csv()
    send_email_campaign(customer_segment)
    send_sms_campaign(customer_segment)
    send_push_notification(customer_segment)

@job(resource_defs={"email_client": email_client, "sms_client": sms_client, "push_notification_client": push_notification_client})
def marketing_campaign_job():
    marketing_campaign_graph()

@daily_schedule(
    pipeline_name="marketing_campaign_job",
    start_date="2024-01-01",
    execution_time="00:00",
    execution_timezone="UTC",
    name="daily_marketing_campaign",
    tags={"owner": "Marketing team"},
    solid_selection=None,
    mode="default",
    should_execute=None,
    environment_vars=None,
    partition_fn=None,
    execution_day_of_month=None,
    execution_day_of_week=None,
    execution_time_window=None,
    end_date=None,
    cron_schedule="0 0 * * *",
    description="Daily marketing campaign job",
    tags_fn=None,
    solid_subset=None,
    execution_timezone="UTC",
    schedule_name="daily_marketing_campaign",
    tags={"owner": "Marketing team"},
)
def daily_marketing_campaign():
    marketing_campaign_job()

if __name__ == "__main__":
    result = marketing_campaign_job.execute_in_process()