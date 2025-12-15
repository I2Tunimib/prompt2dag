from dagster import op, job, RetryPolicy, In, Out, Nothing

# Simplified resource example
# from dagster import ResourceDefinition
# csv_resource = ResourceDefinition.mock_resource()

@op(
    description="Scans a CSV file for user-generated content and returns metadata.",
    out={"metadata": Out(dict, "Metadata including the total number of items found.")}
)
def scan_csv(context):
    # Mock CSV scanning logic
    metadata = {"total_items": 100}
    return metadata

@op(
    description="Evaluates the toxicity level of the content.",
    ins={"metadata": In(dict, "Metadata from the CSV scan")},
    out={
        "toxic_content": Out(bool, "True if content is toxic, False otherwise."),
        "content_id": Out(str, "Unique identifier for the content.")
    }
)
def toxicity_check(context, metadata):
    # Mock toxicity check logic
    content_id = "12345"
    toxic_content = True  # Example: content is toxic
    return toxic_content, content_id

@op(
    description="Removes toxic content from the platform and flags the associated user account for review.",
    ins={"content_id": In(str, "Unique identifier for the toxic content.")},
    out={"action_result": Out(str, "Result of the action taken.")}
)
def remove_and_flag_content(context, content_id):
    # Mock removal and flagging logic
    action_result = f"Content {content_id} removed and user flagged."
    return action_result

@op(
    description="Publishes the safe content to the platform for user visibility.",
    ins={"content_id": In(str, "Unique identifier for the safe content.")},
    out={"action_result": Out(str, "Result of the action taken.")}
)
def publish_content(context, content_id):
    # Mock publishing logic
    action_result = f"Content {content_id} published."
    return action_result

@op(
    description="Collects results from both possible upstream branches and creates a final audit log entry.",
    ins={
        "toxic_content": In(bool, "True if content was toxic, False otherwise."),
        "action_result": In(str, "Result of the action taken in the previous step."),
        "content_id": In(str, "Unique identifier for the content.")
    },
    out={"audit_log_entry": Out(str, "Final audit log entry.")}
)
def audit_log(context, toxic_content, action_result, content_id):
    # Mock audit log creation logic
    audit_log_entry = f"Content ID: {content_id}, Toxic: {toxic_content}, Action: {action_result}"
    return audit_log_entry

@job(
    description="Content moderation pipeline that scans, evaluates, and processes user-generated content.",
    resource_defs={
        # "csv_resource": csv_resource
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def content_moderation_pipeline():
    metadata = scan_csv()
    toxic_content, content_id = toxicity_check(metadata)

    def branch(context, toxic_content):
        if toxic_content:
            return remove_and_flag_content(content_id)
        else:
            return publish_content(content_id)

    action_result = branch(toxic_content)

    audit_log_entry = audit_log(toxic_content, action_result, content_id)

if __name__ == '__main__':
    result = content_moderation_pipeline.execute_in_process()