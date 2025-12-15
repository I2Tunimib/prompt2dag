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
        "content": Out(str, "Content to be processed further.")
    }
)
def toxicity_check(context, metadata):
    # Mock toxicity check logic
    toxic_content = True  # Example: 70% of content is toxic
    content = "Sample content"
    return toxic_content, content

@op(
    description="Removes toxic content from the platform and flags the associated user account for review.",
    ins={"toxic_content": In(bool, "True if content is toxic."),
         "content": In(str, "Content to be processed further.")},
    out={"action_result": Out(str, "Result of the action taken.")}
)
def remove_and_flag_content(context, toxic_content, content):
    if toxic_content:
        # Mock removal and flagging logic
        action_result = "Toxic content removed and user flagged."
    else:
        action_result = "No toxic content to remove."
    return action_result

@op(
    description="Publishes the safe content to the platform for user visibility.",
    ins={"toxic_content": In(bool, "True if content is toxic."),
         "content": In(str, "Content to be processed further.")},
    out={"action_result": Out(str, "Result of the action taken.")}
)
def publish_content(context, toxic_content, content):
    if not toxic_content:
        # Mock publishing logic
        action_result = "Safe content published."
    else:
        action_result = "Content is toxic, not published."
    return action_result

@op(
    description="Collects results from both possible upstream branches and creates a final audit log entry.",
    ins={"remove_result": In(str, "Result of the remove and flag content task."),
         "publish_result": In(str, "Result of the publish content task.")},
    out={"audit_log": Out(str, "Final audit log entry.")}
)
def audit_log(context, remove_result, publish_result):
    # Mock audit log creation logic
    audit_log_entry = f"Remove Result: {remove_result}, Publish Result: {publish_result}"
    return audit_log_entry

@job(
    description="Content moderation pipeline that scans, evaluates, and processes user-generated content.",
    resource_defs={},
    # resources={"csv": csv_resource},
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def content_moderation_pipeline():
    metadata = scan_csv()
    toxic_content, content = toxicity_check(metadata)
    
    remove_result = remove_and_flag_content(toxic_content, content)
    publish_result = publish_content(toxic_content, content)
    
    audit_log(remove_result, publish_result)

if __name__ == '__main__':
    result = content_moderation_pipeline.execute_in_process()