<think>
Okay, let's tackle this Dagster job conversion. The user wants a Python module that represents the described pipeline with all the constraints. First, I need to break down the pipeline steps and map them to Dagster ops and jobs.

Starting with the steps: ingest_csv, quality_check, then conditional branches for production_load or quarantine_and_alert followed by send_alert_email, and finally cleanup. The dependencies are ingest_csv → quality_check → [branches] → cleanup. The branches are parallel, so after quality_check, either production_load or quarantine_and_alert runs. Then, send_alert_email only if quarantine happens, and cleanup depends on both paths.

Wait, the description says that quarantine_and_alert is followed by send_alert_email. So the flow is: if quality score is low, quarantine_and_alert runs, then send_alert_email, then cleanup. But the production path goes directly to cleanup. So cleanup depends on both production_load and send_alert_email? Or does send_alert_email only happen in the quarantine branch, and cleanup depends on both branches merging?

Looking at the pipeline steps: after quality_check branches, the production path is production_load, and the quarantine path is quarantine_and_alert → send_alert_email. Then both paths converge to cleanup. So cleanup must depend on both production_load and send_alert_email. That way, regardless of which branch is taken, cleanup runs after both possible paths.

So the dependencies would be:

ingest_csv → quality_check

quality_check → production_load (if score >=95)

quality_check → quarantine_and_alert → send_alert_email (if score <95)

Then cleanup depends on both production_load and send_alert_email. But how to model that in Dagster? Because in Dagster, each op's output can be used as input to others. So the cleanup op needs to have inputs from both possible paths. But if one path isn't executed, how does Dagster handle it?

Wait, in Dagster, if an op is not executed (because the condition wasn't met), its output won't be available. So if we have cleanup depending on both production_load and send_alert_email, but in a run where only production_load runs, send_alert_email wasn't executed. That would cause an error because the input from send_alert_email isn't present.

So that approach won't work. Instead, the cleanup should depend on both branches, but in a way that allows either path to have been taken. One way is to have both branches output something that is passed to cleanup, but using optional inputs or a fan-in approach where cleanup doesn't require both inputs. Alternatively, use a conditional within the cleanup op to check which inputs are present.

Alternatively, structure the dependencies so that cleanup runs after both possible paths. For example, in the production path: production_load → cleanup. In the quarantine path: send_alert_email → cleanup. But that would require cleanup to run after both, which isn't possible unless we have a way to collect both. Hmm.

Wait, maybe the cleanup should depend on both production_load and send_alert_email. But in cases where production_load isn't executed (because the score was low), then the input from production_load would be missing. Similarly, if send_alert_email isn't executed (because the score was high), that input is missing. So this approach would fail.

So perhaps the correct way is to have both branches end with an op that outputs to cleanup. For example, production_load outputs a result, and send_alert_email outputs a result. Then cleanup takes both as optional inputs. But in Dagster, if an op's input is not provided by any upstream op, the job will fail unless the input is marked as optional.

So in the cleanup op, define the inputs as optional. That way, regardless of which path was taken, cleanup can receive whichever inputs are available.

Alternatively, have both branches produce some output that is passed to cleanup. For example, production_load could output a "prod_done" signal, and send_alert_email could output an "alert_sent" signal. Then cleanup depends on either of those. But how to model that in Dagster's dependency graph.

Wait, in Dagster, you can have an op depend on multiple upstream ops. For example, cleanup can require both production_load and send_alert_email to have run. But in reality, only one of them will run based on the condition. So this would cause an error because one of the upstream ops wasn't executed.

Therefore, this suggests that the initial approach of having conditional branching within an op is better. The quality_check op decides which path to take, and then subsequent ops are conditionally executed. However, Dagster's execution model requires that the dependencies are statically defined. So you can't dynamically choose which ops to run based on some condition within another op.

Wait, but Dagster does allow for conditional execution using the concept of "op outputs" that determine which downstream ops are run. For example, using the Output and DynamicOutput features with mappings. Alternatively, using a fan-in approach where multiple ops are downstream but only one is executed based on some condition.

Alternatively, perhaps the quality_check op returns a result that indicates which branch to take, and then subsequent ops are conditionally executed based on that. But in Dagster, the dependencies are defined statically in the job, so you can't have dynamic dependencies based on op outputs. So the way to handle this is to have both possible downstream ops (production_load and quarantine_and_alert) depend on quality_check, and within those ops, decide whether to execute based on the quality score.

Wait, but that's not possible because the quality_check's output determines which path to take. So perhaps the quality_check op returns the quality score and a decision, and then the next ops check that decision. But in that case, both production_load and quarantine_and_alert would always run, but they would have logic inside to check if they should proceed.

But that's not ideal because we don't want both ops to run; only one should execute based on the condition. So how to model this in Dagster?

Ah, here's the solution: use Dagster's conditional outputs. The quality_check op can yield either a "high_quality" output or a "low_quality" output, and then map those to the respective downstream ops. This requires using DynamicOutput and mapping each possible output to the appropriate op.

Wait, but DynamicOutput is part of the experimental API. Alternatively, using the built-in Output and defining multiple outputs, then having the downstream ops depend on the specific output.

So, the quality_check op can have two outputs: one for high quality and one for low. Then, in the job definition, we can connect the high quality output to production_load, and the low quality output to quarantine_and_alert. Then, send_alert_email depends on quarantine_and_alert, and cleanup depends on both production_load and send_alert_email.

But how to structure the outputs. Let's see:

In the quality_check op, based on the score, we can return a specific output. For example:

If score >= 95, yield Output(..., "high_quality")
Else, yield Output(..., "low_quality")

Then, in the job, we can have:

@job
def data_quality_gate():
    high_quality, low_quality = quality_check(ingest_csv())
    production_load(high_quality)
    quarantine_result = quarantine_and_alert(low_quality)
    send_alert_email(quarantine_result)
    cleanup(production_load(), send_alert_email())

Wait, but in this case, if the quality_check yields only one output (either high or low), then the downstream ops will only receive that output. So if the high_quality output is yielded, production_load runs, and low_quality is not, so quarantine_and_alert doesn't run. Similarly, if low_quality is yielded, quarantine_and_alert runs, and production_load doesn't. Then, send_alert_email depends on quarantine_and_alert's output, so it runs only in the low case. Then cleanup depends on both production_load and send_alert_email. But in the high case, send_alert_email wasn't executed, so cleanup would have a missing input. So that's a problem.

So perhaps the cleanup op should not depend on both, but instead have a way to collect the necessary inputs regardless of which path was taken. Alternatively, have both paths end with an op that feeds into cleanup. For example, production_load → cleanup, and send_alert_email → cleanup. But how to structure that.

Wait, perhaps the cleanup op should take no inputs, but in the job definition, it depends on both production_load and send_alert_email. But if one of those ops didn't run, then the dependency would be missing. So Dagster would throw an error.

Hmm. So this suggests that the initial approach of using conditional outputs and structuring the dependencies accordingly is the way to go. Let's try to outline the code.

First, define the ops:

- ingest_csv: returns file metadata.
- quality_check: takes the metadata, computes score, returns either high or low output.
- production_load: takes the high output.
- quarantine_and_alert: takes the low output.
- send_alert_email: takes the output from quarantine_and_alert.
- cleanup: depends on both production_load and send_alert_email.

But how to make cleanup depend on both when only one path is taken. Because if production_load runs, send_alert_email doesn't, and vice versa. So cleanup would have a dependency on two ops, but only one of them is executed. In Dagster, this would result in an error because the input is not provided.

So perhaps the solution is to have both paths end with an op that produces an output that cleanup can depend on. For example, production_load produces a "prod_result" and send_alert_email produces an "alert_result". Then cleanup depends on both, but in practice, only one is present. But how to handle that.

Alternatively, have cleanup take optional inputs. So in the op definition, the inputs are optional. For example:

@op
def cleanup(context, prod_result: Optional[str] = None, alert_result: Optional[str] = None):
    # perform cleanup

Then, in the job, connect both production_load and send_alert_email to cleanup's inputs. But in cases where one is not executed, the input would be None. However, in Dagster, if an op's input is not provided by any upstream op, the job will fail unless the input is marked as optional with a default value.

So, by defining the inputs with default values (like None), the op can accept either or both inputs. Then, in the job, connect both production_load and send_alert_email to the respective inputs of cleanup.

But how to structure the job dependencies. Let's see:

After quality_check, if high_quality, production_load runs. Then cleanup depends on production_load's output. If low_quality, quarantine_and_alert runs, then send_alert_email runs, and cleanup depends on send_alert_email's output. But how to make cleanup depend on both possible paths.

Wait, perhaps the job should be structured such that cleanup is downstream of both production_load and send_alert_email. So regardless of which path is taken, cleanup runs after. But in Dagster, if an op is downstream of another, it will only run if all its upstream dependencies have executed. But in this case, only one of the upstream dependencies (production_load or send_alert_email) will have executed. So this would cause an error.

Therefore, the correct approach is to use Dagster's conditional execution with multiple outputs and have cleanup depend on both possible paths, but allow the inputs to be optional.

So here's the plan:

1. ingest_csv op runs first.
2. quality_check op takes ingest_csv's output, computes score, and returns two outputs: one for high quality, one for low. But only one of these outputs is actually yielded based on the score.
3. production_load depends on the high quality output.
4. quarantine_and_alert depends on the low quality output.
5. send_alert_email depends on quarantine_and_alert's output.
6. cleanup op takes optional inputs from both production_load and send_alert_email. So in the job, cleanup depends on both, but in practice, only one will have a value.

But how to define multiple outputs in the quality_check op. In Dagster, an op can have multiple outputs, and each can be connected to different downstream ops.

So, the quality_check op would have two outputs: 'high_quality' and 'low_quality'. Based on the score, it yields one of them. Then, in the job, production_load is connected to the 'high_quality' output, and quarantine_and_alert to the 'low_quality' output.

Then, send_alert_email depends on quarantine_and_alert's output, and cleanup depends on both production_load and send_alert_email's outputs. But since only one path is taken, cleanup will receive one input and the other will be None (if the inputs are optional).

So, let's code this.

First, the ingest_csv op:

@op
def ingest_csv(context) -> dict:
    # Ingest CSV logic here
    return {"path": "/data/customers.csv", "metadata": {}}

Then, quality_check with two outputs:

@op(out={"high_quality": Out(is_required=False), "low_quality": Out(is_required=False)})
def quality_check(context, csv_data: dict):
    # Compute quality score
    score = 96  # Example value, in real scenario compute this
    if score >= 95:
        context.log.info("High quality data")
        yield Output(csv_data, "high_quality")
    else:
        context.log.info("Low quality data")
        yield Output(csv_data, "low_quality")

Wait, but using yield Output with different output names. Then, in the job, we can map these outputs to downstream ops.

Then, production_load depends on the 'high_quality' output:

@op
def production_load(context, data: dict):
    # Load to production database
    context.log.info("Loading to production")

quarantine_and_alert depends on 'low_quality' output:

@op
def quarantine_and_alert(context, data: dict):
    # Move to quarantine and prepare alert
    context.log.info("Quarantining data")
    return {"alert_details": "Data quarantined"}

send_alert_email depends on quarantine_and_alert's output:

@op
def send_alert_email(context, alert_details: dict):
    # Send email alert
    context.log.info("Sending alert email")

Then, cleanup op takes optional inputs from both paths:

@op
def cleanup(context, prod_result: Optional[dict] = None, alert_result: Optional[dict] = None):
    # Cleanup logic
    context.log.info("Performing cleanup")

Now, the job definition:

@job
def data_quality_gate():
    csv_data = ingest_csv()
    quality_result = quality_check(csv_data)
    prod_data = production_load(quality_result.high_quality)
    quarantine_data = quarantine_and_alert(quality_result.low_quality)
    alert_data = send_alert_email(quarantine_data)
    cleanup(prod_result=prod_data, alert_result=alert_data)

Wait, but in this setup, if quality_check yields 'high_quality', then 'low_quality' output is not present. So when the job tries to pass quality_result.low_quality to quarantine_and_alert, it would be missing. Similarly, if 'low_quality' is yielded, 'high_quality' is missing, causing production_load to fail.

So this approach won't work because Dagster requires that all inputs are satisfied. Even if the op's output is marked as is_required=False, the downstream op's input must be optional.

Wait, no. The 'quality_check' op's outputs are defined with is_required=False, meaning that they may or may not be present. Then, the downstream ops (production_load and quarantine_and_alert) must have their inputs marked as optional.

So, modifying the production_load and quarantine_and_alert ops to accept optional inputs:

@op
def production_load(context, data: Optional[dict] = None):
    if data is None:
        context.log.info("Skipping production load")
        return
    # Proceed with loading

Similarly for quarantine_and_alert:

@op
def quarantine_and_alert(context, data: Optional[dict] = None):
    if data is None:
        context.log.info("Skipping quarantine")
        return {"alert_details": None}
    # Proceed with quarantine
    return {"alert_details": "Data quarantined"}

But this way, even if the input is None (because the other branch was taken), the op can handle it. However, this isn't ideal because we don't want production_load to run when the data is None. But in Dagster's execution model, once an op is included in the job, it will run unless there's a conditional. So this approach would cause both production_load and quarantine_and_alert to run every time, but with data being None in one of them.

That's not desired. We want only one of them to run based on the quality check.

Therefore, the correct approach is to use Dagster's graph and conditional outputs properly. The key is to use the fact that if an op's output is not yielded, downstream ops that depend on it will not execute.

So, in the quality_check op, we yield either the 'high_quality' or 'low_quality' output. Then, in the job, production_load depends on 'high_quality', and quarantine_and_alert depends on 'low_quality'. Therefore, only one of them will execute based on which output is yielded.

But how to structure the job to connect these outputs.

In the job definition, after getting the quality_result (which is a tuple of outputs), we can pass the specific outputs to the respective ops.

So:

@job
def data_quality_gate():
    csv_data = ingest_csv()
    high_quality, low_quality = quality_check(csv_data)
    prod_result = production_load(high_quality)
    quarantine_result = quarantine_and_alert(low_quality)
    alert_result = send_alert_email(quarantine_result)
    cleanup(prod_result, alert_result)

Wait, but how does Dagster handle multiple outputs from an op? The quality_check op yields either high_quality or low_quality. So when we unpack the outputs, if one is not present, will that cause an error?

No, because in Dagster, when an op has multiple outputs, you can access them by name. So perhaps the correct way is to use the getattr approach or refer to them via the output names.

Alternatively, when defining the job, you can specify which output to use for each downstream op.

For example:

@job
def data_quality_gate():
    csv_data = ingest_csv()
    quality_outputs = quality_check(csv_data)
    prod_result = production_load(quality_outputs.high_quality)
    quarantine_result = quarantine_and_alert(quality_outputs.low_quality)
    alert_result = send_alert_email(quarantine_result)
    cleanup(prod_result, alert_result)

In this case, if quality_check yields 'high_quality', then quality_outputs.high_quality is present, and production_load runs. quality_outputs.low_quality is not present, so quarantine_and_alert will not run because its input is missing. Similarly, if 'low_quality' is yielded, quarantine_and_alert runs, and production_load does not.

But in Dagster, if an op's input is not provided by any upstream op, the job validation will fail. So this approach would not work because when defining the job, the inputs are statically checked. For example, if quarantine_and_alert is defined to take an input from quality_outputs.low_quality, but that output may not be present, Dagster will raise an error during job definition.

Wait, no. Dagster allows for optional inputs by marking them as such in the op definition. So, if the quarantine_and_alert op's input is optional, then even if the upstream op doesn't provide it, the job can still execute.

So, modifying the quarantine_and_alert and production_load ops to have optional inputs:

@op
def production_load(context, data: Optional[dict]):
    if data is None:
        # This shouldn't happen if the op is only called when data is present
        context.log.info("No data provided, skipping")
        return
    # Proceed with loading

Similarly for quarantine_and_alert:

@op
def quarantine_and_alert(context, data: Optional[dict]):
    if data is None:
        context.log.info("No data to quarantine")
        return {"alert_details": None}
    # Proceed with quarantine

But this still doesn't solve the problem because the job will try to pass the output from quality_check to both ops, even if the output isn't present. So, if quality_check yields 'high_quality', then 'low_quality' is not present, and quarantine_and_alert's input is missing. But since the input is optional (with a default of None), Dagster will allow it.

Wait, no. In Dagster, if an op's input is not provided by any upstream op, and the input is not marked as optional, the job definition is invalid. So, to make the inputs optional, the op must define the parameter with a default value.

So, modifying the production_load and quarantine_and_alert ops to have default None:

@op
def production_load(context, data: Optional[dict] = None):
    if data is None:
        context.log.info("Skipping production load")
        return {"prod_status": "skipped"}
    # Load data
    return {"prod_status": "loaded"}

@op
def quarantine_and_alert(context, data: Optional[dict] = None):
    if data is None:
        context.log.info("Skipping quarantine")
        return {"alert_details": None}
    # Quarantine data
    return {"alert_details": "Data quarantined"}

Then, in the job, both production_load and quarantine_and_alert are called, but their inputs may be None if the corresponding output from quality_check is not present. However, this approach causes both ops to run every time, which is not desired. We only want one of them to run based on the quality check.

This suggests that the initial approach is flawed. The correct way to handle conditional branching in Dagster is to use the DynamicOutput and map each possible output to the respective downstream op. However, DynamicOutput is part of the experimental API and may require more complex handling.

Alternatively, use a different approach where the quality_check op returns a decision, and subsequent ops check that decision to determine if they should proceed. But this would require all ops to run, but conditionally execute their logic.

But this isn't ideal because we want to avoid running unnecessary ops.

Another approach is to structure the job with a conditional branch that determines which ops to execute. However, Dagster's job dependencies are static, so dynamic execution based on op outputs isn't directly supported. Therefore, the solution is to use the fact that if an op's input is not available (because the upstream op didn't yield it), the downstream op won't execute.

To achieve this, the quality_check op must yield either the 'high_quality' or 'low_quality' output, and the downstream ops must be connected to these specific outputs. Then, in the job definition, the ops are connected such that only the relevant downstream op receives the input and executes.

But how to define multiple outputs in the quality_check op and connect them properly.

Here's the correct way:

Define the quality_check op with two outputs, 'high_quality' and 'low_quality', each marked as is_required=False. Then, in the job, connect production_load to the 'high_quality' output and quarantine_and_alert to the 'low_quality' output. Since each output is optional, the downstream ops will only execute if the corresponding output is yielded.

In Dagster, when an op has multiple outputs, downstream ops can depend on specific outputs. If the upstream op doesn't yield that output, the downstream op won't execute.

So, modifying the quality_check op:

@op(
    out={
        "high_quality": Out(is_required=False),
        "low_quality": Out(is_required=False),
    }
)
def quality_check(context, csv_data):
    score = 96  # Example, compute this
    if score >= 95:
        yield Output(csv_data, "high_quality")
    else:
        yield Output(csv_data, "low_quality")

Then, in the job:

@job
def data_quality_gate():
    csv_data = ingest_csv()
    quality_results = quality_check(csv_data)
    production_load(quality_results.high_quality)
    quarantine_result = quarantine_and_alert(quality_results.low_quality)
    alert_result = send_alert_email(quarantine_result)
    cleanup(
        prod_result=production_load(),
        alert_result=send_alert_email(),
    )

Wait, no. The syntax here is incorrect. In Dagster, when you have multiple outputs, you can access them by name. So, after quality_check is called, you can get the specific outputs.

But in the job definition, you need to explicitly wire the outputs to the downstream ops. For example:

production_load(quality_results.high_quality)

This means that production_load will only execute if the 'high_quality' output is yielded. Similarly for quarantine_and_alert.

Then, send_alert_email depends on quarantine_result, which is the output of quarantine_and_alert. So if quarantine_and_alert doesn't execute (because 'low_quality' wasn't yielded), send_alert_email won't execute.

Finally, cleanup depends on both production_load and send_alert_email. But if production_load didn't run, its output won't be available. Similarly for send_alert_email.

So, to handle this, the cleanup op must have optional inputs. Let's define cleanup with two optional inputs:

@op
def cleanup(context, prod_result: Optional[dict] = None, alert_result: Optional[dict] = None):
    context.log.info("Performing cleanup")

Then, in the job, connect cleanup to both production_load and send_alert_email:

cleanup(prod_result=production_load(), alert_result=send_alert_email())

But in Dagster, when you call production_load() in the cleanup's arguments, you're referring to the output of the production_load op. If production_load didn't execute, this would be a problem. However, Dagster allows for optional inputs by marking them with default values. So, if production_load didn't run, the input prod_result will be None.

Putting it all together:

@job
def data_quality_gate():
    csv_data = ingest_csv()
    quality_results = quality_check(csv_data)
    prod_data = production_load(quality_results.high_quality)
    quarantine_data = quarantine_and_alert(quality_results.low_quality)
    alert_data = send_alert_email(quarantine_data)
    cleanup(prod_result=prod_data, alert_result=alert_data)

In this setup:

- If quality_check yields 'high_quality', production_load runs, and quarantine_and_alert and send_alert_email do not. cleanup receives prod_data but alert_data is None.
- If quality_check yields 'low_quality', quarantine_and_alert runs, followed by send_alert_email. cleanup receives alert_data but prod_data is None.

But in the job definition, when connecting cleanup's inputs, prod_data is the output of production_load, which may not have executed. Similarly for alert_data. However, Dagster allows this if the inputs are optional.

Wait, no. If production_load didn't execute, then prod_data is not available, and passing it to cleanup would result in an error. Therefore, this approach won't work because the job definition references ops that may not have executed.

This suggests that the correct way is to use the fact that if an op's upstream dependency didn't execute, the op won't execute. Therefore, cleanup must depend on both production_load and send_alert_email, but only the ones that actually ran will trigger cleanup.

But how to model that. Maybe cleanup should depend on both production_load and send_alert_email, but in the job definition, it's connected to both. However, if one of them didn't run, Dagster will still run cleanup as long as at least one of its upstream dependencies executed.

Wait, no. In Dagster, an op will execute if all of its required upstream dependencies have executed. If any of the upstream dependencies are optional (is_required=False), then the op can execute even if some upstream dependencies didn't run.

So, to make cleanup depend on both production_load and send_alert_email, but allow either to be missing, we need to mark the inputs as optional.

But in the job definition, how to connect them. Let's see:

@op
def cleanup(context, prod_result: Optional[dict] = None, alert_result: Optional[dict] = None):
    # Cleanup steps

@job
def data_quality_gate():
    csv_data = ingest_csv()
    quality_results = quality_check(csv_data)
    prod_data = production_load(quality_results.high_quality)
    quarantine_data = quarantine_and_alert(quality_results.low_quality)
    alert_data = send_alert_email(quarantine_data)
    cleanup(prod_data, alert_data)

But in this case, if production_load didn't run (because quality_check yielded 'low_quality'), then prod_data is not available, and passing it to cleanup would cause an error. Similarly, if send_alert_email didn't run (because quality_check yielded 'high_quality'), alert_data is not available.

Therefore, this approach is incorrect.

The correct way is to have cleanup depend on both production_load and send_alert_email, but allow their inputs to be optional. However, in Dagster, the way to do this is to use the fact that if an op's input is not provided by any upstream op, it can still execute if the input is marked as optional.

So, modifying the cleanup op to have optional inputs with defaults:

@op
def cleanup(context, prod_result: Optional[dict] = None, alert_result: Optional[dict] = None):
    # Cleanup logic here
    context.log.info("Performing cleanup")

Then, in the job, connect both production_load and send_alert_email to cleanup's inputs. But how to ensure that cleanup runs after both possible paths.

Wait, perhaps the job should be structured such that cleanup depends on both production_load and send_alert_email. So:

@job
def data_quality_gate():
    csv_data = ingest_csv()
    quality_results = quality_check(csv_data)
    prod_data = production_load(quality_results.high_quality)
    quarantine_data = quarantine_and_alert(quality_results.low_quality)
    alert_data = send_alert_email(quarantine_data)
    cleanup(prod_result=prod_data, alert_result=alert_data)

But again, if production_load didn't run, prod_data is undefined, leading to an error. So this approach won't work.

Alternative approach: Use a fan-in where both branches end with an op that produces a result, and cleanup depends on both. But how to ensure that cleanup runs after either branch.

Perhaps introduce two separate ops that signal completion of each branch, then have cleanup depend on both.

For example:

After production_load, run an op like 'prod_complete'.

After send_alert_email, run an op like 'alert_complete'.

Then, cleanup depends on both 'prod_complete' and 'alert_complete'.

But this adds more ops, which may not be necessary.

Alternatively, use the fact that in Dagster, an op can depend on multiple previous ops, and will run as long as all of its upstream dependencies have executed. But if some dependencies are optional (is_required=False), then the op can run even if some dependencies are missing.

But I'm not sure how to model this.

Another idea: Use a dummy output from production_load and send_alert_email, then have cleanup depend on both. For example:

@op
def production_load(context, data):
    # ... load data
    return {"status": "loaded"}

@op
def send_alert_email(context, alert_details):
    # ... send email
    return {"status": "email_sent"}

@op
def cleanup(context, prod_status, email_status):
    # Check which inputs are present
    if prod_status:
        context.log.info("Prod loaded")
    if email_status:
        context.log.info("Email sent")
    # Cleanup

But in the job, how to connect these. If we have:

cleanup(prod_status=production_load(), email_status=send_alert_email())

Then, if production_load didn't run, prod_status is missing. Similarly for email_status.

So, to handle this, the inputs to cleanup must be optional:

@op
def cleanup(context, prod_status: Optional[dict] = None, email_status: Optional[dict] = None):
    # perform cleanup

Then, in the job:

cleanup(prod_status=production_load(), email_status=send_alert_email())

But again, if production_load didn't run, prod_status is not available, leading to an error.

This suggests that the initial approach of using optional inputs isn't viable in Dagster because the job definition requires that all inputs are connected to upstream ops, even if those ops may not execute.

The correct solution, therefore, is to use Dagster's asset-based approach or to structure the job such that both paths eventually feed into cleanup through a common op. However, given the constraints of the problem, we need to find a way within ops and jobs.

After researching, I recall that in Dagster, when an op has multiple outputs, downstream ops that depend on specific outputs will only execute if that output is yielded. Therefore, the job can be structured as follows:

- ingest_csv runs.
- quality_check yields either 'high_quality' or 'low_quality'.
- production_load depends on 'high_quality'.
- quarantine_and_alert depends on 'low_quality'.
- send_alert_email depends on quarantine_and_alert.
- cleanup depends on both production_load and send_alert_email.

But how to make cleanup depend on both when only one may have executed.

The answer is that cleanup must be downstream of both production_load and send_alert_email, but since only one of them will have executed, Dagster will only require that the executed op's output is available. However, this isn't possible because Dagster requires that all inputs to an op are provided by upstream ops. If an input is not provided, the job will fail.

Therefore, the correct approach is to have two separate cleanup steps, but the problem states that there's a single cleanup task that runs after both paths.

This suggests that the initial pipeline description may have a flaw, but according to the user's instructions, we need to model it as described.

Given the time I've spent on this, I think the best way is to proceed with the following structure, using optional inputs and ensuring that cleanup runs after both paths by making it depend on both, even if one path's input is None.

So, the code would be:

from dagster import op, job, Out, Output
from typing import Optional

@op
def ingest_csv(context) -> dict:
    """Ingest raw customer CSV data."""
    context.log.info("Ingesting CSV")
    return {"path": "/data/customers.csv", "metadata": {}}

@op(
    out={
        "high_quality": Out(is_required=False),
        "low_quality": Out(is_required=False),
    }
)
def quality_check(context, csv_data: dict):
    """Calculate data quality score and branch."""
    # Example score, replace with actual calculation
    score = 96
    if score >= 95:
        context.log.info("High quality data")
        yield Output(csv_data, "high_quality")
    else:
        context.log.info("Low quality data")
        yield Output(csv_data, "low_quality")

@op
def production_load(context, data: dict):
    """Load data to production database."""
    context.log.info("Loading to production")
    return {"status": "loaded"}

@op
def quarantine_and_alert(context, data: dict):
    """Move data to quarantine and prepare alert."""
    context.log.info("Quarantining data")
    return {"alert_details": "Data quarantined"}

@op
def send_alert_email(context, alert_details: dict):
    """Send alert email to data stewards."""
    context.log.info("Sending alert email")
    return {"status": "email_sent"}

@op
def cleanup(
    context,
    prod_result: Optional[dict] = None,
    alert_result: Optional[dict] = None,
):
    """Perform cleanup operations."""
    context.log.info("Cleaning up")
    if prod_result:
        context.log.info("Production cleanup")
    if alert_result:
        context.log.info("Alert cleanup")

@job
def data_quality_gate():
    csv_data = ingest_csv()
    quality_outputs = quality_check(csv_data)
    prod_data = production_load(quality_outputs.high_quality)
    quarantine_data = quarantine_and_alert(quality_outputs.low_quality)
    alert_data = send_alert_email(quarantine_data)
    cleanup(prod_result=prod_data, alert_result=alert_data)

if __name__ == "__main__":
    result = data_quality_gate.execute_in_process()

But wait, in this setup, if quality_check yields 'high_quality', then quarantine_and_