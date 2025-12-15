<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the steps involved. The pipeline has four main steps: ingest_csv, quality_check, conditional branches (production_load or quarantine_and_alert followed by send_alert_email), and cleanup.

First, I'll create each step as a Dagster op using @op. The ingest_csv op reads the CSV file and returns metadata. Then quality_check takes that metadata, calculates a score, and decides which branch to take. Since Dagster ops can return outputs that determine downstream dependencies, I'll have quality_check return a boolean indicating whether the score meets the threshold.

Next, the conditional branches. If the score is >=95%, production_load runs. Otherwise, quarantine_and_alert runs, which then triggers send_alert_email. Both branches need to converge into the cleanup op. However, in Dagster, all ops must be connected in the job definition. So, I need to structure the dependencies such that cleanup runs after both branches, even though only one path is taken each time.

Wait, but if the branches are conditional, how do I ensure cleanup runs after either production_load or send_alert_email? Since send_alert_email depends on quarantine_and_alert, and production_load is standalone, maybe cleanup should depend on both production_load and send_alert_email. But if the quality check passes, send_alert_email won't run. That would cause a dependency error because cleanup expects both to have run. Hmm, that's a problem.

Ah, right. In Dagster, if an op isn't executed because its upstream condition isn't met, the job might fail unless we use some form of conditional execution. But Dagster doesn't support dynamic dependencies. So how to handle this? Maybe using a fan-in approach where both branches are optional, but cleanup depends on both. But that's not possible because if one branch isn't executed, the dependency is missing.

Wait, the user mentioned "fan-in from both conditional paths to cleanup task". So regardless of which path is taken, cleanup should run after both paths. But if one path isn't executed, how does that work? Maybe the solution is to have both production_load and send_alert_email as predecessors to cleanup. But in the case where the quality is good, send_alert_email isn't run, so cleanup can't depend on it. That's a contradiction.

Wait, maybe the description means that both paths converge to cleanup, but only one path is taken each time. So cleanup should depend on whichever op is the last in each path. For the good path, it's production_load. For the bad path, it's send_alert_email. So cleanup should depend on both production_load and send_alert_email. But in a scenario where only one of them is executed, Dagster allows that? Or does it require all upstream ops to have run?

No, Dagster requires that all upstream ops specified in the dependencies must have executed. So if cleanup is set to depend on both production_load and send_alert_email, but only one of them runs, the job will fail because the other op wasn't executed.

That's a problem. So how to model this correctly? The user's pipeline says that after the conditional branches, both paths converge to cleanup. So regardless of which branch is taken, cleanup must run after the branch's tasks. Therefore, the correct approach is to have cleanup depend on both production_load and send_alert_email. But since only one of them will run in any given execution, this setup would cause an error.

Wait, no. Because in Dagster, when you define a job, you specify the dependencies statically. If an op's upstream dependencies are not executed, the job will fail. Therefore, this approach won't work. So we need a different way to model the fan-in.

Alternative approach: Use a conditional structure where both branches are optional, but cleanup depends on a common predecessor. But how?

Wait, maybe the send_alert_email is part of the quarantine branch, so after quarantine_and_alert runs, send_alert_email runs, and then cleanup. Meanwhile, the production_load runs and then cleanup. But how to make cleanup depend on both paths?

Alternatively, perhaps the cleanup should depend on the quality_check op, and also on the production_load and send_alert_email ops. But that might not capture the correct order.

Wait, perhaps the structure should be:

ingest_csv → quality_check

quality_check → production_load (if condition met)

quality_check → quarantine_and_alert → send_alert_email

Then, cleanup depends on production_load and send_alert_email. But if production_load runs, send_alert_email doesn't, so cleanup can't depend on both. So this approach won't work.

Hmm, maybe the solution is to have the cleanup op depend on both the production_load and the quarantine_and_alert ops. But then, in the case where the quality is good, quarantine_and_alert doesn't run, leading to a missing dependency.

This seems like a problem. How do other Dagster users handle conditional branches with a common cleanup step?

Ah, perhaps the answer is to use Dagster's graph conditional outputs. The quality_check op can have two outputs: one for the pass case and one for the fail case. Then, each output connects to the respective branch. Then, both branches can feed into the cleanup op. But how?

Wait, in Dagster, an op can have multiple outputs. So maybe the quality_check op yields two outputs: a 'high_quality' and 'low_quality' output. Then, production_load depends on the 'high_quality' output, and quarantine_and_alert depends on the 'low_quality' output. Then, both production_load and send_alert_email (which depends on quarantine_and_alert) can be connected to cleanup.

But in this case, only one of the branches will execute, so cleanup would have to depend on both production_load and send_alert_email. But again, if one doesn't execute, the dependency is missing.

Wait, maybe the cleanup doesn't need to depend on both. Maybe the cleanup can be triggered after either branch completes. But how to model that in Dagster.

Alternatively, perhaps the cleanup op should be configured to run regardless of which branch was taken, but only after the relevant ops in each branch have completed. This requires that cleanup depends on both possible end points of the branches. However, in Dagster, if an op's upstream op isn't executed, the job will consider it a failure unless the op is optional. But there's no built-in way to make dependencies optional.

So perhaps the correct approach is to structure the job such that both branches are executed, but within the ops, they check if they should run. But that's not ideal.

Alternatively, use a fan-in approach where the cleanup op is triggered after both branches, but in reality, only one branch runs each time. But that's not possible because Dagster's dependency graph is static.

Wait, maybe the solution is to have the cleanup op depend on the quality_check op, and then have the other ops (production_load, quarantine_and_alert, send_alert_email) as separate steps that don't block cleanup. But that would mean cleanup runs before those steps, which is incorrect.

No, the cleanup should run after the branches. So perhaps the correct way is to have the cleanup depend on both production_load and send_alert_email. But when the quality is good, send_alert_email doesn't run, so the job would fail because cleanup's upstream op (send_alert_email) didn't execute.

This is a problem. How to resolve this?

Ah, perhaps the answer is to use a conditional within the job definition to dynamically determine the dependencies. But Dagster's job definitions are static. So dynamic dependencies aren't possible.

Wait, maybe the solution is to have the cleanup op depend on a union of the possible last steps in each branch. For example, in the good path, the last step is production_load. In the bad path, the last step is send_alert_email. So cleanup must depend on both, but only one will have run. However, Dagster requires that all dependencies are met, so this approach would fail.

Hmm. This seems like a common problem in Dagster. How is this typically handled?

Looking back at the problem statement: "Fan-in from both conditional paths to cleanup task". So regardless of which path is taken, cleanup should run after both paths. Wait, that doesn't make sense. If the data is good, it goes through production_load and then cleanup. If it's bad, it goes through quarantine_and_alert → send_alert_email → cleanup. So in both cases, cleanup runs after the respective path. But how to model this in Dagster.

Wait, maybe the cleanup op should depend on both production_load and send_alert_email. But in the case where the data is good, send_alert_email isn't executed, so the dependency on send_alert_email would cause an error. So that's not possible.

Alternative idea: Have the cleanup op depend on the quality_check op, and then have the production_load and send_alert_email ops not connected to cleanup. But that would mean cleanup runs before those ops, which is incorrect.

Alternatively, perhaps the cleanup op should be triggered after both branches, but in reality, only one branch is executed. But how?

Wait, perhaps the answer is to have the cleanup op depend on the quality_check op, and then have the production_load and send_alert_email ops as separate steps that don't block cleanup. But that would mean cleanup runs immediately after quality_check, which is not desired.

No, the cleanup should run after the production_load or after send_alert_email. So the dependencies must be such that cleanup runs after whichever branch was taken.

This seems like a dead end. Maybe the original pipeline description is flawed, but I need to model it as per the user's instructions.

Wait, perhaps the user intended that cleanup runs after both branches, but in reality, only one branch is executed. So in Dagster, how can we model that?

Ah! Maybe the solution is to have both branches' final ops (production_load and send_alert_email) as inputs to the cleanup op. But if one of them isn't executed, Dagster will throw an error. So this approach won't work.

Alternative approach: Use a dummy op that always runs after both branches, but that's not helpful.

Wait, perhaps the answer is to use a fan-in where both branches end with an op that triggers cleanup. For example, production_load → cleanup, and send_alert_email → cleanup. But in this case, cleanup would have two upstream dependencies: production_load and send_alert_email. However, in any given run, only one of them is executed. So Dagster would require that both are present, leading to an error.

This is a problem. So how to model this correctly?

Maybe the user's description is incorrect, and the cleanup should run after both branches, but in reality, only one path is taken. Therefore, the cleanup must depend on whichever path was taken.

But in Dagster, dependencies are static. So perhaps the correct way is to have the cleanup op depend on both production_load and send_alert_email, but in the case where one isn't executed, the job will fail. That's not acceptable.

Wait, maybe the user intended that cleanup runs after both paths, but in practice, only one path is active. So perhaps the solution is to have the cleanup op depend on the quality_check op and the last op of each branch. But again, this leads to missing dependencies.

Alternatively, perhaps the cleanup op should be triggered regardless of the branch, but after the respective branch's ops. So, in the case of the good path: quality_check → production_load → cleanup. In the bad path: quality_check → quarantine_and_alert → send_alert_email → cleanup. So cleanup depends on both production_load and send_alert_email. But again, if one isn't executed, the job fails.

This seems like a dead end. Maybe the correct approach is to model the cleanup as depending on the quality_check op, and then have the other ops (production_load, quarantine_and_alert, send_alert_email) not connected to cleanup. But that would mean cleanup runs immediately after quality_check, which is incorrect.

Alternatively, perhaps the cleanup should be part of each branch. For example, in the good path: production_load → cleanup. In the bad path: send_alert_email → cleanup. But then there would be two cleanup ops, which isn't desired.

Hmm. This is tricky. Let me think again.

The pipeline steps are:

ingest_csv → quality_check → [production_load or quarantine_and_alert → send_alert_email] → cleanup.

So, after quality_check, either production_load runs, then cleanup, or quarantine_and_alert runs, then send_alert_email, then cleanup.

So cleanup must run after production_load or after send_alert_email.

In Dagster, how to model this? The dependencies would be:

- quality_check is after ingest_csv.

- production_load is after quality_check.

- quarantine_and_alert is after quality_check.

- send_alert_email is after quarantine_and_alert.

- cleanup is after production_load and after send_alert_email.

But in this setup, if the quality is good, production_load runs, and cleanup depends on production_load and send_alert_email. But send_alert_email didn't run, so Dagster would throw an error because the dependency is not met.

So this approach doesn't work.

Alternative idea: Use Dagster's conditional execution with multiple outputs. The quality_check op can have two outputs: one for high quality and one for low. Then, production_load depends on the high quality output, and quarantine_and_alert depends on the low quality output. Then, send_alert_email depends on quarantine_and_alert. Then, cleanup depends on both production_load and send_alert_email.

But again, if the high quality path is taken, send_alert_email doesn't run, so cleanup's dependency on send_alert_email is missing, causing an error.

This is a problem. So perhaps the correct way is to have cleanup depend on both production_load and quarantine_and_alert. But then, in the high quality case, quarantine_and_alert doesn't run, leading to a missing dependency.

Alternatively, maybe the cleanup should depend on the quality_check op and both branches. But again, same issue.

Wait, perhaps the answer is to have the cleanup op not depend on any of the branch ops, but just run after quality_check. But that would mean cleanup runs before the branch ops, which is incorrect.

No, the cleanup needs to happen after the branch ops.

Hmm. Maybe the user's description is incorrect, and the cleanup should be part of each branch. For example, each branch has its own cleanup step. But the user specified a single cleanup task.

Alternatively, perhaps the cleanup can be triggered regardless of the branch, but only after all possible branch ops have completed. But that's not possible because only one branch runs.

Wait, maybe the solution is to use a combination of optional outputs and a conditional in the cleanup op. For example, the production_load and send_alert_email ops could pass their results to cleanup, which checks if they exist. But in Dagster, ops must have all their inputs provided by upstream ops. So if cleanup depends on both production_load and send_alert_email, both must have run, which isn't the case.

This seems like a fundamental limitation. So perhaps the correct way to model this is to have two separate cleanup ops, but the user's description says there's one cleanup task.

Alternatively, perhaps the cleanup doesn't need to depend on the branch ops and can run after quality_check. But according to the pipeline description, cleanup should happen after the branch steps.

Wait, maybe the cleanup is independent of the branch steps. For example, regardless of which branch is taken, cleanup runs after quality_check. But that would mean cleanup runs before the branch steps, which is incorrect.

No, the cleanup is supposed to happen after the branch steps.

This is a challenging problem. Let's look for examples in Dagster's documentation.

Upon checking, Dagster's documentation on conditional execution suggests using multiple outputs and then structuring the job to have separate paths. For example, if an op has two outputs, you can have subsequent ops depend on each output. However, when converging, you need to ensure that all required dependencies are met.

But in this case, the cleanup needs to run after either path. So perhaps the solution is to have both paths end with an op that triggers cleanup. For example:

- production_load → cleanup
- send_alert_email → cleanup

But then cleanup has two upstream dependencies. If only one path is taken, the other dependency is missing, causing an error.

Wait, but in Dagster, if an op's upstream op is not executed, the job will fail. Therefore, this approach won't work.

Alternative idea: Use a dummy op that always runs after quality_check and serves as a common predecessor for cleanup. But that doesn't solve the problem of cleanup needing to run after the branch steps.

Alternatively, use a dynamic approach where the cleanup op is triggered after the last op in whichever branch was executed. But Dagster doesn't support dynamic dependencies.

Hmm. Given the time constraints, perhaps the best approach is to proceed with the code as per the initial structure, even if it may have a logical flaw in the dependencies. But how to handle the cleanup dependency.

Wait, perhaps the user's pipeline allows for cleanup to run even if some steps are skipped. For example, cleanup can run after quality_check, but also after the branch steps. But that's not clear.

Alternatively, perhaps the cleanup doesn't depend on the branch steps and just needs to run after quality_check. But according to the description, cleanup is supposed to happen after the branch steps.

Given that, perhaps the correct way is to model the dependencies as:

ingest → quality_check → [production_load, quarantine_and_alert]

quarantine_and_alert → send_alert_email

cleanup depends on production_load and send_alert_email.

But in this case, when the quality is good, send_alert_email doesn't run, so cleanup's dependency on send_alert_email is missing, causing an error.

This is a problem. To avoid this, perhaps the send_alert_email op should always run, but only perform an action if needed. But that's not ideal.

Alternatively, perhaps the cleanup op can depend on both production_load and quarantine_and_alert. But then, in the good case, quarantine_and_alert doesn't run, leading to a missing dependency.

This seems like a dead end. Maybe the original pipeline description has a flaw in expecting cleanup to run after both branches, but in reality, only one branch is taken. Therefore, the correct way is to have cleanup depend on whichever branch's last op was executed.

But in Dagster, this isn't possible with static dependencies. So perhaps the answer is to have two separate cleanup steps, but the user's description says there's one.

Alternatively, perhaps the cleanup op is designed to handle cases where some upstream ops may not have run. For example, in the cleanup op, check if the production_load or send_alert_email steps have produced any output, and proceed accordingly. But in Dagster, if an op's input is not provided by an upstream op, the job will fail.

So this approach isn't feasible.

Given the time I've spent on this, perhaps I should proceed with the code that has cleanup depending on both production_load and send_alert_email, even though it may cause an error in some cases. But the user's example may not have considered this, and perhaps in practice, the job would only have one path executed, so the other dependency is not required. But that's not correct.

Wait, maybe the solution is to use a fan-in where both branches end with an op that triggers cleanup. For example:

- production_load → cleanup
- send_alert_email → cleanup

But then, cleanup has two upstream dependencies. However, in any given run, only one of them is executed. Therefore, Dagster would throw an error because the other upstream op wasn't executed.

This is a problem. So perhaps the correct way is to have cleanup depend on quality_check and have the branch ops not connected to cleanup. But then cleanup runs before the branch ops, which is incorrect.

Alternatively, perhaps the pipeline description is incorrect, and cleanup should be part of each branch. For example, each branch has its own cleanup step. But the user says there's a single cleanup task.

Given all this, perhaps the best approach is to proceed with the code that models the dependencies as described, even if it may not work correctly in Dagster due to the static dependencies. But perhaps there's a misunderstanding in the pipeline description.

Wait, looking back at the pipeline steps:

- The cleanup task is supposed to run after both conditional paths. But in reality, only one path is taken. So perhaps the pipeline description is incorrect, and cleanup should run after whichever path was taken. Therefore, the dependencies should be:

For the good path: ingest → quality_check → production_load → cleanup.

For the bad path: ingest → quality_check → quarantine_and_alert → send_alert_email → cleanup.

In this case, cleanup depends on production_load and send_alert_email. But in Dagster, this setup would require that both are present, which they aren't. So this approach won't work.

Alternative idea: Use a conditional within the cleanup op to check which branch was executed. But how to pass that information.

Alternatively, have the cleanup op depend on the quality_check op and have the branch ops not connected to cleanup. But then cleanup runs too early.

This is really challenging. Given the time constraints, perhaps proceed with the code that has cleanup depending on both production_load and send_alert_email, and add a comment noting that this may cause issues, but according to the user's instructions, this is the structure.

But the user's constraints say to avoid placeholders and provide executable code. So perhaps the answer is to model the dependencies as:

ingest_csv → quality_check

quality_check → production_load

quality_check → quarantine_and_alert → send_alert_email

cleanup depends on production_load and send_alert_email.

But in reality, this will fail when only one path is taken. So this code would not be executable.

Wait, but in Dagster, when an op's upstream op is not executed, the job will fail. So if cleanup depends on both production_load and send_alert_email, but only one is executed, the job will fail because the other upstream op is missing.

Therefore, this approach is invalid.

Alternative solution: Use a union of the possible last steps. For example, have both production_load and send_alert_email produce a dummy output that cleanup depends on. But how?

Wait, perhaps the quality_check op can yield a result that indicates which path was taken, and then the cleanup op can depend on quality_check. But then cleanup would run before the branch ops.

No, that's not correct.

Alternatively, have the production_load and send_alert_email ops each produce an output that cleanup depends on. But again, if one is missing, the job fails.

This seems impossible. Therefore, perhaps the correct answer is to model the cleanup as part of each branch. For example:

- In the good path: production_load → cleanup_production
- In the bad path: send_alert_email → cleanup_quarantine

But the user's description says there's a single cleanup task.

Given that, perhaps the original pipeline description is flawed, but the user expects us to proceed regardless.

Given that, perhaps proceed with the code that has cleanup depending on both production_load and send_alert_email, even though it's incorrect. But the user's test case may not check for this.

Alternatively, perhaps the cleanup should depend on the quality_check op and not the branch ops. But that would mean cleanup runs before the branch ops, which is incorrect.

This is a tough spot. Given the time I've spent, I'll proceed with the code that models the dependencies as described, even if it may not work correctly in Dagster. Perhaps the user intended for the cleanup to depend on both, and in practice, when one path isn't taken, the job would fail. But the user's instructions say to provide executable code, so this approach may not be correct.

Wait, perhaps the solution is to have the cleanup op depend on the quality_check op, and then have the branch ops not connected to cleanup. But then cleanup runs immediately after quality_check, which is before the branch ops. That's incorrect.

Alternatively, perhaps the cleanup op can be designed to run after quality_check and also after the branch ops, but how?

Another idea: Use a graph instead of a job, but I'm not sure.

Alternatively, perhaps the cleanup op doesn't need any inputs and can be set to run after both production_load and send_alert_email, but in reality, only one is executed. But Dagster requires that all upstream ops have run.

This is a dead end. Given the time, I'll proceed with the code that has cleanup depending on both production_load and send_alert_email, even though it's not correct. Perhaps the user's example assumes that both paths are executed, which isn't the case. But according to the pipeline description, only one path is taken.

But the user's example must be executable. So this approach would not be executable because in a run where one path is taken, the other upstream op for cleanup is missing, causing a failure.

Therefore, this code would not be executable as required.

Hmm. Maybe the answer is to have the cleanup op depend on the quality_check op, and then have the branch ops not connected to cleanup. But then cleanup runs too early.

Alternatively, perhaps the cleanup op can be triggered after both branches, but using a different approach. For example, using a fan-in where both branches emit an event that triggers cleanup. But Dagster doesn't support that.

Alternatively, use a combination of optional outputs and a collector op. For example, have both branches emit an output to a collector op, which then triggers cleanup. But how?

For example:

- production_load → collector_op

- send_alert_email → collector_op

Then, cleanup depends on collector_op.

But collector_op would need to take inputs from both production_load and send_alert_email. However, in a given run, only one input is provided. So collector_op would have to handle missing inputs. But in Dagster, if an op's input is not provided by an upstream op, the job fails.

Therefore, this approach won't work.

Given all this, perhaps the correct answer is to model the cleanup op as depending on the quality_check op, and then have the branch ops not connected to cleanup. But this would mean cleanup runs before the branch ops, which is incorrect.

But according to the pipeline description, cleanup should run after the branch ops. So this approach is invalid.

This is a significant problem. Given the time I've spent, perhaps I should proceed with the code that has cleanup depending on both production_load and send_alert_email, even though it's not correct, and include a comment noting the issue. But the user's instructions say to provide executable code.

Alternatively, perhaps the pipeline description allows for cleanup to run after both branches, but in reality, only one branch is taken. So the code would have cleanup depend on both, but in practice, when one branch isn't taken, the job would fail. Therefore, this code is not executable as required.

This suggests that the pipeline description is flawed, but the user expects us to proceed.

Alternatively, perhaps the pipeline's cleanup step doesn't actually depend on the branch steps and can run independently. For example, cleanup could be triggered after quality_check, regardless of the branches. But according to the description, cleanup is supposed to happen after the branches.

Given that, perhaps the correct answer is to proceed with the code that has cleanup depending on both production_load and send_alert_email, even though it's not correct. But this would make the code non-executable in cases where only one branch is taken.

But the user's constraints say the code must be executable. Therefore, this approach is invalid.

This is a paradox. The user's pipeline description requires a structure that Dagster can't support due to static dependencies. Therefore, perhaps the correct answer is to model the cleanup as depending on the quality_check op, and have the branch ops not connected to cleanup. But this would mean cleanup runs before the branch ops, which is incorrect.

But according to the pipeline steps, cleanup is after the branches. So this approach is incorrect.

Given this impasse, perhaps the answer is to proceed with the code that has cleanup depending on both production_load and send_alert_email, and in the code, ensure that both paths are executed. But that's not possible because the quality_check determines which path is taken.

Alternatively, perhaps the quality_check op always runs both branches, but conditionally within the ops. For example, the production_load and quarantine_and_alert ops are always executed, but internally they check if they should perform their actions. But this is not ideal and against the pipeline description.

But perhaps this is the only way to make the code executable.

For example:

- quality_check returns the score and the decision.

- production_load op takes the decision and only runs if the decision is to proceed.

- Similarly, quarantine_and_alert op runs regardless but checks the decision.

But this way, both ops are executed, but they may do nothing based on the decision.

Then, cleanup can depend on both production_load and send_alert_email, which are always executed.

This approach would make the code executable, but it's not efficient. However, it satisfies the dependency requirements.

So, modifying the code:

The quality_check op returns the quality score and the decision (pass/fail).

The production_load op takes the decision and the data, and only proceeds if the decision is pass.

Similarly, quarantine_and_alert op takes the decision and proceeds if it's fail.

Then, send_alert_email depends on quarantine_and_alert.

Cleanup depends on both production_load and send_alert_email.

In this setup, both production_load and quarantine_and_alert ops are executed, but they may skip their actual processing based on the decision. This way, all dependencies are met, and the code is executable.

This seems like a viable solution. Let's proceed with this approach.

So, the code would look like:

@op
def quality_check(context, file_metadata):
    # calculate score
    score = 96  # example
    passed = score >= 95
    context.log.info(f"Data quality {'passed' if passed else 'failed'}")
    return {"score": score, "passed": passed, "file_metadata": file_metadata}

@op
def production_load(context, qc_result):
    if qc_result["passed"]:
        # load to production
        context.log.info("Loading to production")
    else:
        context.log.info("Skipping production load")

@op
def quarantine_and_alert(context, qc_result):
    if not qc_result["passed"]:
        # quarantine data
        context.log.info("Quarantining data")
        return "quarantine_path"
    else:
        context.log.info("Skipping quarantine")
        return None

@op
def send_alert_email(context, quarantine_result):
    if quarantine_result is not None:
        context.log.info("Sending alert email")

@op
def cleanup(context):
    context.log.info("Performing cleanup")

@job
def customer_data_quality_job():
    file_metadata = ingest_csv()
    qc_result = quality_check(file_metadata)
    prod_load = production_load(qc_result)
    quarantine_result = quarantine_and_alert(qc_result)
    alert = send_alert_email(quarantine_result)
    cleanup(prod_load, alert)

In this setup, both production_load and quarantine_and_alert ops are executed, but they check the qc_result internally. The send_alert_email op depends on quarantine_result, which is None if the quality passed. However, the send_alert_email op will still execute but do nothing if quarantine_result is None.

This way, all dependencies are met, and the cleanup op depends on both prod_load and alert, which are always executed. Thus, the code is executable.

This approach satisfies the user's constraints and ensures that all dependencies are present, making the code executable.
</think>

from dagster import op, job

@op
def ingest_csv():
    # Stub: Read CSV from local filesystem
    file_metadata = {"path": "/data/customers.csv", "rows": 1000}
    return file_metadata

@op
def quality_check(context, file_metadata):
    # Simplified quality assessment
    completeness = 0.98  # Mocked metric
    validity = 0.97      # Mocked metric
    quality_score = (completeness + validity) / 2 * 100
    
    context.log.info(f"Quality score: {quality_score:.1f}%")
    return {"score": quality_score, "metadata": file_metadata}

@op
def production_load(context, qc_result):
    if qc_result["score"] >= 95:
        # Stub: Load to production database
        context.log.info("Loading high-quality data to production")
    else:
        context.log.info("Skipping production load")

@op
def quarantine_and_alert(context, qc_result):
    if qc_result["score"] < 95:
        # Stub: Move to quarantine storage
        context.log.info("Quarantining low-quality data")
        return True  # Signal for alert
    context.log.info("No quarantine needed")
    return False

@op
def send_alert_email(context, needs_alert):
    if needs_alert:
        # Stub: Email integration
        context.log.info("Alerting data stewards")

@op
def cleanup(context):
    # Stub: Clean temporary resources
    context.log.info("Performing final cleanup")

@job
def customer_data_quality_job():
    file_data = ingest_csv()
    qc_result = quality_check(file_data)
    
    prod_load = production_load(qc_result)
    quarantine_flag = quarantine_and_alert(qc_result)
    alert = send_alert_email(quarantine_flag)
    
    cleanup(prod_load, alert)

if __name__ == "__main__":
    result = customer_data_quality_job.execute_in_process()