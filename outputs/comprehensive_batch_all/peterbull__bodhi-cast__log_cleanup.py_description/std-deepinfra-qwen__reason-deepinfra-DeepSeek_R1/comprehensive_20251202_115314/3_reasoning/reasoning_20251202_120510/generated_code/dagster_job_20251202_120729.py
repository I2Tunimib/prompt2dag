<think>
Okay, I need to convert the given Airflow pipeline description into a Dagster job. Let's start by understanding the original pipeline's structure and requirements.

The pipeline's main purpose is to clean up old Airflow log files using parallel workers. The steps include a start task, parallel cleanup tasks with locking, and configuration handling. The original uses Airflow's BashOperator and EmptyOperator, but in Dagster, I'll use ops.

First, the Start task is an EmptyOperator in Airflow. In Dagster, this can be a simple op that does nothing but act as the entry point. So I'll create a @op called start_pipeline.

Next, the parallel log cleanup tasks. Each worker processes log directories, uses file-based locking, and has a three-phase cleanup. Since Dagster allows parallel execution via dependencies, I'll model each worker as an op. But since they need to run in parallel, they should all depend on the start_pipeline op and then be followed by a final op that depends on all of them. Wait, the original has multiple BashOperators in parallel. So in Dagster, each parallel task would be an op, and they can all be invoked after the start op. However, since the number of workers isn't specified, maybe the user expects a configurable number. But the problem statement says "multiple BashOperator tasks execute in parallel fan-out pattern". So perhaps the number of workers is fixed or determined by configuration.

But the example might need to hardcode a few workers for simplicity. Alternatively, use a configurable parameter. However, the user might want to have dynamic parallelism, but Dagster's graph/op structure is static. So perhaps create a few worker ops, each representing a worker process. Alternatively, use a single op that handles multiple directories in parallel, but that's not how Airflow does it. Hmm.

Wait, the original uses multiple BashOperator tasks in parallel. So in Dagster, each of these would be separate ops. But how many? Since the user example might not specify, perhaps create a configurable number via a list of directories. For example, if the BASE_LOG_FOLDER has multiple subdirectories, each worker handles one. But for simplicity, maybe assume a fixed number of workers, say three, each processing a directory. Alternatively, have a config parameter that lists the directories to process, and the op is called once per directory.

But in Dagster, you can't dynamically create ops at runtime. The ops are defined statically. So perhaps the user is expected to have a configurable list of directories, and the op processes each in a loop. Alternatively, use a fan-out approach where one op generates a list of directories, and then a mapped op processes each. That would be more dynamic.

But the original uses multiple BashOperator tasks in parallel, which suggests that each is a separate task. So in Dagster, using a mapped op would be appropriate. So the steps would be:

1. start_pipeline op
2. get_log_directories op (retrieves list of directories to process)
3. For each directory, run the cleanup_worker op in parallel (mapped)
4. Then a final op that depends on all cleanup_worker ops completing.

But the original also mentions that each worker uses a lock file. So each worker op would need to handle locking. However, in a mapped op, each instance would process a directory, and the lock could be per directory or per worker. The original uses /tmp/airflow_log_cleanup_worker.lock, which suggests a single lock file per worker, but perhaps each worker has its own lock. Alternatively, maybe the lock is per directory. But the original's lock file name doesn't include the directory, so perhaps it's a global lock. Wait, the original says "file-based locking using /tmp/airflow_log_cleanup_worker.lock". That suggests that each worker uses the same lock file, which would prevent concurrent execution of the cleanup steps. But that's conflicting with the parallel execution. Wait, maybe the lock is per worker, but since the workers are running in parallel, the lock file would be shared, causing them to wait. But that might not be the case. Alternatively, maybe each worker has a unique lock file name. But the original description isn't clear. Hmm.

This part is a bit ambiguous. The original says "file-based locking using /tmp/airflow_log_cleanup_worker.lock". So perhaps each worker uses the same lock file, which would serialize their execution. But that contradicts the parallel execution. So maybe the lock is per directory or per worker. Alternatively, the lock is used to prevent multiple instances of the same worker from running. But since the original uses parallel tasks, perhaps each task has its own lock file. For example, worker 1 uses lock file 1, worker 2 uses lock file 2, etc. But the original description doesn't specify this. So perhaps the lock file name includes a unique identifier for the worker. But since the user hasn't provided that detail, maybe the lock file is per worker instance, but in the code, each mapped op would generate a unique lock file name based on the directory or some index.

Alternatively, maybe the lock is a global one, but the original uses multiple workers in parallel, which would conflict. That doesn't make sense. So perhaps the lock is per directory. For example, when processing directory A, the lock file is /tmp/airflow_log_cleanup_A.lock. But again, the original description says the lock file is /tmp/airflow_log_cleanup_worker.lock. So maybe the lock is per worker process. But if multiple workers are running in parallel, they would all try to create the same lock file, leading to conflicts. That suggests that the lock file name should be unique per worker. But the original description doesn't mention that. This is a point where I need to make an assumption.

Given that the original uses multiple parallel tasks with locking, perhaps each task uses a unique lock file. For example, the lock file could include the worker's index or the directory name. Since the original's lock file name is static, maybe that's a mistake, but for the Dagster code, I need to handle it correctly. So perhaps in the code, each worker uses a unique lock file name based on the directory they're processing.

Alternatively, maybe the lock is a system-wide lock to prevent multiple instances of the entire DAG from running concurrently. But the original's description says "file-based locking to prevent concurrent execution conflicts", which suggests that each worker uses a lock to prevent concurrent execution on the same resources. So perhaps each worker is responsible for a specific directory, and the lock is per directory. Therefore, the lock file name includes the directory's name or a unique identifier.

But without more details, I'll proceed by assuming that each worker uses a lock file named with a unique identifier, perhaps based on the directory they're processing. So, in the code, when a worker processes a directory, it creates a lock file specific to that directory.

Next, configuration handling. The original retrieves configuration from DAG run parameters or Airflow variables. In Dagster, this can be handled via config schemas. The job can have a config schema that includes max_log_age_in_days, enable_delete_child_log, and maybe directories to process. Resources can be used for shared configurations like the base log folder.

The original also mentions resources or config stubs. So perhaps create a resource for the Airflow configuration, which includes base_log_folder and other variables. Then, the ops can access this resource to get the necessary paths.

The three-phase cleanup process involves deleting old files, removing empty subdirs, and removing top-level dirs. Each of these can be implemented in Python within the op, using os and shutil modules. The original uses find commands, but in Dagster, we can use Python's file operations.

Error handling: The original mentions graceful error handling with lock file cleanup on failures. So in the op, we need to ensure that if an exception occurs, the lock file is removed. This can be done with a try/finally block.

Configurable sleep time to stagger worker execution. This can be handled by adding a sleep in each worker op, perhaps with a configurable delay based on the worker's index or some other parameter.

Now, structuring the code:

- Define a resource that holds the base log folder and other Airflow variables.
- The start_pipeline op is the entry point.
- An op to get the list of log directories to process. This could read from the resource's base_log_folder and list its subdirectories.
- For each directory, a mapped op (cleanup_worker) processes it. Each instance of the op would handle one directory.
- Each cleanup_worker op acquires a lock file, performs the three-phase cleanup, handles errors, and releases the lock.
- Finally, an op that runs after all workers complete.

Wait, the original's parallel tasks are multiple BashOperators, which suggests that each is a separate task. So in Dagster, using a mapped op makes sense. So the steps would be:

start -> get_log_dirs -> cleanup_worker.map(...) -> all_done.

But the original's Start is an EmptyOperator, which just starts the pipeline. So in Dagster, the start_pipeline op is the first step, then get_log_dirs depends on it, then the cleanup_worker ops depend on get_log_dirs, and then all_done depends on all cleanup_worker ops.

But perhaps the get_log_dirs op is part of the start_pipeline, or maybe start_pipeline is just a no-op and the get_log_dirs is the next step.

Alternatively, maybe the start_pipeline is the first op, and then get_log_dirs depends on it, followed by the mapped workers.

But the original's Start task is just an entry point. So in Dagster, the start_pipeline op can be the first op, then the get_log_directories op runs after it, which then triggers the mapped cleanup_worker ops.

But perhaps the get_log_directories op is part of the start task. Alternatively, maybe the list of directories is retrieved in the start op. But separating concerns is better.

So:

@op
def start_pipeline(context):
    context.log.info("Pipeline started")

@op
def get_log_directories(context, airflow_config):
    # Get base log folder from resource
    base_log_folder = airflow_config.base_log_folder
    # List subdirectories in base_log_folder
    directories = [os.path.join(base_log_folder, d) for d in os.listdir(base_log_folder) if os.path.isdir(os.path.join(base_log_folder, d))]
    return directories

@op
def cleanup_worker(context, directory, airflow_config, max_log_age, enable_child_log):
    # Implement the three-phase cleanup with locking
    lock_file = f"/tmp/airflow_log_cleanup_{os.path.basename(directory)}.lock"
    try:
        # Acquire lock
        with open(lock_file, 'w') as f:
            f.write(str(os.getpid()))
        # Phase 1: Delete old files
        # ...
        # Phase 2: Remove empty subdirs
        # ...
        # Phase 3: Remove top-level dirs if empty
        # ...
    finally:
        if os.path.exists(lock_file):
            os.remove(lock_file)

@op
def all_done(context):
    context.log.info("All workers completed")

Then, the job would be structured as:

@job(
    resource_defs={
        "airflow_config": ResourceDefinition.hardcoded_resource(AirflowConfigResource(
            base_log_folder="/airflow/logs",
            max_log_age=30,
            enable_delete_child_log=False
        ))
    },
    config={
        "ops": {
            "cleanup_worker": {
                "config": {
                    "max_log_age": {"env": "MAX_LOG_AGE"},
                    "enable_child_log": {"env": "ENABLE_CHILD_LOG"}
                }
            }
        }
    }
)
def airflow_log_cleanup_job():
    directories = get_log_directories(start_pipeline())
    results = cleanup_worker.map(directories=directories)
    all_done(results)

Wait, but the way to structure dependencies in Dagster is:

start -> get_log_dirs -> cleanup_worker.map -> all_done

So the code would be:

@job
def airflow_log_cleanup_job():
    start = start_pipeline()
    dirs = get_log_directories(start)
    workers = cleanup_worker.map(dirs)
    all_done(workers)

But each cleanup_worker op would need to take a single directory as input. So the get_log_directories op returns a list of directories, and the cleanup_worker is mapped over each element.

But in the code above, the cleanup_worker op's parameters need to accept a single directory. So the get_log_directories op returns a list, and the cleanup_worker is mapped over that list.

So, the get_log_directories op returns a list of directories. Then, using .map() on cleanup_worker would create an op for each directory in the list.

But in Dagster, when you map an op, each instance of the op receives a single element from the iterable. So the cleanup_worker op's directory parameter would be a single directory string.

Now, handling resources and configuration. The original uses Airflow variables and DAG run parameters. In Dagster, resources can be used to provide configuration that's shared across ops. The cleanup_worker op may need configuration parameters like max_log_age_in_days and enable_delete_child_log. These can be passed via config or resources.

For example, the job can have a config schema that includes max_log_age_in_days and enable_delete_child_log. Alternatively, these can be part of a resource.

But since the original allows runtime override via DAG run parameters, using config would be appropriate. So each cleanup_worker op can have a config schema that includes max_log_age and enable_child_log. However, since the worker is mapped, each instance would need to receive the same config, or the config can be set at the job level.

Alternatively, the config can be part of the resource. Let's create a resource that holds the Airflow configuration, including base_log_folder, max_log_age, enable_delete_child_log, etc.

So, defining a resource:

class AirflowConfigResource:
    def __init__(self, base_log_folder, max_log_age_in_days, enable_delete_child_log):
        self.base_log_folder = base_log_folder
        self.max_log_age_in_days = max_log_age_in_days
        self.enable_delete_child_log = enable_delete_child_log

Then, the resource can be provided to the job, and ops can access it via the context.

But the original also mentions that the DAG run configuration can override the max_log_age_in_days. So perhaps the job's config allows overriding this parameter, which is then passed to the resource or directly to the ops.

Alternatively, the job's config can specify these parameters, and the resource is built using the config values.

This is getting a bit complicated. Let's structure it as follows:

- The job has a config schema that includes max_log_age_in_days and enable_delete_child_log.
- A resource provides the base_log_folder and other static configurations.
- The cleanup_worker op uses both the resource and the job's config.

Wait, but resources are typically for external connections or shared configurations, while job config is for runtime parameters. So perhaps:

- The base_log_folder is part of a resource, as it's a static configuration.
- The max_log_age_in_days and enable_delete_child_log are part of the job's config, which can be set at runtime.

But the original allows these to be set via Airflow variables (which are like Dagster resources) or DAG run parameters (like Dagster's job config). So perhaps the job's config takes precedence over the resource's defaults.

Alternatively, the resource can be configured with default values from Airflow variables, but the job's config can override them.

This requires some handling. For simplicity, perhaps the job's config includes max_log_age_in_days and enable_delete_child_log, which are passed to the ops. The base_log_folder is part of a resource.

So, the resource is defined with base_log_folder, and the job's config includes the other parameters.

In code:

from dagster import op, job, resource, Field, Dict, List, Noneable, graph_asset

@resource(config_schema={
    "base_log_folder": Field(str, description="Base directory for Airflow logs"),
})
def airflow_config_resource(context):
    return context.resource_config

Then, in the job, the resource is configured with base_log_folder, and the job's config includes max_log_age_in_days and enable_delete_child_log.

But the cleanup_worker op needs access to both the resource (for base_log_folder) and the config (for max_log_age). Wait, no: the base_log_folder is used in the get_log_directories op to list the directories. The cleanup_worker op processes each directory, so it doesn't need the base_log_folder. The max_log_age and enable_delete_child_log are used in the cleanup_worker op.

So, perhaps the get_log_directories op uses the resource to get the base_log_folder, then returns the list of directories. The cleanup_worker op takes a directory as input and uses the job's config parameters (max_log_age_in_days and enable_delete_child_log) to perform the cleanup.

But how to pass the config to the mapped ops? Each instance of the cleanup_worker op would need access to the config. In Dagster, ops can have config schemas, and when an op is mapped, each instance can receive the same config or different configs. However, in this case, all workers would use the same max_log_age and enable_delete_child_log settings, so the config can be set at the job level and accessed by each op instance.

So, the cleanup_worker op would have a config schema that includes max_log_age_in_days and enable_delete_child_log. The job's config would specify these values, which are then passed to each instance of the cleanup_worker op.

Putting this together:

The job's config schema would include:

{
  "ops": {
    "cleanup_worker": {
      "config": {
        "max_log_age_in_days": int,
        "enable_delete_child_log": bool
      }
    }
  }
}

Then, in the cleanup_worker op, the context.op_config would contain these values.

But when using mapped ops, each instance of the op receives the same config. So if the job is configured with specific values, all workers use them.

Now, implementing the three-phase cleanup:

Phase 1: Delete old log files using find command with mtime filter. In Python, this can be done using os.walk and checking file modification times.

Phase 2: Remove empty subdirectories within log folders. Again, using os.rmdir after checking if empty.

Phase 3: Remove empty top-level log directories. After processing, check if the directory is empty and delete it.

Implementing these steps requires careful traversal and error handling.

Also, the lock file mechanism: each worker creates a lock file before starting and deletes it after finishing, even on failure. Using a try/finally block ensures the lock is removed.

Staggered sleep time: each worker sleeps for a configurable amount of time to distribute load. This can be handled by adding a sleep at the beginning of the op, with the duration based on a config parameter or the worker's index. However, since the workers are mapped, each instance could have a different sleep time. For example, if the directories are processed in order, each worker could sleep for (index * sleep_time) seconds. But the original mentions "configurable sleep time staggers worker execution", so perhaps the sleep time is a config parameter, and each worker sleeps for a random or staggered time based on that.

But for simplicity, perhaps the sleep time is a config parameter, and each worker sleeps for that duration. However, that wouldn't stagger them. Alternatively, each worker could sleep for a random time up to a maximum value. But the original says "configurable sleep time", so maybe the sleep time is a fixed value that each worker uses, causing them to stagger based on when they start. But in parallel execution, they would start at the same time. So perhaps the sleep time is multiplied by the worker's index. But since the workers are mapped, each has an index. However, in Dagster's mapped ops, each instance doesn't have an inherent index. So perhaps the directories are sorted, and each worker's sleep time is based on its position in the list. For example, the first directory's worker sleeps for 1 * sleep_time, the second for 2 * sleep_time, etc.

This requires passing the index along with the directory. But in a mapped op, each instance processes a single directory, but there's no built-in index. So perhaps the get_log_directories op returns a list of directories along with their indices, and the cleanup_worker op takes both the directory and the index as inputs. Alternatively, the sleep time can be random.

But the original says "configurable sleep time staggers worker execution to distribute load". So perhaps each worker sleeps for a random time between 0 and the configured sleep time. For example, if the sleep time is 10 seconds, each worker sleeps for a random number between 0 and 10. This would stagger their execution.

Implementing this in the cleanup_worker op:

import time
import random

sleep_time = context.op_config.get('sleep_time', 10)
time.sleep(random.uniform(0, sleep_time))

But the original's sleep time is part of the configuration. So the job's config should include a sleep_time parameter.

Now, putting all this together into code:

First, define the resources and config schemas.

The resource for Airflow config (base_log_folder):

@resource(config_schema={
    "base_log_folder": Field(str, default_value="/airflow/logs", description="Base directory for Airflow logs"),
})
def airflow_config_resource(context):
    return context.resource_config["base_log_folder"]

Wait, perhaps the resource is just the base_log_folder string. Alternatively, the resource can include other Airflow variables, but according to the original, the variables are airflow_log_cleanup__max_log_age_in_days and airflow_log_cleanup__enable_delete_child_log. These are likely part of the job's config, not the resource.

So, the resource provides the base_log_folder, and the job's config provides max_log_age_in_days and enable_delete_child_log.

The get_log_directories op uses the resource to get the base_log_folder and lists its subdirectories.

The cleanup_worker op uses the directory passed to it, along with the job's config parameters (max_log_age_in_days, enable_delete_child_log, sleep_time) to perform the cleanup.

Now, writing the code step by step.

First, the start_pipeline op:

@op
def start_pipeline(context):
    context.log.info("Pipeline started")

Then, the get_log_directories op, which depends on start_pipeline and uses the resource:

@op(required_resource_keys={"airflow_config"})
def get_log_directories(context):
    base_log_folder = context.resources.airflow_config["base_log_folder"]
    # List all subdirectories in base_log_folder
    directories = []
    for entry in os.listdir(base_log_folder):
        path = os.path.join(base_log_folder, entry)
        if os.path.isdir(path):
            directories.append(path)
    context.log.info(f"Found {len(directories)} log directories to process")
    return directories

Wait, the resource is defined with a config schema that includes base_log_folder. So the resource's configuration is passed via the resource_defs in the job.

Wait, the resource definition:

@resource(config_schema={
    "base_log_folder": Field(str, description="Base directory for Airflow logs", default_value="/airflow/logs"),
})
def airflow_config_resource(context):
    return context.resource_config

Then, in the job, the resource is provided as:

resource_defs={
    "airflow_config": airflow_config_resource
}

So, in the get_log_directories op, which requires the "airflow_config" resource, it can access context.resources.airflow_config["base_log_folder"].

But wait, the resource is configured with a config schema that includes "base_log_folder", so the resource's configuration is a dictionary with that key. So the resource function returns context.resource_config, which is the config dictionary. Therefore, in the op, to get the base_log_folder, it's context.resources.airflow_config["base_log_folder"].

Now, the cleanup_worker op. It needs to take a directory as input, and use the job's config parameters (max_log_age_in_days, enable_delete_child_log, sleep_time). So the op's config schema should include these parameters.

@op(config_schema={
    "max_log_age_in_days": Field(int, default_value=30, description="Maximum age of logs to retain"),
    "enable_delete_child_log": Field(bool, default_value=False, description="Enable deletion of child process logs"),
    "sleep_time": Field(int, default_value=10, description="Maximum sleep time to stagger worker execution"),
})
def cleanup_worker(context, directory):
    # Stagger execution
    sleep_time = context.op_config["sleep_time"]
    delay = random.uniform(0, sleep_time)
    context.log.info(f"Sleeping for {delay:.2f} seconds to stagger execution")
    time.sleep(delay)
    
    lock_file = f"/tmp/airflow_log_cleanup_{os.path.basename(directory)}.lock"
    try:
        # Acquire lock
        with open(lock_file, 'w') as f:
            f.write(str(os.getpid()))
        context.log.info(f"Acquired lock file {lock_file}")
        
        max_age_days = context.op_config["max_log_age_in_days"]
        enable_child = context.op_config["enable_delete_child_log"]
        
        # Phase 1: Delete old files
        cutoff_time = time.time() - (max_age_days * 86400)
        for root, dirs, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                file_mtime = os.path.getmtime(file_path)
                if file_mtime < cutoff_time:
                    os.remove(file_path)
                    context.log.info(f"Deleted old file: {file_path}")
        
        # Phase 2: Remove empty subdirectories
        for root, dirs, files in os.walk(directory, topdown=False):
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                if not os.listdir(dir_path):
                    os.rmdir(dir_path)
                    context.log.info(f"Removed empty directory: {dir_path}")
        
        # Phase 3: Remove top-level directory if empty
        if not os.listdir(directory):
            os.rmdir(directory)
            context.log.info(f"Removed empty top-level directory: {directory}")
            
    except Exception as e:
        context.log.error(f"Error during cleanup: {str(e)}")
        raise
    finally:
        if os.path.exists(lock_file):
            os.remove(lock_file)
            context.log.info(f"Released lock file {lock_file}")

Then, the all_done op:

@op
def all_done(context):
    context.log.info("All cleanup workers have completed")

Now, structuring the job:

@job(
    resource_defs={
        "airflow_config": airflow_config_resource
    },
    config={
        "ops": {
            "cleanup_worker": {
                "config": {
                    "max_log_age_in_days": 30,
                    "enable_delete_child_log": False,
                    "sleep_time": 10
                }
            }
        }
    }
)
def airflow_log_cleanup_job():
    start = start_pipeline()
    directories = get_log_directories(start)
    workers = cleanup_worker.map(directories=directories)
    all_done(workers)

Wait, the cleanup_worker op's input is named 'directory', but when mapping, the input is 'directories' (the list from get_log_directories). So in the .map() call, we need to specify that each element of 'directories' is passed to the 'directory' parameter of the cleanup_worker op.

In Dagster, when using .map(), the argument name should match the op's input parameter name. So if the cleanup_worker op has an input called 'directory', then the .map() call should be:

cleanup_worker.map(directory=directories)

But the get_log_directories op returns a list of directories. So:

directories = get_log_directories(start)
workers = cleanup_worker.map(directory=directories)

Yes. So the code would be:

@job(
    resource_defs={
        "airflow_config": airflow_config_resource
    },
    config={
        "ops": {
            "cleanup_worker": {
                "config": {
                    "max_log_age_in_days": 30,
                    "enable_delete_child_log": False,
                    "sleep_time": 10
                }
            }
        }
    }
)
def airflow_log_cleanup_job():
    start = start_pipeline()
    dirs = get_log_directories(start)
    workers = cleanup_worker.map(directory=dirs)
    all_done(workers)

Finally, the main block:

if __name__ == "__main__":
    result = airflow_log_cleanup_job.execute_in_process()

Putting all together, here's the complete code:

import os
import time
import random
from dagster import op, job, resource, Field

@resource(config_schema={
    "base_log_folder": Field(str, default_value="/airflow/logs", description="Base directory for Airflow logs"),
})
def airflow_config_resource(context):
    return context.resource_config

@op
def start_pipeline(context):
    context.log.info("Pipeline started")

@op(required_resource_keys={"airflow_config"})
def get_log_directories(context):
    base_log_folder = context.resources.airflow_config["base_log_folder"]
    directories = []
    for entry in os.listdir(base_log_folder):
        path = os.path.join(base_log_folder, entry)
        if os.path.isdir(path):
            directories.append(path)
    context.log.info(f"Found {len(directories)} log directories to process")
    return directories

@op(config_schema={
    "max_log_age_in_days": Field(int, default_value=30),
    "enable_delete_child_log": Field(bool, default_value=False),
    "sleep_time": Field(int, default_value=10),
})
def cleanup_worker(context, directory):
    sleep_time = context.op_config["sleep_time"]
    delay = random.uniform(0, sleep_time)
    context.log.info(f"Sleeping for {delay:.2f} seconds")
    time.sleep(delay)
    
    lock_file = f"/tmp/airflow_log_cleanup_{os.path.basename(directory)}.lock"
    try:
        with open(lock_file, 'w') as f:
            f.write(str(os.getpid()))
        context.log.info(f"Lock acquired: {lock_file}")
        
        max_age = context.op_config["max_log_age_in_days"]
        cutoff = time.time() - (max_age * 86400)
        
        # Phase 1: Delete old files
        for root, _, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                if os.path.getmtime(file_path) < cutoff:
                    os.remove(file_path)
                    context.log.info(f"Deleted: {file_path}")
        
        # Phase 2: Remove empty subdirs
        for root, dirs, _ in os.walk(directory, topdown=False):
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                if not os.listdir(dir_path):
                    os.rmdir(dir_path)
                    context.log.info(f"Removed empty dir: {dir_path}")
        
        # Phase 3: Remove top-level if empty
        if not os.listdir(directory):
            os.rmdir(directory)
            context.log.info(f"Removed top dir: {directory}")
            
    except Exception as e:
        context.log.error(f"Error: {e}")
        raise
    finally:
        if os.path.exists(lock_file):
            os.remove(lock_file)
            context.log.info(f"Lock released: {lock_file}")

@op
def all_done(context):
    context.log.info("All workers completed")

@job(
    resource_defs={"airflow_config": airflow_config_resource},
    config={
        "ops": {
            "cleanup_worker": {
                "config": {
                    "max_log_age_in_days": 30,
                    "enable_delete_child_log": False,
                    "sleep_time": 10
                }
            }
        }
    }
)
def airflow_log_cleanup_job():
    dirs = get_log_directories(start_pipeline())
    cleanup_worker.map(directory=dirs)
    all_done()

if __name__ == "__main__":
    airflow_log_cleanup_job.execute_in_process()

Wait, but in the job function, the dependencies are not properly set. The code as written would call get_log_directories(start_pipeline()), which is correct. Then, cleanup_worker.map(directory=dirs) creates mapped ops, but the all_done op depends on them. However, in the current code, the all_done() op is not connected to the cleanup_worker.map() result.

In Dagster, the syntax should be:

workers = cleanup_worker.map(directory=dirs)
all_done(workers)

But the all_done op doesn't take any inputs. So this would cause an error because the op expects no inputs but is given a list of outputs from the mapped ops.

To fix this, the all_done op should depend on the completion of all cleanup_worker ops. Since the mapped ops' outputs are a list, but all_done doesn't need them, we can use the fact that in Dagster, an op's dependencies are determined by the inputs it receives. If all_done has no inputs, it won't depend on anything. So to make all_done depend on the completion of all cleanup_worker ops, we need to pass the result of the mapped ops as an input to all_done, even if it's not used.

So modifying the all_done op to accept a list:

@op
def all_done(context, _):
    context.log.info("All workers completed")

But then, the mapped ops produce a list of outputs (each cleanup_worker returns None, perhaps), and all_done takes this list as input. However, since the cleanup_worker doesn't return anything, the mapped ops would output a list of Nones.

Alternatively, have the cleanup_worker return a success status, but for simplicity, we can have the all_done op take a list of Any as input.

So modifying the all_done op:

@op
def all_done(context, _):
    context.log.info("All workers completed")

Then, in the job:

workers = cleanup_worker.map(directory=dirs)
all_done(workers.collect())

But collect() is used to gather the results of mapped ops into a list. However, in Dagster, when you map an op, the output is a list of the individual op outputs. So to pass all the outputs to all_done, you can collect them.

Wait, in Dagster, when you have a mapped op, the output is a list of the results of each individual op instance. So if cleanup_worker has no output, then workers will be a list of None, and passing workers to all_done would require all_done to take a list input.

But the current cleanup_worker op doesn't have an output defined. So by default, each instance returns Nothing, and the mapped output is a list of Nothing. However, in Dagster, you can't pass a list of Nothing as an input to another op. So to make all_done depend on the completion of all cleanup_worker ops, the all_done op must have an input that depends on the output of the mapped ops.

But since the cleanup_worker doesn't return anything, perhaps we can have it return a dummy value, like True, to indicate completion.

Modifying the cleanup_worker op:

@op(config_schema={...})
def cleanup_worker(context, directory):
    # ... existing code ...
    return True

Then, the mapped workers will output a list of True values. The all_done op can take this list as input:

@op
def all_done(context, results):
    context.log.info(f"All {len(results)} workers completed")

Then, in the job:

workers = cleanup_worker.map(directory=dirs)
all_done(workers.collect())

But collect() is used when the mapped op is dynamic, but in this case, the mapping is static. So perhaps:

all_done(workers)

But in Dagster, when you have a mapped op, the output is a list of the individual outputs. So if each cleanup_worker returns True, workers will be a list of True values. Then, passing workers to all_done would require all_done to take a list input.

Alternatively, use the fact that the all_done op must run after all