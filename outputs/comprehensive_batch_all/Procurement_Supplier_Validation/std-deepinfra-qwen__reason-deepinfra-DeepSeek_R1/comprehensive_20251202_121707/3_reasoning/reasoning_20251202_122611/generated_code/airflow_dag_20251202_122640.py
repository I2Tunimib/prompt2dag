<think>
Okay, I need to create an Apache Airflow DAG for the Procurement Supplier Validation Pipeline based on the given description. Let me start by understanding the pipeline steps and the constraints provided.

First, the pipeline has three main steps: Load & Modify Data, Entity Reconciliation with Wikidata, and Save Final Data. Each step uses a Docker image and communicates via specific ports. The DAG needs to orchestrate these tasks in sequence.

Since each step is a Dockerized service, I should use the DockerOperator in Airflow. I'll need to import DockerOperator from airflow.providers.docker.operators.docker. Also, make sure to set the Docker connection properly. The default_args should include retries as specified, which is 1.

Each task will run a container from the respective image. The first task, load_and_modify, uses the image i2t-backendwithintertwino6-load-and-modify:latest. It needs environment variables DATASET_ID=2 and TABLE_NAME_PREFIX=JOT_. The command might be to start the service on port 3003, but since the API integration mentions calling the service, maybe the container is already set up to perform the task when run. The volumes should mount the DATA_DIR from the host to the container, probably /data as per the environment variable.

Next, the entity_reconciliation task uses the reconciliation service image. It has PRIMARY_COLUMN=supplier_name, RECONCILIATOR_ID=wikidataEntity, and DATASET_ID=2. This task depends on the output of the first task, so the dependency should be set using >> operator.

The third task, save_final_data, uses the save service image with DATASET_ID=2. It takes the output from the previous step and saves it to CSV.

All tasks need to be part of the same Docker network, app_network, so the network_mode should be set to that. Also, each container should auto-remove after execution to handle cleanup.

I should define the DAG with a start date, maybe a daily schedule_interval, but the user didn't specify, so I'll set it to None. Catchup should be False to prevent backfilling.

Environment variables for each task should include DATA_DIR, which is mounted as a volume. The DockerOperator's parameters include the environment variables, network mode, auto_remove, and the command if necessary. Wait, the problem says each step uses an API integration on port 3003, but the DockerOperator runs a container. Maybe the services are already running, and the containers execute the specific tasks by making API calls? Or perhaps each container runs the service and the task is to trigger it. Hmm, maybe the DockerOperator just runs the container which performs the step, assuming that the necessary services (like MongoDB on port 27017 and Intertwino API on 5005) are available on the app_network.

So, each task's container is part of app_network, allowing them to communicate with MongoDB and the API. The DockerOperator will start the container, which performs the respective step, then exits. The auto_remove ensures the container is cleaned up.

I need to set the docker_conn_id, but if it's using the default Docker socket, maybe it's 'docker.default'. Alternatively, if a connection is needed, the user would have to set it up, but the code should reference it. For the sake of the code, I'll include docker_conn_id='docker_default' as a common setup.

Putting it all together, the DAG will have three tasks connected in sequence. Each uses DockerOperator with the respective image, environment variables, network settings, and volumes.

Let me check the parameters again. The first task's output is table_data_2.json, which the second task uses as input. The second task produces reconciled_table_2.json, which the third task uses. Since they're all writing to DATA_DIR, the volume mount ensures they can access these files.

Now, writing the code:

Import DAG and necessary operators. The DockerOperator is from airflow.providers.docker.operators.docker. Default args include retries=1, owner, etc.

Define the DAG with dag_id, schedule_interval=None, catchup=False, default_args, tags.

Then define each task with DockerOperator. The environment variables for each task are based on the key parameters. The volumes should mount the host's DATA_DIR (from environment variable) to /data in the container. Assuming DATA_DIR is set in the Airflow environment, perhaps as a variable, but for the code, it's passed as a volume. So, the code uses ['{{ var.value.DATA_DIR }}:/data'] but maybe the user expects a placeholder. Alternatively, use os.environ.get('DATA_DIR', '/default/data'), but since the problem says to assume required packages are installed, perhaps the DATA_DIR is configured externally, and the volume is set as '/data:/data'? Wait, the problem states "Shared volume mounting (DATA_DIR via environment variable)", so the host's DATA_DIR is mounted into the container's /data. So in the DockerOperator, volumes should be [f"{os.environ['DATA_DIR']}:/data"], but using Airflow variables might be better. However, the exact setup depends on how DATA_DIR is configured. To make it flexible, perhaps use a Variable.get('DATA_DIR') and pass it as an environment variable to the tasks. Alternatively, mount the host directory as a volume.

Wait, the DockerOperator's volumes parameter expects a list of strings in the format 'host_dir:container_dir'. So if the DATA_DIR is an environment variable on the Airflow worker, then in the DockerOperator, volumes can be set as [f"{os.environ['DATA_DIR']}:/data"], but this might not be safe if the variable isn't set. Alternatively, use a default path. However, the problem says to avoid placeholders and use minimal safe defaults. So perhaps the code should use a placeholder like Mount(source='/path/to/data', target='/data', type='bind'), but the user expects the DATA_DIR to be handled via environment variables. Maybe the DockerOperator's environment variables include DATA_DIR, and the container uses that. But the problem says "Shared volume mounting (DATA_DIR via environment variable)", implying that the host's DATA_DIR is mounted into the container's /data via a volume. So in the DockerOperator, volumes should be [f"{os.environ.get('DATA_DIR', '/opt/data')}:/data"], but how to get the host's DATA_DIR into the Airflow worker's environment? That's probably setup outside the DAG code. So in the code, the volumes would be ['${DATA_DIR}:/data'], but in Python, we can't use shell variables directly. So using os.environ.get('DATA_DIR', '/default/data') to get the host's DATA_DIR and mount it. So in the code:

volumes = [f"{os.environ.get('DATA_DIR', '/tmp/data')}:/data"]

But this requires importing os. So in the code, import os, then use os.environ.get.

Alternatively, if the DATA_DIR is set as an Airflow variable, use Variable.get('DATA_DIR'), but the problem says "via environment variable", so probably using the host's environment variable.

So, in each DockerOperator task, set volumes as [f"{os.environ.get('DATA_DIR', '/tmp/data')}:/data"].

But the user might not have os imported. So need to import os in the code.

Now, putting all together:

Import DAG, DockerOperator, etc.

Default args: owner, depends_on_past, email, etc., with retries=1.

Define the DAG with appropriate parameters.

Then, the three tasks:

load_and_modify = DockerOperator(
    task_id='load_and_modify',
    image='i2t-backendwithintertwino6-load-and-modify:latest',
    command=None,  # assuming the image's entrypoint is correctly set
    environment={
        'DATASET_ID': '2',
        'TABLE_NAME_PREFIX': 'JOT_'
    },
    network_mode='app_network',
    auto_remove=True,
    docker_conn_id='docker_default',
    volumes=[f"{os.environ.get('DATA_DIR', '/data')}:/data"]
)

Similarly for the other tasks. The entity_reconciliation task has environment variables PRIMARY_COLUMN, RECONCILIATOR_ID, DATASET_ID. The save_final_data task has DATASET_ID.

Then, set dependencies with load_and_modify >> entity_reconciliation >> save_final_data.

Wait, the problem mentions error handling through retries (default 1), which is set in default_args. Also, the Docker network is app_network, so network_mode='app_network' in each task.

I need to make sure that the DockerOperator parameters are correctly set. The command might be necessary if the image requires a specific command to run, but if the image's ENTRYPOINT is set up to run the service when the container starts, then maybe no command is needed. Alternatively, if the service is started with a command, but the problem doesn't specify, so perhaps leave command as None.

Testing the code: Each task runs a Docker container with the specified image, environment variables, network, and volumes. The tasks are sequential.

So the final code would look like this, with proper imports, DAG setup, and tasks.
</think>

import os
from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'email_on_failure': False,
    'email_on_retry': False,
}

data_dir = os.environ.get('DATA_DIR', '/data')

with DAG(
    dag_id='procurement_supplier_validation',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['procurement', 'supplier'],
) as dag:

    load_and_modify = DockerOperator(
        task_id='load_and_modify',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        api_version='auto',
        auto_remove='success',
        environment={
            'DATASET_ID': '2',
            'TABLE_NAME_PREFIX': 'JOT_'
        },
        network_mode='app_network',
        docker_url='unix://var/run/docker.sock',
        volumes=[f'{data_dir}:/data'],
    )

    entity_reconciliation = DockerOperator(
        task_id='entity_reconciliation',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        api_version='auto',
        auto_remove='success',
        environment={
            'PRIMARY_COLUMN': 'supplier_name',
            'RECONCILIATOR_ID': 'wikidataEntity',
            'DATASET_ID': '2'
        },
        network_mode='app_network',
        docker_url='unix://var/run/docker.sock',
        volumes=[f'{data_dir}:/data'],
    )

    save_final_data = DockerOperator(
        task_id='save_final_data',
        image='i2t-backendwithintertwino6-save:latest',
        api_version='auto',
        auto_remove='success',
        environment={'DATASET_ID': '2'},
        network_mode='app_network',
        docker_url='unix://var/run/docker.sock',
        volumes=[f'{data_dir}:/data'],
    )

    load_and_modify >> entity_reconciliation >> save_final_data