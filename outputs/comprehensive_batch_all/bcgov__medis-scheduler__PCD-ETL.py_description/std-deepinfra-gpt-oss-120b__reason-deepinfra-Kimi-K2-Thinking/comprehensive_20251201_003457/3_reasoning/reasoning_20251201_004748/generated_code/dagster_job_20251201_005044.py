@op
def start_extract_1():
    return None

@op
def extract_task_1(start_extract_1):
    ...

@op
def extract_task_2(start_extract_1):
    ...

@op
def upload_file(extract_task_1, extract_task_2, ...):
    ...