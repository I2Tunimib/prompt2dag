result = task1()
   if result["should_proceed"]:
       task2(result["file_path"])
       task3(result["file_path"])