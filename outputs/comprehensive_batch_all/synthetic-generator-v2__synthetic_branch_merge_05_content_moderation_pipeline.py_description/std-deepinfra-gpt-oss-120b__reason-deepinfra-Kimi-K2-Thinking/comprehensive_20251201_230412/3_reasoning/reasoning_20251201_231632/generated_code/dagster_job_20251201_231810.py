@op(out={
       "toxic_content": Out(is_required=False),
       "safe_content": Out(is_required=False)
   })
   def toxicity_check(context, csv_metadata):
       # logic
       if toxicity > threshold:
           yield Output(content, "toxic_content")
       else:
           yield Output(content, "safe_content")