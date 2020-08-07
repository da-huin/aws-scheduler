import scheduler

handler = scheduler.Scheduler()

def deploy(template_dir="template/", 
            delete_anther_schedule=False,
                aws_access_key_id=None,
                aws_secret_access_key=None,
                region_name=None):
    handler.deploy(template_dir, delete_anther_schedule, aws_access_key_id, aws_secret_access_key, region_name)
