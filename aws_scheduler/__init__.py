
from .kernel import scheduler

def deploy(template_dir="template/", 
                aws_access_key_id=None,
                aws_secret_access_key=None,
                region_name=None,
                no_cache=False,
                delete_unmanaged=False):

    handler = scheduler.Scheduler(template_dir, aws_access_key_id, aws_secret_access_key, region_name)
    handler.deploy(no_cache=no_cache, delete_unmanaged=delete_unmanaged)
