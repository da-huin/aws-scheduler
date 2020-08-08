
from .kernel import scheduler

def deploy(template_dir="template/", 
                aws_access_key_id=None,
                aws_secret_access_key=None,
                region_name=None,
                no_cache=False,
                delete_unmanaged=False):
    """
    This package makes it easy to manage AWS Glue Crawler and AWS Cloudwatch schedulers on AWS services.

    Parameters
    ----------
    * (required) template_dir: `str`
        YOUR_TEMPLATES_DIRECTORY

    * aws_access_key_id: `str, aws_secret_access_key: `str`
        You don't need to use these two parameters if your authentication file is in ~/.aws/config.
    
    * region_name: `str`
        YOUR_REGION_NAME

    * no_cache: (default: False)
        If you don't use the cache, all schedulers are redistributed.

    * delete_unmanaged: bool (default: False)
        (Caution!) Setting this value to True will automatically delete unmanaged schedulers from this package. 
        And If you leave this value as False, the deletion will not be automatic.
    """

    handler = scheduler.Scheduler(template_dir, aws_access_key_id, aws_secret_access_key, region_name)
    handler.deploy(no_cache=no_cache, delete_unmanaged=delete_unmanaged)
