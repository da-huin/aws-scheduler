<p align="center">
  <a href="" rel="noopener">
 <img width=200px height=200px src="./static/icon.png" alt="Project logo" ></a>
 <br>

 
</p>

<h3 align="center">AWS SCHEDULER</h3>

<div align="center">

[![Status](https://img.shields.io/badge/status-active-success.svg)]()
[![GitHub Issues](https://img.shields.io/github/issues/da-huin/aws_scheduler.svg)](https://github.com/kylelobo/The-Documentation-Compendium/issues)
[![GitHub Pull Requests](https://img.shields.io/github/issues-pr/da-huin/aws_scheduler.svg)](https://github.com/kylelobo/The-Documentation-Compendium/pulls)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](/LICENSE)

</div>

---

<p align="center"> This package makes it easy to manage AWS Glue Crawler and AWS Cloudwatch schedulers on AWS services.
    <br> 
</p>

## 📝 Table of Contents

- [About](#about)
- [Getting Started](#getting_started)
- [Reference](#reference)
- [Acknowledgments](#acknowledgement)

## 🧐 About <a name = "about"></a>

This package makes it easy to manage AWS Glue Crawler and AWS Cloudwatch schedulers on AWS services.

You can use following functions.

* [deploy](#deploy)

## 🏁 Getting Started <a name = "getting_started"></a>

### Installing

```
pip install aws_scheduler
```

### Tutorial

1. create `templates folder` wherever you want.

1. create `yaml` file in `just created templates folder`.

    ```
    templates/
    - hello.yaml
    ```

1. edit `just created yaml file`.

1. write this code to yaml file.

    * details refer to [HERE](#yaml)

    ```yaml
    kind: cloudwatch
    name: cloudwatch-helloworld
    spec:
    Schedule: cron(0 4 * * ? *)
    name: HelloWorld
    ---
    kind: glue
    name: glue-helloworld
    spec:
    S3TargetPath: s3://YOUR_BUCKET_NAME/helloworld
    Schedule: cron(0 4 * * ? *)
    name: HelloWorld
    ```

1. Run this code

    ```python
    import aws_scheduler

    template_dir = "YOUR_TEMPLATES_DIRECTORY"

    # You don't need to use these two parameters if your authentication file is in ~/.aws/config.
    aws_access_key_id = "YOUR_AWS_ACCESS_KEY_ID"
    aws_secret_access_key = "YOUR_AWS_SECRET_ACCESS_KEY"
    region_name = "YOUR_REGION_NAME"

    # (Caution!) Setting this value to True will automatically delete unmanaged schedulers from this package. 
    # And If you leave this value as False, the deletion will not be automatic.
    delete_unmanaged = False

    # If you don't use the cache, all schedulers are redistributed.
    no_cache = False

    aws_scheduler.deploy(template_dir,aws_access_key_id, aws_secret_access_key, region_name, no_cache=no_cache,
                        delete_unmanaged=delete_unmanaged)
    ```

    run result:
    ```
    [cloudwatch] HelloWorld rule created.
    [glue] HelloWorld crawler created.
    ```

1. You can check it here.

    Glue Console: https://aws.amazon.com/glue/

    Lambda Console: https://console.aws.amazon.com/lambda/home

## 🎈 Reference <a name="reference"></a>

<a name="yaml"></a>

* Schedule format: https://docs.aws.amazon.com/lambda/latest/dg/services-cloudwatchevents-expressions.html

```yaml
kind: cloudwatch
name: cloudwatch-helloworld
spec:
    Schedule: cron(0 4 * * ? *)
    name: HelloWorld
    # if omit FunctionName, default value is used be [name] value.
    FunctionName: HelloWorld
    # The value you pass to the lambda. Can be omitted.
    Input:
        - hello
        - world
    # you can use EventPattern. Can be omitted.
    EventPattern:
        source:
            - aws.glue
        detail-type:
        - Detail Type
        detail:
            state:
            - Succeeded
            crawlerName:
            - YourCrawelrName
---
kind: glue
name: glue-helloworld
spec:
    S3TargetPath: s3://YOUR_BUCKET_NAME/helloworld
    # aws Schedule
    Schedule: cron(0 4 * * ? *)
    name: HelloWorld
    # if omit DatabaseNamm, default value is used be [name] value.
    DatabaseName: HelloWorld
```

## 🎉 Acknowledgements <a name = "acknowledgement"></a>

- Title icon made by [Freepik](https://www.flaticon.com/kr/authors/freepik).

- If you have a problem. please make [issue](https://github.com/da-huin/aws_scheduler/issues).

- Please help develop this project 😀

- Thanks for reading 😄
