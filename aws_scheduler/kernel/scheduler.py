import re
from pprint import pprint
import json
import os
import yaml
import template_manager
import boto3
from . import schema


class Scheduler():
    def __init__(self, template_dir, aws_access_key_id=None, aws_secret_access_key=None, region_name=None, legacy_template_path="/tmp/aws_scheduler"):

        os.makedirs(legacy_template_path, exist_ok=True)

        self._legacy_template_path = legacy_template_path
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._region_name = region_name
        self._tm = template_manager.TemplateManager(template_dir)

        self._tm.register("glue",
                          worker=lambda x: x,
                          template_schema=schema.get_glue_schema())

        self._tm.register("cloudwatch",
                          worker=lambda x: x,
                          template_schema=schema.get_cloudwatch_schema())

        self._event_client = self._load_client("events")
        self._lambda_client = self._load_client("lambda")
        self._glue_client = self._load_client("glue")
        self._s3_client = self._load_client("s3")
        self._iam_client = self._load_client("iam")
        self._tags = {'created_by': 'aws_scheduler'}

    def deploy(self, no_cache=False, delete_unmanaged=False):
        filtered_templates = self.filter_template()

        templates = None

        if no_cache:
            templates = self._tm._templates
        else:
            templates = filtered_templates

        if delete_unmanaged:
            self._delete_unmanaged_glue_crawlers()
            self._delete_unmanaged_cloudwatch_rules()

        for key in templates:
            template = templates[key]
            kind = template["kind"]
            name = template["name"]
            spec = self._tm.get_spec(name)

            if kind == "cloudwatch":
                self._put_cloudwatch_event(spec)
            elif kind == "glue":
                self._put_glue(spec)
            else:
                raise ValueError(f"invalid kind {kind}")

        self._save_legacy_templates()

    def _save_legacy_templates(self):
        with open(self._get_legacy_template_path(), "w", encoding="utf-8") as fp:
            fp.write(json.dumps(self._tm._templates, ensure_ascii=False))

    def _load_legacy_templates(self):
        result = ""
        if os.path.isfile(self._get_legacy_template_path()):

            with open(self._get_legacy_template_path(), "r", encoding="utf-8") as fp:
                result = json.loads(fp.read())
        else:
            result = None

        return result

    def filter_template(self):
        result = {}
        legacy_templates = self._load_legacy_templates()
        if legacy_templates == None:
            return self._tm._templates

        
        for t_name in self._tm._templates:
            t = self._tm._templates[t_name]["origin"]

            same = False
            for lt_name in legacy_templates:
                lt = legacy_templates[lt_name]["origin"]
                if lt_name == t_name and t.get("spec") == lt.get("spec"):
                    same = True
                    break


            if not same:
                result[t_name] = t

        return result

    def _get_legacy_template_path(self):
        return self._legacy_template_path + "/scheduler-legacy-template.json"

    def _list_glue_crawlers(self):
        crawlers = []
        next_token = ""

        while True:
            response = self._glue_client.list_crawlers(
                MaxResults=1000, NextToken=next_token, Tags=self._tags)

            crawlers.extend(response["CrawlerNames"])
            new_next_token = response.get("NextToken", "")

            if next_token == new_next_token:
                break

            next_token = new_next_token

        return crawlers

    def _list_cloudwatch_rules(self):
        rules = []

        response = self._event_client.list_rules()

        rules.extend([rule["Name"] for rule in response["Rules"]])

    
        return rules

    def _delete_unmanaged_glue_crawlers(self):

        managed_crawlers = [self._tm.get_spec(name)["name"] for name in self._tm.find("glue")]

        unmanaged_crawlers = list(set(self._list_glue_crawlers()) - set(managed_crawlers))

        for crawler_name in unmanaged_crawlers:
            self._glue_client.delete_crawler(Name=crawler_name)
            print(f"[glue] {crawler_name} deleted (unmanaged crawler).")

        return unmanaged_crawlers

    def _delete_unmanaged_cloudwatch_rules(self):
        
        managed_rule = []
        for name in self._tm.find("cloudwatch"):
            spec = self._tm.get_spec(name)
            function_name = spec.get("FunctionName", spec["name"])
            managed_rule.append(function_name)

        unmanaged_rule = list(set(self._list_cloudwatch_rules()) - set(managed_rule))

        for rule_name in unmanaged_rule:

            target_id = f"{rule_name}-target"

            self._event_client.remove_targets(Rule=rule_name, Ids=[target_id])
            # Rule can't be deleted since it has targets.

            self._event_client.delete_rule(Name=rule_name)
            print(f"[cloudwatch] {rule_name} deleted (unmanaged rule).")

        return unmanaged_rule

    def _put_glue(self, spec):

        name = spec["name"]
        database_name = spec.get("DatabaseName", name)

        try:
            self._glue_client.get_database(Name=name)
        except:
            print(f"{name} database created.")
            self._glue_client.create_database(DatabaseInput={
                "Name": name,
                "Description": "Database",
            })
        deprecated = spec.get("deprecated", False)
        description = spec.get("Description", "")
        try:
            result = self._glue_client.delete_crawler(Name=name)
            # print(f"{name} crawler deleted.")
        except:
            pass
        
        if not deprecated:
            glue_role_name = "aws_scheduler_role"
            
            try:
                self._iam_client.get_role(RoleName=glue_role_name)
            except:

                self._iam_client.create_role(RoleName=glue_role_name, AssumeRolePolicyDocument=json.dumps(
                    {'Version': '2012-10-17', 'Statement': [{'Effect': 'Allow', 'Principal': {'Service': 'glue.amazonaws.com'}, 'Action': 'sts:AssumeRole'}]}))

                self._iam_client.attach_role_policy(
                    RoleName=glue_role_name,
                    PolicyArn="arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"
                )


            result = self._glue_client.create_crawler(
                Name=name, Description=description, Role=glue_role_name,
                Targets={
                    "S3Targets": [{"Path": spec["S3TargetPath"]}],
                }, Schedule=spec["Schedule"], DatabaseName=database_name, SchemaChangePolicy={
                    "UpdateBehavior": "UPDATE_IN_DATABASE",
                    "DeleteBehavior": "DELETE_FROM_DATABASE",
                }, Tags=self._tags
            )
            print(f"[glue] {name} crawler created.")

        return result

    def _put_cloudwatch_event(self, spec):
        name = spec["name"]
        function_name = spec.get("FunctionName", name)

        description = spec.get("Description", "")
        schedule_expression = spec.get("Schedule", "")
        event_pattern = spec.get("EventPattern", "")
        if event_pattern:
            event_pattern = json.dumps(event_pattern, ensure_ascii=False)
        input_value = spec.get("Input", {})
        deprecated = spec.get("deprecated", False)
        target_id = f"{function_name}-target"

        try:
            result = []
            result.append(self._event_client.remove_targets(
                Rule=function_name, Ids=[target_id]))
            result.append(
                self._event_client.delete_rule(Name=function_name))

        except self._event_client.exceptions.ResourceNotFoundException:
            pass
        
        if not deprecated:
            try:
                self._lambda_client.add_permission(
                    FunctionName=function_name,
                    StatementId=function_name + "_Statement",
                    Action='lambda:InvokeFunction',
                    Principal="*"
                )

            except:
                pass     

            self._event_client.put_rule(
                Name=name, EventPattern=event_pattern, ScheduleExpression=schedule_expression, State='ENABLED', Description=description)

            finded_lambda = self._lambda_client.get_function(
                FunctionName=function_name)
            lambda_arn = finded_lambda["Configuration"]["FunctionArn"]

            result = self._event_client.put_targets(Rule=name, Targets=[{
                "Id": target_id,
                "Arn": lambda_arn,
                "Input": json.dumps(input_value, ensure_ascii=False)
            }])
            print(f"[cloudwatch] {name} rule created.")

        return result

    def _load_client(self, kind):
        return boto3.client(kind,
                            aws_access_key_id=self._aws_access_key_id,
                            aws_secret_access_key=self._aws_secret_access_key,
                            region_name=self._region_name)
