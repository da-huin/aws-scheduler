import re
from pprint import pprint
import json
import os
import yaml
import template

class Scheduler():
    def __init__(self):
        self.template_handler = template.Template()
        info = self.utils.get_info()
        self.glue_role_arn = info["glue_role_arn"]
        self.event_client = self.utils.load_client("events")
        self.lambda_client = self.utils.load_client("lambda")
        self.glue_client = self.utils.load_client("glue")
        self.s3_client = self.utils.load_client("s3")

        self.scheduler_templates_path = self.utils.get_path(
            "resources_scheduler_templates")
        self.template_handler.init(self.scheduler_templates_path)
        self.origin_templates = self.template_handler.find(
            "glue") + self.template_handler.find("cloudwatch")
        self.filtered_templates = self.filter_template()
        self.prefix = info["sceduler_prefix"]

    def save_legacy_templates(self):
        with open(self.get_legacy_template_path(), "w", encoding="utf-8") as fp:
            fp.write(json.dumps(self.origin_templates, ensure_ascii=False))

    def load_legacy_templates(self):
        result = ""
        if os.path.isfile(self.get_legacy_template_path()):

            with open(self.get_legacy_template_path(), "r", encoding="utf-8") as fp:
                result = json.loads(fp.read())
        else:
            result = None

        return result

    def filter_template(self):
        result = []
        legacy_templates = self.load_legacy_templates()
        if legacy_templates == None:
            return self.origin_templates

        for at in self.origin_templates:
            at_name = at["name"]

            same = False
            for lt in legacy_templates:
                lt_name = lt["name"]
                # pprint(at)
                # exit(0)
                if lt_name == at_name and at["template"].get("spec") == lt["template"].get("spec"):
                    same = True
                    break

            if not same:
                result.append(at)

        return result

    def get_legacy_template_path(self):

        os.makedirs(self.utils.get_path("resources_temp"), exist_ok=True)

        return self.utils.get_path("resources_temp") + "/scheduler-legacy-template.json"

    def make_snake(self, name):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    def put_glue(self, spec):
        name = spec["name"]
        db_name = self.make_snake(self.prefix) + "_" + self.make_snake(name)
        crawler_name = f"{self.prefix}{name}Crawler"

        try:
            self.glue_client.get_database(Name=db_name)
        except:
            self.glue_client.create_database(DatabaseInput={
                "Name": db_name,
                "Description": "Database",
            })
            # self.glue_client.get_crawler(Name=crawler_name)

        try:
            self.glue_client.delete_crawler(Name=crawler_name)
        except:
            pass

        result = self.glue_client.create_crawler(
            Name=crawler_name, Description=spec["Description"], Role=self.glue_role_arn,
            Targets={
                "S3Targets": [{"Path": spec["S3TargetPath"]}],
            }, Schedule=spec["Schedule"], DatabaseName=db_name, SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "DELETE_FROM_DATABASE",
            })

        print(result)

    def put_cloudwatch_event(self, spec):
        name = spec["name"]
        function_name = spec.get("FunctionName", name)
        key = self.prefix + function_name
        name_key = self.prefix + name

        description = spec.get("Description", "")
        schedule_expression = spec.get("Schedule", "")
        event_pattern = spec.get("EventPattern", "")
        if event_pattern:
            event_pattern = json.dumps(event_pattern, ensure_ascii=False)
        input_value = spec.get("Input", {})
        deleted = spec.get("deleted", False)
        target_id = f"{key}-target"
        self.utils.logging(f"{key} 권한을 추가하는 중입니다.")
        try:
            self.lambda_client.add_permission(
                FunctionName=function_name,
                StatementId=key + "_Statement",
                Action='lambda:InvokeFunction',
                Principal="*"
            )
        except:
            self.utils.logging(f"{key} 권한 추가를 패스했습니다.")
        else:
            self.utils.logging(f"{key} 권한 추가가 완료되었습니다.")
        if deleted:
            try:    
                self.utils.logging(f"{key} Target 을 삭제하는 중입니다.")
                result = []
                result.append(self.event_client.remove_targets(
                    Rule=key, Ids=[target_id]))
                self.utils.logging(f"{key} Target 삭제를 완료했습니다.")
                self.utils.logging(f"{key} Rule 을 삭제하는 중입니다.")
                result.append(self.event_client.delete_rule(Name=key))
                self.utils.logging(f"{key} Rule 삭제를 완료했습니다.")
            except self.event_client.exceptions.ResourceNotFoundException:
                self.utils.logging(f"{key} 를 찾지 못했으나 패스했습니다.")
        else:
            self.utils.logging(f"{key} Rule 을 작성하는 중입니다.")
            self.event_client.put_rule(
                Name=name_key, EventPattern=event_pattern, ScheduleExpression=schedule_expression, State='ENABLED', Description=description)
            self.utils.logging(f"{key} Rule 을 작성했습니다.")

            try:
                self.utils.logging(f"{key} LambdaFunction Arn 을 가져오는 중입니다.")
                finded_lambda = self.lambda_client.get_function(
                    FunctionName=function_name)
                lambda_arn = finded_lambda["Configuration"]["FunctionArn"]
                self.utils.logging(
                    f"{key} LambdaFunction Arn 을 가져왔습니다. {lambda_arn}")
            except:
                raise ValueError(f"invalid FunctionName [{function_name}]")

            self.utils.logging(f"{key} Target을 작성하는 중입니다.")

            result = self.event_client.put_targets(Rule=name_key, Targets=[{
                "Id": target_id,
                "Arn": lambda_arn,
                "Input": json.dumps(input_value, ensure_ascii=False)
            }])

            self.utils.logging(f"{key} Target을 작성했습니다.")

        return result

    def _deploy(self, template):
        kind = template["kind"]
        spec = self.template_handler.get_spec(template["name"])
        if kind == "cloudwatch":
            self.put_cloudwatch_event(spec)
        elif kind == "glue":
            self.put_glue(spec)
        else:
            raise ValueError(f"invalid kind {kind}")

    def deploy_all(self, no_cache):
        templates = None
        if no_cache:
            templates = self.origin_templates
        else:
            templates = self.filtered_templates

        for template in templates:
            self._deploy(template)

    def deploy_to_s3(self):
        # pprint(self.origin_templates)
        self.s3_client.put_object(Bucket=self.utils.info["bucket_name"],
                                  Body=json.dumps(self.origin_templates, ensure_ascii=False, default=str).encode("utf-8"), Key="default/deployer/scheduler/template.json", ACL="public-read", ContentType="application/json")
    def to_camel_case(self, snake_str):
        components = snake_str.split('_')
        return components[0] + ''.join(x.title() for x in components[1:])

    def auto_create(self):
        bucket_name = self.utils.info["bucket_name"]
        semi_replicator_glue_schedule = self.utils.info["semi_replicator_glue_schedule"]
        
        self.utils.logging("Semi Replicator 을 기준으로 템플릿 예제를 생성하는 중입니다.")
        input_data = self.template_handler.get_spec("cloudwatch-DirectorSemiReplicator")["Input"]
        db_uid = input_data["db_uid"]
        glues = []
        for target in input_data["targets"]:
            db_name, db_table_name, _ = target
            # SR Robot Presswire RobotEditorScenario
            camel_name = self.to_camel_case(f"SR_{db_uid}_{db_name}_{db_table_name}")
            glues.append({
                "kind": "glue",
                "name": f"glue-{camel_name}",
                "spec": {
                    "name": f"{camel_name}",
                    "S3TargetPath": f"s3://{bucket_name}/default/lake/discover/general/value/SemiReplicator/{db_uid}/{db_name}/{db_table_name}",
                    "Schedule": semi_replicator_glue_schedule
                }
            })
        
        # with open(f"{self.scheduler_templates_path}/auto-created.yaml", "w", encoding="utf-8") as fp:
        print(yaml.dump_all(glues, encoding="utf-8").decode("utf-8"))

        self.utils.logging("Semi Replicator 을 기준으로 템플릿 예제 생성을 완료했습니다.")

    def deploy(self):
        no_cache = args.no_cache
        self.deploy_all(no_cache)
        self.save_legacy_templates()
        self.deploy_to_s3()
