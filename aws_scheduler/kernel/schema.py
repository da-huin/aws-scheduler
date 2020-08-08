
def get_glue_schema():
    return {
        "schema": {
            "type": "object",
            "properties": {
                "name": {},
                "Schedule": {},
                "S3TargetPath": {},
                "Description": {}
            },
            "required": [
                "name",
                "Schedule",
                "S3TargetPath"
            ]
        },
        "properties": {
            "Description": {
                "default": ""
            }
        }
    }


def get_cloudwatch_schema():
    return {
        "schema": {
            "type": "object",
                    "properties": {
                        "Description": {},
                        "name": {},
                        "Schedule": {},
                        "Input": {},
                        "EventPattern": {}
                    },
            "required": [
                        "name"
                    ]
        },
        "properties": {
            "Description": {
                "default": ""
            },
            "Input": {
                "default": {}
            }
        }
    }
