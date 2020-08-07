import base64
import uuid
import time
from pprint import pprint
import os
import yaml
import tools
import server.settings


class Template():

    def __init__(self):
        self.general = tools.general.General()
        setting = server.settings.Settings()
        # environ = setting.get_envrion()
        self.layer_template_schema = setting.get_layer("template_schema")
        self.template_schemas = setting.get("template_schemas")
        self.process_schemas = setting.get("process_schemas")
        self.all_kind_info = {}
        self._all_items = {}
        

    def update(self, name):
        self.valid_exist_template(name)
        
        return self.load(self.templates[name]["path"], update=True)

    def get_items(self, kind):
        items = {}
        for name in self._all_items[kind]:
            items[name] = self.get_item(kind, name)

        return items


    def load(self, path, update):
        with open(path, "r", encoding="utf-8") as fp:

            loaded_template = yaml.full_load_all(fp.read())

            for index, raw_template in enumerate(loaded_template):
                

                self.general.get_validated_obj(
                    raw_template, self.layer_template_schema)

                kind = raw_template["kind"]
                name = raw_template["name"]
                meta = raw_template["meta"]
                random_name = False
                
                if name == "random":
                    name = base64.b64encode((path + str(index)).encode()).decode()
                    random_name = True

                category = raw_template["category"]
                tags = raw_template["tags"]


                if name in self.templates and update == False:
                    raise ValueError(f"name already exists. name is {name}")

                if kind == "":
                    raise ValueError("kind is empty.")

                if name == "":
                    raise ValueError("name is empty.")

                if category == "":
                    raise ValueError("category is empty.")

                if not isinstance(meta, dict):
                    raise ValueError(f"meta is not a dict. meta is {meta}")

                # spec validate
                raw_template["spec"] = self.general.get_validated_obj(raw_template["spec"], self.template_schemas[raw_template["kind"]])

                self.templates[name] = {
                    "kind": kind,
                    "name": name,
                    "category": category,
                    "tags": tags,
                    "template": raw_template,
                    "path": path,
                    "meta": meta,
                    "random_name": random_name
                }


            

                
        # return self.templates[name]

    def get_item(self, kind, name):
        return self._all_items[kind][name]["value"]

    def get(self, name):
        # print(self.templates)
        return self.templates[name]["template"]

    def init(self, start_dir, regex=r".+\.yaml", kind_settings=[]):
        self._loads(start_dir, regex)

        for piece in kind_settings:
            kind = piece["kind"]
            worker = piece["worker"]
            parser = piece["parser"]
            options = piece.get("options", {})
            self._register(kind, worker, parser, options)
        if len(kind_settings) > 0:
            self._load_items()


    def _loads(self, start_dir, regex=r".+\.yaml"):
        self.templates = {}
        for path in self.general.find_all_by_name(start_dir, regex):
            self.load(path, update=False)
            
        return self.templates

    def find_by_name(self, name):
        self.valid_exist_template(name)
        return self.templates[name]

    def get_abspath_in_template(self, template_name, path):
        self.valid_exist_template(template_name)

        if os.path.isabs(path):
            return path
        if path[:2] == "./":
            path = path[2:]

        result = "%s/%s" % (os.path.dirname(
            self.templates[template_name]["path"]), path)

        return result
        
    def find(self, kind="", category=None, tags=[], meta={}):
        result = []

        for name in self.templates:

            piece = self.templates[name]

            if kind != "": 
                if kind != piece["kind"]:
                    continue

            if isinstance(category, str):
                if category != piece["category"]:
                    continue
            
            if len(meta) != 0:
                if not self.general.is_obj_looking_for(piece["meta"], meta):
                    continue

            if len(tags) != 0:
                if not self.general.is_array_looking_for(piece["tags"], tags):
                    continue

            result.append(piece)

        return result

    def get_spec(self, name):
        self.valid_exist_template(name)
        return self.templates[name]["template"]["spec"]

    def valid_exist_template(self, name):
        # for name in self.templates:
        #     print(name)
        if name not in self.templates:
            raise ValueError(
                "name is not exists in templates. name is [%s]" % name)

    def get_kind(self, name):
        self.valid_exist_template(name)

        return self.templates[name]["template"]["kind"]

    def _parse(self, kind, name):
        self.valid_exist_template(name)
        # template = self.templates[name]["template"]
        value = self._get_kind_info(kind)["parser"](name)

        return value

    def _load_items(self):

        for name in self.templates:
            piece = self.templates[name]
            kind = piece["kind"]
            name = piece["name"]

            if self._is_load_separately(kind):
                continue

            self._load_item(kind, name)


    def load_item(self, name):
        
        kind = self.get_kind(name)
        self._load_item(kind, name)


    def _load_item(self, kind, name):
        if self.is_loaded(kind, name):
            if self._is_load_once(kind) and self._all_items[kind][name]:
                return
        if self._get_kind_info(kind)["parser"] == None:
            return
        value = self._parse(kind, name)
        if kind not in self._all_items:
            self._all_items[kind] = {}
        
        self._all_items[kind][name] = self._make_item(kind, name, value)

    def is_loaded(self, kind, name):
        return kind in self._all_items and name in self._all_items[kind]

    def _make_item(self, kind, name, value):
        return {
            "kind": kind,
            "name": name,
            "value": value
        }

    def _register(self, kind, worker, parser, options):
        self._set_kind_info(kind, self._make_kind_info(kind, worker, parser, options))


    def process_by_kind(self, kind, name, args={}):
        self._load_item(kind, name)

        args["template_name"] = name
        args = self.general.get_validated_obj(args, self.process_schemas[kind])
        kind_info = self._get_kind_info(kind)
        worker = kind_info["worker"]

        if not worker:
            raise ValueError(f"{name}'s worker is not exsists.")

        return worker(args)


    def _is_load_once(self, kind):
        kind_info = self._get_kind_info(kind)
        options = kind_info["options"]
        if options.get("load_once", False):
            return True
        return False

    def _is_load_separately(self, kind):
        kind_info = self._get_kind_info(kind)
        options = kind_info["options"]
        if options.get("load_separately", False):
            return True
        return False

    def _make_kind_info(self, kind, worker, parser, options={}):
        return {
            "kind": kind,
            "worker": worker,
            "parser": parser,
            "options": options
        }

    def _set_kind_info(self, kind, kind_info):
        self.all_kind_info[kind] = kind_info

    def _get_kind_info(self, kind):
        return self.all_kind_info[kind]
