import great_expectations as gx
from great_expectations.data_context.types.base import DataContextConfig
from ruamel.yaml import YAML

yaml = YAML()
with open("/home/jayaprakashn/LondonBicycles/great_expectations/tmp_ge.yml") as f:
    config = yaml.load(f)
try:
    DataContextConfig.from_commented_map(commented_map=config)
    print("Config is valid for this GE version.")
except Exception as e:
    print("Config validation failed:", e)

import great_expectations
print(great_expectations.__version__)
print(great_expectations.__file__)
