import json
from .schema import ClientConfig

def load_config(path="project_config/config.json") -> ClientConfig:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return ClientConfig(**data)


if __name__ == "__main__":
    config = load_config()
    print(config.dict())
