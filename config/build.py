import yaml
CONFIG_PATH = 'config/examples/example.yaml'
import os
class Config:
    def __init__(self, yaml_file):
        self.HOST = yaml_file['HOST']
        self.PORT = yaml_file["PORT"]
        self.KAFKA_PORT = yaml_file["KAFKA_PORT"]
        self.API_VERSION =  yaml_file["API_VERSION"]
        self.STATE_TABLE = yaml_file["STATE_TABLE"]
        self.RESULT_TABLE = yaml_file["RESULT_TABLE"]
        self.SHEMA_NAME = yaml_file["SHEMA_NAME"]
        self.FRAMES_TOPIC = yaml_file["FRAMES_TOPIC"]
        self.DB_PASSWORD = yaml_file["DB_PASSWORD"]
        self.DB_USER = yaml_file["DB_USER"]
        self.DB_NAME = yaml_file["DB_NAME"]
        self.MODEL_PATH = yaml_file["MODEL_PATH"]
        self.STATES =  {
            "PROCESSING" : "processing_video",
            "INFER": "video_inferencing",
            "COMPLETE": "finished",
            "STOP": "stopped"
        }

async def get_config(yaml_file_path):
    with open(yaml_file_path) as f:
        try:
            yaml_file = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            print("Invalid path to config yaml")
        config = Config(yaml_file)
        return config

# if __name__ == "__main__":
#     #test, что config считывается успешно
#     config = get_config('config/examples/example.yaml')
#     print(vars(config))
