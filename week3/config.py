import os
import platform
import configparser

HOME_DIR = os.environ["USERPROFILE"] if platform.system() == "Windows" else os.environ["HOME"]
INPUT_DIR = os.path.join(HOME_DIR, "PycharmProjects", "dhs622", "week3", "input")
OUTPUT_DIR = os.path.join(HOME_DIR, "PycharmProjects", "dhs622", "week3", "output")
try:
    assert os.path.exists(OUTPUT_DIR)
except:
    raise Exception("please create a directory where you will store data, "
                    "and edit the OUTPUT_DIR line in config.py accordingly")

config_file_full_path = os.path.join(HOME_DIR, "dhs622_config.cfg")
config = configparser.ConfigParser()
config.read(config_file_full_path)

app_name = config["telegram-credentials-1"]["app-name"]
api_id = config["telegram-credentials-1"]["api-id"]
api_hash = config["telegram-credentials-1"]["api-hash"]

