from configparser import ConfigParser
#import pathlib
import os
import importlib.resources

def get_api_key():
    config = ConfigParser()
#    config_file_path = os.path.join(
#        pathlib.Path(__file__).parent / "config.ini"
#    )
#    config.read(config_file_path)
    with importlib.resources.open_text('bilob.utils', 'config.ini') as f:
        config.read_file(f)
    return config["keys"]["api_key"], config["keys"]["api_secret"]

