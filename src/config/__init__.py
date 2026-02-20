import configparser
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

def read_config():
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the full path to the config file
    filename = os.path.join(script_dir, 'config.ini')
    config = configparser.ConfigParser()
    config.read(filename)
    print(config.sections())
    return config

if __name__ == '__main__':
    read_config()