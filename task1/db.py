import psycopg2

from logger import logger
from config import INI_FILE
from helpers import read_ini_config


class PostgresDB:
    def __init__(self, config_path: str = INI_FILE):
        cfg_kwargs = read_ini_config(config_path)
        self.conn = psycopg2.connect(**cfg_kwargs)
        self.conn.autocommit = False

    def close(self):
        self.conn.close()

