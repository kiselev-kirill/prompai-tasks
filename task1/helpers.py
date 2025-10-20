import os
from configparser import ConfigParser
from pathlib import Path

from config import ENV_FILE, INI_FILE
from constants import INI_POSTGRES_NAME
from logger import logger


def load_env_variables(path: Path = ENV_FILE) -> None:
    """
    Загружает переменные окружения из файла `.env`.

    Используется как альтернатива внешним библиотекам (например, python-dotenv),
    поскольку в рамках задания запрещено их использование.

    :param path: путь к файлу `.env`, из которого нужно считать переменные окружения
    """
    if not path.exists():
        logger.error("Invalid BASE_DIR")
        raise ValueError("Incorrect Path")
    with path.open(mode="r") as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith("#"):  # пустую строку или комментарий не учитываем
                continue
            env_name, env_value = line.split("=")  # получаем наши env переменные
            os.environ[env_name] = env_value  # сетим их в окружении


def read_ini_config(path: str = INI_FILE) -> dict[str, str]:
    """
    Считывает конфигурацию PostgreSQL из INI-файла и подставляет значения из переменных окружения.

    Поведение:
    - Сохраняет регистр ключей.
    - Если значение в INI начинается с `$`, заменяет его на соответствующую
      переменную окружения (например, $DB_NAME -> os.environ["DB_NAME"]).
    - Возвращает словарь с параметрами подключения к PostgreSQL.

    :param path: путь к INI-файлу (по умолчанию INI_FILE)
    :return: словарь с параметрами подключения к PostgreSQL
    """
    load_env_variables()
    env_variables = os.environ.copy()

    parser = ConfigParser()
    parser.read(path)

    ini_config = dict(parser[INI_POSTGRES_NAME])
    for ini_key, ini_value in ini_config.items():
        if ini_value.startswith("$"):
            _, value = ini_value.split("$")
            ini_config[ini_key] = env_variables[value]
    return ini_config


