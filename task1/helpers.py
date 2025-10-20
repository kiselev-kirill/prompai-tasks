import hashlib
import json
import os
import uuid
from configparser import ConfigParser
from datetime import datetime
from pathlib import Path
from typing import Any

from config import ENV_FILE, INI_FILE
from constants import INI_POSTGRES_NAME, MAX_COLUMN_BYTES_NAME_LEN
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
        return
    with path.open(mode="r", encoding="utf-8") as file:
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


def generate_alias(key: str) -> str:
    """
    Генерирует алиас для ключа, если его длина превышает MAX_COLUMN_NAME_LEN символов.

    Для длинных ключей создается уникальный хеш из первых 8 символов SHA1 и возвращается
    в формате "alias_<hash>". Короткие ключи возвращаются без изменений.

    :param key: исходный ключ словаря
    :return: алиас для ключа, подходящий для использования в PostgreSQL
    """
    bytes_key = key.encode()
    if len(bytes_key) <= MAX_COLUMN_BYTES_NAME_LEN:
        return key
    hashed = hashlib.sha1(bytes_key).hexdigest()[:8]
    return f"alias_{hashed}"


def determine_type(value: Any) -> str:
    """
    Определяет соответствующий тип данных PostgreSQL для переданного значения.

    Типы:
    - None -> TEXT
    - bool -> BOOLEAN
    - int -> BIGINT
    - float -> DOUBLE PRECISION
    - str -> TIMESTAMPTZ (если ISO 8601) или TEXT
    - dict/list -> JSONB
    - остальные значения -> TEXT

    :param value: любое значение словаря
    :return: строка с именем типа PostgreSQL
    """
    match value:
        case None:
            return "TEXT"
        case bool():
            return "BOOLEAN"
        case int():
            return "BIGINT"
        case float():
            return "DOUBLE PRECISION"
        case str():
            try:
                datetime.fromisoformat(value.replace("Z", "+00:00"))
                return "TIMESTAMPTZ"
            except ValueError:
                return "TEXT"
        case dict() | list():
            return "JSONB"
        case uuid.UUID():
            return "UUID"
        case _:
            return "TEXT"


def serialize_value(value: Any) -> Any:
    """
    Сериализует сложные типы данных в JSON для сохранения в PostgreSQL.

    - dict и list преобразуются в JSON-строку.
    - Остальные значения возвращаются без изменений.

    :param value: любое значение словаря
    :return: сериализованное значение для PostgreSQL
    """
    match value:
        case dict() | list():
            return json.dumps(value)
        case uuid.UUID():
            return str(value)
        case _:
            return value
