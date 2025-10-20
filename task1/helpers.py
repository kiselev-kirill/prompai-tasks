import os
from pathlib import Path

from config import ENV_FILE, INI_FILE
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




