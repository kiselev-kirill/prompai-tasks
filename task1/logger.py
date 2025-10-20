import logging


class DictToPgLogger:
    """
    Класс для логирования.
    Поддерживает уровни INFO и ERROR, кастомное форматирование.
    """

    def __init__(self, name: str | None = None):
        """
        Инициализация логгера.

        :param name: имя логгера
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "[%(asctime)s] [%(name)s] %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def info(self, msg: str, *args, **kwargs):
        """Логировать информационное сообщение"""
        self.logger.info(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):
        """Логировать сообщение об ошибке"""
        self.logger.error(msg, *args, **kwargs)

    @property
    def get_logger(self) -> logging.Logger:
        """Возвращает объект логгера."""
        return self.logger


# глобальный логгер для задания 1
logger = DictToPgLogger("dict_to_pg")
