import logging


class DictToPgLogger:
    """
    Класс для логирования.
    Поддерживает уровни INFO и ERROR, кастомное форматирование.
    """
    def __init__(self, name: str | None = None, level: int = logging.INFO):
        """
        Инициализация логгера.

        :param name: имя логгера
        :param level: уровень логирования (по умолчанию INFO)
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setLevel(level)

            # Форматтер наших логов
            formatter = logging.Formatter(
                "[%(asctime)s] [%(name)s] %(levelname)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    @property
    def get_logger(self) -> logging.Logger:
        """Возвращает объект логгера."""
        return self.logger


# глобальный логгер
logger = DictToPgLogger("dict_to_pg").get_logger
