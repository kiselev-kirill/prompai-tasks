import traceback
import types
import json

MAX_REPR_LEN = 80  # максимальная длина строки при выводе локальных переменных


class ExceptionReport:
    """
    Класс для извлечения полной информации об исключении.

    Атрибуты:
        error_name (str): Тип исключения и сообщение.
        module_name (str): Имя модуля, где возникла ошибка.
        stack_trace (list[dict]): Полный стек-трейс с локальными переменными.
    """

    __slots__ = ("_error_name", "_module_name", "_stack_trace")  # Допустимые атрибуты

    def __init__(self, exc: BaseException):
        """
        Инициализация нашего класса.

        - Задаем имя error_name, module_name
        - Извлекаем стек-трейс

        :param exc: BaseException базовый объект исключения
        """
        self._error_name = f"{exc.__class__.__name__}: {exc}"
        tb = exc.__traceback__
        if tb is not None:

            tb_frame = tb.tb_frame
            self._module_name = tb_frame.f_globals.get("__name__", "__main__")

        self._stack_trace = self._extract_stack(tb=tb)

    @property
    def error_name(self) -> str:
        return self._error_name

    @error_name.setter
    def error_name(self, value):
        raise ValueError("Cannot set error_name")

    @property
    def module_name(self) -> str:
        return self._module_name

    @module_name.setter
    def module_name(self, value):
        raise ValueError("Cannot set module_name")

    @property
    def stack_trace(self) -> list[dict]:
        return self._stack_trace

    @stack_trace.setter
    def stack_trace(self, value):
        raise ValueError("Cannot set stack_trace")

    def _extract_stack(self, tb: types.TracebackType) -> list[dict]:
        """
        Извлекает стек трейс с локальными переменными.

        - Каждый кадр превращается в словарь с ключами file, line, function, code, locals.
        - Локальные переменные приводятся к строкам с усечением длинных значений.

        :param tb: traceback объекта исключения
        :return: список словарей с информацией о кадрах стека
        """
        stack_trace = []
        frame_summary = traceback.extract_tb(tb)
        current_tb = tb

        for frame in frame_summary:
            locals_filtered = {}
            if current_tb is not None:
                locals_dict_copy = current_tb.tb_frame.f_locals.copy()
                locals_dict_copy.pop("err", None)

                for key, value in locals_dict_copy.items():
                    repr_value = repr(value)
                    if len(repr_value) > MAX_REPR_LEN:
                        locals_filtered[key] = repr_value[:MAX_REPR_LEN] + "..."
                    else:
                        locals_filtered[key] = repr_value
                current_tb = current_tb.tb_next

            stack_trace.append({
                "file": frame.filename,
                "line": frame.lineno,
                "function": frame.name,
                "code": frame.line,
                "locals": locals_filtered
            })

        stack_trace.reverse()
        return stack_trace

    def to_json(self, **kwargs) -> str:
        """
        Сериализует отчет об исключении в JSON.

        - Включает error_name, module_name и stack_trace.

        :param kwargs: параметры для json.dumps
        :return: JSON-строка с информацией об исключении
        """
        json_report = {
            "error_name": self.error_name,
            "module_name": self.module_name,
            "stack_trace": self.stack_trace,
        }
        return json.dumps(json_report, indent=2, **kwargs)


if __name__ == "__main__":

    # test функция, где исключение срабатывает
    def test_truncation():
        big_list = list(range(1000))
        long_s = "x" * 500
        var = {"key": "value" * 50}
        x = 1 / 0


    def foo():
        long_set = set(range(1000))
        test_truncation()


    def outer():
        big_list_2 = list(range(1000))

        try:
            foo()
        except BaseException as err:
            report = ExceptionReport(err)
            print(report.to_json())
    outer()
