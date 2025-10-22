# Создание класса исключения

--- 

## Подход к решению 

1. Использовал встроенную библиотеку `traceback` для извлечения всего стек трейса
2. Проходился по каждому трейсбеку во `FrameSummary`. Копировал словарь локальных переменных, удалял оттуда `err` (если есть) и проходился по нему для преобразования в `repr()` и проверял длину значения
--- 

## Требования

1. Установлен Python 3.11

--- 

## Тестовая функция и выходные данные в JSON

- **Функция**
```python3 
if __name__ == "__main__":

    # test функция
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
```

- **Выходные данные в формате JSON**

```json
    {
      "file": "C:\\Users\\KiselevKO\\PycharmProjectsTests\\test-tasks\\prompai-tasks\\task2\\custom_exception_handler.py",
      "line": 126,
      "function": "test_truncation",
      "code": "x = 1 / 0",
      "locals": {
        "big_list": "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 2...",
        "long_s": "'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx...",
        "var": "{'key': 'valuevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluev...}"
      }
    },
    {
      "file": "C:\\Users\\KiselevKO\\PycharmProjectsTests\\test-tasks\\prompai-tasks\\task2\\custom_exception_handler.py",
      "line": 131,
      "function": "foo",
      "code": "test_truncation()",
      "locals": {
        "long_set": "{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 2...}"
      }
    },
    {
      "file": "C:\\Users\\KiselevKO\\PycharmProjectsTests\\test-tasks\\prompai-tasks\\task2\\custom_exception_handler.py",
      "line": 138,
      "function": "outer",
      "code": "foo()",
      "locals": {
        "big_list_2": "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 2...]"
      }
    }
}
```
--- 

## Запуск
- Перейдите в директорию task2

```bash
cd task2
```
  
- Запустите скрипт
### Windows
```shell
python custom_exception_handler.py
```

### Linux / MacOS
```shell
python3 custom_exception_handler.py
```
