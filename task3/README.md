# Fan-out и fan-in

--- 

## Подход к решению

- Создаются несколько процессов (воркеров) для проверки чисел на простоту через `executor.submit()`
- Результаты собираются в основном процессе через `as_completed()` 
- Корректная остановка по SIGINT/SIGTERM через глобальный флаг `stop`
- Логирование старта, прогресса и завершения работы с подсчётом пропускной способности  
- Таймаут `future.result(timeout=2)` предотвращает зависание процессов 

Особенности:  
- Использовал `ProcessPoolExecutor` вместо `multiprocessing` для простоты
- Частично писал код с помощью ChatGPT, так как очень редко работал с `ProcessPoolExecutor`/`multiprocessing` но концепт понимал.  
- В будущем можно применить `InterpreterPoolExecutor` (Python 3.14) [статья на Хабре](https://habr.com/ru/articles/957058/), это меньше нагрузки на процессор и эффективнее

--- 

## Требования

1. Установлен Python 3.11

---

## Запуск

- Перейдите в директорию task3

```bash
cd task3
```
  
- Запустите скрипт
### Windows
```shell
python worker_pool.py --workers 4 --limit 1000
```

### Linux / MacOS
```shell
python3 worker_pool.py --workers 4 --limit 1000
```
