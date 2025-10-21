# dict_to_pg

## Архитектура

- **config.py** — содержит константы для конфигурирования задания.
- **service.py** — содержит класс для взаимодействия с БД и upsert записей.
- **create_db.py** — скрипт для создания локальной базы данных и предоставления прав пользователю.
- **helpers.py** — вспомогательные функции для сериализации, определения типа данных, генерации алиасов и т.д.
- **logger.py** — класс для логирования сообщений уровней INFO и ERROR.
- **dict_to_pg.py** — основной скрипт, выполняющий весь функционал по вставке/обновлению данных из словарей.

---

## Требования

1. Установлен PostgreSQL 17 локально
2. Установлен poetry `version = 0.1.0` для установки pcycopg2
3. Клиент `psql` должен быть доступен в PATH. Для Windows путь должен быть `C:\Program Files\PostgreSQL\17\bin\psql.exe`

---

## Установка и запуск

1. Установите [Poetry](https://python-poetry.org/) ( версия `0.1.0` для проекта).
2. Перейдите в директорию prompai-tasks

   ```bash
   cd prompai-tasks
   ```

3. Установите зависимости:
    ```bash
    poetry install --no-root
    ```
---

## Файл `.env` и `config.ini`

- Перейдите в директорию task1

   ```bash
   cd task1
   ```

- Создайте файл `.env` по примеру `.env.example`, добавьте вашти значения. Пользователь, пароль и название БД подхватятся из этого файла.
    ```.env
    PG_PASSWORD=SuperSecretPassword!
    PG_NAME=dict_to_pg
    PG_USER=my_pg_user
    ```
- Создайте файл `config.ini` в директории task1/:
   ```ini
   [postgres]
   host = localhost
   port = 5432
   dbname = $PG_NAME
   user = $PG_USER
   password = $PG_PASSWORD
   ```
---

## Локальная настройка базы данных PostgreSQL

Для запуска локальной базы данных используйте скрипт `create_db.py`

### Linux / macOS

Откройте терминал и выполните команду:

```bash
python3 create_db.py
```

### Windows

Откройте PowerShell или Командную строку (cmd) и выполните команду:

```bash
python create_db.py
```

---
## Запустите sample.json

```bash
poetry run python3 dict_to_pg.py --table users --input sample.json
```
