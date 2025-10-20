import os
import subprocess

from helpers import load_env_variables
from logger import logger


def main():
    """
    Создает локальную базу данных PostgreSQL и пользователя с правами доступа.

    Действия:
    1. Загружает переменные окружения через load_env_variables().
    2. Создает базу данных PG_NAME.
    4. Выдает все привилегии новому пользователю.

    :raises subprocess.CalledProcessError: при ошибке выполнения SQL через psql
    """
    load_env_variables()

    db_name = os.getenv("PG_NAME")
    admin_user = "postgres"
    db_host = os.getenv("PG_HOST", "localhost")
    db_user = os.getenv("PG_USER")
    db_password = os.getenv("PG_PASSWORD")
    copy_env = os.environ.copy()

    sql_statements = [
        f"CREATE USER {db_user} WITH PASSWORD '{db_password}';",
        f"CREATE DATABASE {db_name} OWNER {db_user};",
        f"ALTER SCHEMA public OWNER TO {db_user};",
        f"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {db_user};",
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {db_user};",
        f"GRANT CREATE, USAGE ON SCHEMA public TO {db_user};"
    ]
    for statement in sql_statements:
        cmd = [
            "psql",
            "-U", admin_user,
            "-h", db_host,
            "-c", statement
        ]
        try:
            subprocess.run(cmd, check=True, env=copy_env)
        except subprocess.CalledProcessError as err:
            logger.error(f"Ошибка при исполнении sql скрипта <{statement}>: {err}")


if __name__ == "__main__":
    main()
