from typing import Any

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from logger import logger
from config import INI_FILE
from constants import SET_UUID_ID
from helpers import (
    read_ini_config,
    serialize_value,
    determine_type,
    generate_alias
)


class PostgresDB:

    def __init__(self, config_path: str = INI_FILE):
        cfg_kwargs = read_ini_config(config_path)
        self.conn = psycopg2.connect(**cfg_kwargs)
        self.conn.autocommit = False

    def close(self):
        self.conn.close()

    def alter_column(self, table: str, column: str, col_type: str):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name=%s AND column_name=%s
            """, (table, column))
            if cur.fetchone() is None:
                logger.info(f"Добавляю колонку {column} в таблицу {table}")
                cur.execute(
                    query=sql.SQL("ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {col_type}")
                    .format(
                        table=sql.Identifier(table),
                        column=sql.Identifier(column),
                        col_type=sql.SQL(col_type)
                    )
                )
                self.conn.commit()

    def ensure_alias_mapping(
            self, table: str, original_key: str, alias: str
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS column_aliases (
                    table_name TEXT NOT NULL,
                    original_key TEXT NOT NULL,
                    alias TEXT NOT NULL,
                    PRIMARY KEY(table_name, original_key)
                )
            """)
            cur.execute("""
                INSERT INTO column_aliases(table_name, original_key, alias)
                VALUES (%s,%s,%s)
                ON CONFLICT(table_name, original_key) DO NOTHING
            """, (table, original_key, alias))
            self.conn.commit()

    def generate_key_alias_mapping(
            self, records: list[dict[str, Any]]
    ) -> tuple[dict, dict, set[str]]:
        """
        Генерирует алиасы для всех ключей в записях и определяет тип данных для каждого поля.

        Для каждого уникального ключа в списке записей:
        - создаётся алиас (в случае слишком длинного имени);
        - определяется пример значения для выбора типа колонки;
        - формируются два словаря:
          1. key_to_alias — оригинальный ключ и алиас;
          2. alias_to_type — определённый тип данных для каждой колонки (по алиасу).

        :param records: Список словарей, где каждый словарь представляет собой запись для вставки.
        :return: Кортеж из трёх элементов:
                 (key_to_alias: dict[str, str],
                  alias_to_type: dict[str, str],
                  all_keys: set[str]) — множество всех оригинальных ключей.
        """
        all_keys = set().union(*[r.keys() for r in records])
        key_to_alias = {}
        alias_to_type = {}

        for key in all_keys:
            alias = generate_alias(key)
            key_to_alias[key] = alias
            sample_val = next((r[key] for r in records if key in r), None)
            alias_to_type[alias] = determine_type(sample_val)

        return key_to_alias, alias_to_type, all_keys

    def prepare_values(
            self, records: list[dict[str, Any]], all_keys: set, key_to_alias: dict
    ) -> tuple[list[str], list[list[Any]]]:
        """
        Подготавливает значения записей к вставке в базу данных.

        Значения сериализуются,
        а ключи заменяются на их алиасы для формирования правильных SQL-колонок.

        :param records: Список записей (словарей) для вставки.
        :param all_keys: Множество всех ключей, встречающихся в записях.
        :param key_to_alias: Оригинальный ключ и алиас;
        :return: Кортеж из двух элементов:
                 (added_cols: list[str] — список колонок по алиасам,
                  values_list: list[list[Any]] — список списков со значениями).
        """
        added_cols = [key_to_alias[key] for key in all_keys]
        values_list = [
            [serialize_value(record.get(key)) for key in all_keys]
            for record in records
        ]
        return added_cols, values_list

    def get_upsert_key(self, all_keys: set, key_to_alias: dict) -> str | None:
        """
        Определяет ключ для операции UPSERT (ON CONFLICT).

        Проверяет наличие естественных ключей 'id' или 'uuid' в наборе all_keys
        и возвращает соответствующий алиас. Если ключ не найден — возвращает None.

        :param all_keys: Множество всех оригинальных ключей в записях.
        :param key_to_alias: Словарь соответствия оригинальных ключей и алиасов.
        :return: Алиас ключа для UPSERT, либо None, если подходящий ключ не найден.
        """
        for key in SET_UUID_ID:
            if key in all_keys:
                return key_to_alias[key]
        return None

    def ensure_table_exists(self, table: str, records: list[dict[str, Any]]):
        """
        Создает таблицу, если её нет, с ключом id или uuid в зависимости от данных.

        :param table: имя таблицы
        :param records: список словарей, по которому можно определить первичный ключ
        """
        pk_column = None
        for key in ("id", "uuid"):
            if any(key in record for record in records):
                pk_column = key
                break

        if pk_column is None:
            pk_column = "id"

        col_type = "SERIAL" if pk_column == "id" else "UUID"

        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {table} (
                    {pk} {pk_type} PRIMARY KEY
                )
            """).format(
                table=sql.Identifier(table),
                pk=sql.Identifier(pk_column),
                pk_type=sql.SQL(col_type)
            ))
            self.conn.commit()

    def upsert_records(self, table: str, records: list[dict[str, Any]]):
        inserted, updated, added_columns = 0, 0, 0
        if not records:
            return inserted, updated, added_columns

        self.ensure_table_exists(table, records)

        key_to_alias, alias_to_type, all_keys = self.generate_key_alias_mapping(records)
        for key, alias in key_to_alias.items():
            col_type = alias_to_type[alias]
            try:
                self.alter_column(table, alias, col_type)
                added_columns += 1
            except Exception as err:
                logger.error(f"Ошибка при добавлении колонки {alias}: {err}")

            if alias != key:
                self.ensure_alias_mapping(table, key, alias)

        # Подготовить значения для вставки
        added_cols, values_list = self.prepare_values(records, all_keys, key_to_alias)
        upsert_key = self.get_upsert_key(all_keys, key_to_alias)

        with self.conn.cursor() as cur:
            cols_identifiers = [sql.Identifier(col) for col in added_cols]
            table_sql = sql.Identifier(table)
            if upsert_key:
                upsert_col = sql.Identifier(upsert_key)
                set_expr = sql.SQL(', ').join(
                    sql.SQL("{}=EXCLUDED.{}").format(col, col)
                    for col in cols_identifiers if col.string != upsert_key
                )
                query = sql.SQL(
                    "INSERT INTO {table} ({fields}) VALUES %s ON CONFLICT ({upsert})"
                    " DO UPDATE SET {set_expr}"
                ).format(
                    table=table_sql,
                    fields=sql.SQL(', ').join(cols_identifiers),
                    upsert=upsert_col,
                    set_expr=set_expr
                )
            else:
                query = sql.SQL(
                    "INSERT INTO {table} ({fields}) VALUES %s"
                ).format(
                    table=table_sql,
                    fields=sql.SQL(', ').join(cols_identifiers)
                )

            execute_values(cur, query, values_list)
            self.conn.commit()

        return inserted, updated, added_columns
