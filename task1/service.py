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


class PgJsonUpserter:
    """
    Класс для динамической вставки и обновления (UPSERT) JSON-записей в postgres.
        Автоматически создаёт таблицы и колонки по структуре входных данных,
        определяет типы колонок и выполняет безопасные UPSERT-операции по ключам id или uuid
    """

    def __init__(self, config_path: str = INI_FILE):
        cfg_kwargs = read_ini_config(config_path)
        self.conn = psycopg2.connect(**cfg_kwargs)
        self.conn.autocommit = False

    def close(self):
        self.conn.close()

    def commit(self):
        self.conn.commit()

    def alter_column(self, table: str, column: str, col_type: str):
        """
        Добавляет новую колонку в таблицу, если она отсутствует.

        Проверяет наличие колонки в information_schema.columns и,
        при её отсутствии, выполняет команду ALTER TABLE

        :param table: имя таблицы
        :param column: имя добавляемой колонки
        :param col_type: тип данных новой колонки
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name=%s AND column_name=%s
            """, (table, column))
            cur.execute(
                query=sql.SQL("ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {col_type}")
                .format(
                    table=sql.Identifier(table),
                    column=sql.Identifier(column),
                    col_type=sql.SQL(col_type)
                )
            )
            logger.info(f"Добавляю колонку (если не существует)"
                        f" {column} в таблицу {table} с типом {col_type}")

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
            logger.info(f"Создаю таблицу column_aliases (если не существует)")
            cur.execute("""
                INSERT INTO column_aliases(table_name, original_key, alias)
                VALUES (%s,%s,%s)
                ON CONFLICT(table_name, original_key) DO NOTHING
            """, (table, original_key, alias))
            logger.info(f"Вставляю в таблицу column_aliases значения {original_key, alias}")

    @staticmethod
    def generate_key_alias_mapping(
            records: list[dict[str, Any]]
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
        all_keys = {key for record in records for key in record}  # уникальные ключи
        key_to_alias = {}
        alias_to_type = {}

        for key in all_keys:
            alias = generate_alias(key)
            key_to_alias[key] = alias

            # next нужен, чтобы сразу получить значение, а не генератор
            val = next(record[key] for record in records if key in record)
            alias_to_type[alias] = determine_type(val)

        return key_to_alias, alias_to_type, all_keys

    @staticmethod
    def prepare_values(
            records: list[dict[str, Any]], all_keys: set,
            key_to_alias: dict[str, str]
    ) -> tuple[list[str], list[list[Any]]]:
        """
        Подготавливает значения записей к вставке в базу данных
        Значения сериализуются

        :param records: Список словарей.
        :param all_keys: Множество всех оригинальных ключей в записях.
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

    @staticmethod
    def get_upsert_key(all_keys: set, key_to_alias: dict) -> str | None:
        """
        Определяет ключ для операции UPSERT (ON CONFLICT).

        Проверяет наличие естественных ключей 'id' или 'uuid' в наборе all_keys
        и возвращает соответствующий алиас. Если ключ не найден — возвращает None.

        :param all_keys: Множество всех оригинальных ключей в записях.
        :param key_to_alias: Словарь оригинальных ключей и алиасов.
        :return: alias ключа для UPSERT, либо None
        """
        for key in SET_UUID_ID:
            if key in all_keys:
                return key_to_alias[key]

    @staticmethod
    def filter_values(
            cols: list[str], values_list: list[list], upsert_key: str
    ) -> tuple[list[list], list[list]]:
        """
        Разделяет список значений строк на два списка в зависимости
        от наличия значения ключа upsert.

        :param cols: Список имен колонок, соответствующих значениям в каждой строке.
        :param values_list: Список значений строк (каждая строка — это список значений).
        :param upsert_key: Имя колонки, которая является первичным ключом или ключом для upsert.
        :return: Кортеж из двух списков:
                 - filtered_with_id: список со значениями с upsert_key
                 - filtered_no_id: список со значениями без upsert_key
        """
        upsert_key_idx = cols.index(upsert_key)  # получаем индекс нашего upsert_key
        filtered_with_id = []
        filtered_no_id = []

        for row in values_list:
            if row[upsert_key_idx] is None:
                # добавляем только значения (без значения upsert_key)
                # индексы в values и в cols одинаковые, поэтому можем так фильтровать
                filtered_no_id.append([val for i, val in enumerate(row)
                                       if i != upsert_key_idx])
            else:
                filtered_with_id.append(row)
        return filtered_with_id, filtered_no_id

    def ensure_table_exists(
            self, table: str, records: list[dict[str, Any]]
    ) -> None:
        """
        Проверяет наличие таблицы в базе данных и создаёт её при отсутствии.

        Выбирает тип первичного ключа (PK) в зависимости от входных данных:
        - Если в записях присутствует одно из полей из множества SET_UUID_ID (например, "uuid"),
          создаётся колонка с типом UUID и значением по умолчанию `gen_random_uuid()`.
        - В противном случае используется автоинкрементный первичный ключ `id SERIAL`.

        :param table: Имя таблицы, которую требуется проверить или создать.
        :param records: Список словарей, по которым определяется,
                        использовать ли UUID или SERIAL в качестве первичного ключа.
        :return: None
        """
        pk_column = "id"  # Default значение
        for key in SET_UUID_ID:
            if any(key in record for record in records):
                pk_column = key.lower()
                break

        if pk_column == "id":
            pk_def = (sql.SQL("{pk} SERIAL PRIMARY KEY")
                      .format(pk=sql.Identifier(pk_column)))
        else:
            pk_def = (sql.SQL("{pk} UUID PRIMARY KEY DEFAULT gen_random_uuid()")
                      .format(pk=sql.Identifier(pk_column)))

        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {table} (
                    {pk_def}
                )
            """).format(
                table=sql.Identifier(table),
                pk_def=pk_def
            ))
            logger.info(f"Создаю таблицу {table} c {pk_column} (если не существует)")

    def count_columns(self, table: str) -> int:
        """
        Возвращает количество колонок в указанной таблице.

        Использует системную таблицу information_schema.columns
        для подсчёта всех колонок по имени таблицы.

        :param table: имя таблицы
        :return: количество колонок
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) 
                FROM information_schema.columns 
                WHERE table_name = %s
            """, (table,))
            return cur.fetchone()[0]

    def upsert_records(
            self, table: str, records: list[dict[str, Any]]
    ) -> tuple[int, int, int]:
        """
        Вставляет или обновляет записи в указанной таблице.

        Функция выполняет следующие действия:
        1. Если таблица не существует, создаёт её автоматически
        2. Проверяет и добавляет отсутствующие колонки или изменяет их типы при необходимости
        3. Разделяет записи на две группы:
           - с существующим первичным ключом (upsert)
           - без первичного ключа (Postgres сгенерирует значение ключа автоматически)
        4. Выполняет вставку или обновление записей.
        5. Обновляет последовательность для первичного ключа (id)

        :param table: Имя таблицы для вставки/обновления записей.
        :param records: Список словарей, представляющих записи для вставки или обновления.
        :return: Кортеж из трёх целых чисел:
            - inserted: количество вставленных записей (без первичного ключа)
            - updated: количество обновлённых записей (с существующим первичным ключом)
            - count_columns_after - columns_count_before: количество добавленных или изменённых колонок
        """
        inserted, updated, columns_count_before = 0, 0, self.count_columns(table)
        if not records:
            return inserted, updated, columns_count_before

        # создаем таблицу, если не существует
        self.ensure_table_exists(table, records)

        # получаем словари (key: alias, alias: type) и множество всех оригинальных ключей
        key_to_alias, alias_to_type, all_keys = self.generate_key_alias_mapping(records)
        for key, alias in key_to_alias.items():
            col_type = alias_to_type[alias]
            self.alter_column(table, alias, col_type)

            if alias != key:
                self.ensure_alias_mapping(table, key, alias)

        # Подготовим значения для вставки
        added_cols, values_list = self.prepare_values(records, all_keys, key_to_alias)

        # Смотрим, какой у нас upsert key
        upsert_key = self.get_upsert_key(all_keys, key_to_alias)

        # Получаем два отфильтрованных списка (с upsert key и без)
        filtered_with_id, filtered_no_id = self.filter_values(added_cols, values_list, upsert_key)
        with self.conn.cursor() as cur:
            table_sql = sql.Identifier(table)
            upsert_col = sql.Identifier(upsert_key)
            if filtered_with_id:
                cols_identifiers_with_id = [sql.Identifier(col) for col in added_cols]
                set_expr = sql.SQL(', ').join(
                    sql.SQL("{}=EXCLUDED.{}").format(col, col)
                    for col in cols_identifiers_with_id if col.string != upsert_key
                )
                query = sql.SQL(
                    "INSERT INTO {table} ({fields}) VALUES %s ON CONFLICT ({upsert})"
                    " DO UPDATE SET {set_expr}"
                    " RETURNING {upsert}"
                ).format(
                    table=table_sql,
                    fields=sql.SQL(', ').join(cols_identifiers_with_id),
                    upsert=upsert_col,
                    set_expr=set_expr,
                )
                updated_ids = execute_values(cur, query, filtered_with_id, fetch=True)
                logger.info(f"Добавляю записи с UUID/ID,"
                            f" если вставляем существующие, то обновлю их")

                if upsert_key.lower() == "id":
                    update_sequence_query = sql.SQL("""
                        SELECT setval(
                            pg_get_serial_sequence({table_name}, {id_name}),
                            COALESCE((SELECT MAX({id_col}) FROM {table_name_for_max}), 0)
                        )
                    """).format(
                        table_name=sql.Literal(table),
                        id_name=sql.Literal(upsert_key),
                        id_col=upsert_col,
                        table_name_for_max=table_sql
                    )
                    cur.execute(update_sequence_query)

                updated += len(updated_ids)

            if filtered_no_id:
                cols_identifiers_no_id = [
                    sql.Identifier(col) for col in added_cols if col not in SET_UUID_ID
                ]
                query = sql.SQL(
                    "INSERT INTO {table} ({fields}) VALUES %s"
                    "RETURNING {upsert_col}"
                ).format(
                    table=table_sql,
                    fields=sql.SQL(', ').join(cols_identifiers_no_id),
                    upsert_col=upsert_col
                )
                inserted_ids = execute_values(cur, query, filtered_no_id, fetch=True)
                logger.info(f"Добавляю новые записи, создавая новые ID/UUID")
                inserted += len(inserted_ids)

        count_columns_after = self.count_columns(table)
        return inserted, updated, count_columns_after - columns_count_before
