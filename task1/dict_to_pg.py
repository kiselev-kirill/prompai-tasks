import argparse
import json

import psycopg2

from service import PgJsonUpserter
from logger import logger


def main():
    parser = argparse.ArgumentParser(description="Сохраняет JSON в PostgreSQL")
    parser.add_argument("--table", required=True, help="Имя таблицы")
    parser.add_argument("--input", required=True, help="Путь к JSON файлу")
    args = parser.parse_args()

    loader: PgJsonUpserter = PgJsonUpserter()

    try:
        with open(args.input, mode="r", encoding="utf-8") as file:
            records = json.load(file)
    except Exception as err:
        logger.error(f"Ошибка при чтении JSON файла: <{err}>")
        return

    try:
        inserted, updated, added_columns = loader.upsert_records(args.table, records)
        loader.commit()
        logger.info(f"Вставлено: {inserted}, обновлено: {updated},"
                    f" добавлено колонок: {added_columns}")
    except psycopg2.Error as err:
        logger.error(f"Произошла ошибка: <{err}>")
    finally:
        loader.close()


if __name__ == "__main__":
    main()