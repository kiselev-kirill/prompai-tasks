import argparse
import json

from db import PostgresDB
from logger import logger


def main(db: PostgresDB = PostgresDB()):
    parser = argparse.ArgumentParser(description="Сохраняет JSON в PostgreSQL")
    parser.add_argument("--table", required=True, help="Имя таблицы")
    parser.add_argument("--input", required=True, help="Путь к JSON файлу")
    args = parser.parse_args()

    try:
        with open(args.input, mode="r", encoding="utf-8") as file:
            records = json.load(file)
    except Exception as err:
        logger.error(f"Ошибка при чтении JSON: {err}")
        return

    try:
        inserted, updated, added_columns = db.upsert_records(args.table, records)
        logger.info(f"Вставлено: {inserted}, обновлено: {updated},"
                    f" добавлено колонок: {added_columns}")
    finally:
        db.close()


if __name__ == "__main__":
    main()