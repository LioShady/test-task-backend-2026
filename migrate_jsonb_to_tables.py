"""Одноразовая миграция: abstract_customer (JSONB-массивы) -> отдельные таблицы.

Перед запуском сделать pg_dump.
Запуск:  DATABASE_URL=postgresql+psycopg2://user:pass@host/db python migrate_jsonb_to_tables.py

Всё выполняется одной транзакцией: при любой ошибке база остаётся как была.
Повторный запуск безопасен: если старых колонок уже нет, скрипт ничего не делает.

Что переносится:
  - amo_customers, amo_leads, amo_contacts, amo_companies -> отдельные таблицы;
  - id и send_methods у abstract_customer сохраняются как есть;
  - sequence abstract_customer.id сдвигается на max(id) после переноса;

Допущения:
  - abstract_customer уже существует в старой форме; новые колонки в ней
    не добавляются (если появится новая колонка в models.AbstractCustomer,
    её нужно добавить отдельным ALTER TABLE);
  - при дублях id внутри разных склеек (были баги слияния) детерминированно
    побеждает запись с меньшим abstract_customer_id (см. DISTINCT ON).
"""
import os

from flask import Flask
from sqlalchemy import inspect, text

from models import db


# --- вспомогательные выражения над элементом jsonb-массива -------------------

def g(field: str) -> str:
    """Поле из блока general элемента jsonb-массива (как text)."""
    return f"elem->'general'->>'{field}'"


ID = f"({g('id')})::bigint"
API_VALUE = "COALESCE(elem->'api_value', '{}'::jsonb)"


# старая JSONB-колонка -> (новая таблица, конфликтные колонки, колонки и выражения из elem)
ENTITIES = {
    "amo_customers": ("amo_customer", "id", {
        "id": ID,
        "login": g("login"),
        "name": g("name"),
        "billing_type": g("billing_type"),
        "system": g("system"),
        "host": g("host"),
        "is_robot": f"COALESCE(({g('is_robot')})::boolean, false)",
        "is_nds": f"COALESCE(({g('is_nds')})::boolean, false)",
    }),
    "amo_leads": ("amo_lead", "id", {
        "id": ID,
        "login": g("login"),
        "name": g("name"),
    }),
    "amo_contacts": ("amo_contact", "id", {
        "id": ID,
        "name": g("name"),
        "phone": g("phone"),     
    }),
    "amo_companies": ("amo_company", "id", {
        "id": ID,
        "name": g("name"),
        "inn": g("inn"),         
    }),
}


def _ensure_abstract_customer_columns(conn):
    """Добавляем в существующую abstract_customer колонки, которых может не быть."""
    cols = {c["name"] for c in inspect(conn).get_columns("abstract_customer")}
    if "created_at" not in cols:
        conn.execute(text(
            "ALTER TABLE abstract_customer "
            "ADD COLUMN created_at TIMESTAMP NOT NULL DEFAULT now()"
        ))
    if "updated_at" not in cols:
        conn.execute(text(
            "ALTER TABLE abstract_customer ADD COLUMN updated_at TIMESTAMP"
        ))


def _fill_abstract_customer_ids(conn, lock_timeout: str = "10s"):
    """Заполняем sequence abstract_customer.id по max(id), чтобы новые INSERT
    не конфликтовали с уже перенесёнными строками."""
    conn.execute(text(f"SET LOCAL lock_timeout = '{lock_timeout}'"))
    conn.execute(text(
        """
        SELECT setval(
            pg_get_serial_sequence('abstract_customer', 'id'),
            COALESCE((SELECT max(id) FROM abstract_customer), 0) + 1,
            false
        )
        """
    ))


def migrate(conn):
    # Пока идёт перенос, никто не должен писать в старую таблицу.
    # lock_timeout не даёт миграции висеть, если ночной прогон ещё идёт.
    conn.execute(text("SET LOCAL lock_timeout = '30s'"))
    conn.execute(text("LOCK TABLE abstract_customer IN ACCESS EXCLUSIVE MODE"))

    db.metadata.create_all(conn)  # создаст только отсутствующие (новые) таблицы
    _ensure_abstract_customer_columns(conn)

    for old_col, (table, conflict, cols) in ENTITIES.items():
        arr = (f"CASE WHEN jsonb_typeof(a.{old_col}) = 'array' "
               f"THEN a.{old_col} ELSE '[]'::jsonb END")

        total = conn.execute(text(
            f"SELECT COALESCE(sum(jsonb_array_length({arr})), 0) FROM abstract_customer a"
        )).scalar()

        names = ", ".join(["abstract_customer_id", "api_value", *cols])
        exprs = ", ".join(["src.abstract_customer_id", "src.api_value", *[
            f"src.{name}" for name in cols
        ]])

        # DISTINCT ON (id) ORDER BY id, abstract_customer_id -- детерминированно
        # оставляем запись с меньшим abstract_customer_id при дублях.
        inserted = conn.execute(text(f"""
            INSERT INTO {table} ({names})
            SELECT {exprs}
            FROM (
                SELECT DISTINCT ON (elem_id)
                       a.id            AS abstract_customer_id,
                       {API_VALUE}     AS api_value,
                       elem_id         AS id,
                       {", ".join(f"{expr} AS {name}" for name, expr in cols.items() if name != "id")}
                FROM abstract_customer a
                CROSS JOIN LATERAL jsonb_array_elements({arr}) AS e(elem)
                CROSS JOIN LATERAL (SELECT {ID} AS elem_id) AS x
                WHERE {ID} IS NOT NULL
                ORDER BY elem_id, a.id
            ) AS src
            ON CONFLICT ({conflict}) DO NOTHING
        """)).rowcount

        print(f"{table}: в JSON {total}, перенесено {inserted}, "
              f"дублей/битых пропущено {total - inserted}")

    drops = ", ".join(f"DROP COLUMN {col}" for col in ENTITIES)
    conn.execute(text(f"ALTER TABLE abstract_customer {drops}"))

    _fill_abstract_customer_ids(conn)


def main():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = os.environ["DATABASE_URL"]
    db.init_app(app)

    with app.app_context(), db.engine.begin() as conn:
        insp = inspect(conn)
        if not insp.has_table("abstract_customer"):
            db.metadata.create_all(conn)
            print("Таблицы не было: создана новая схема, переносить нечего")
            return
        old_cols = {c["name"] for c in insp.get_columns("abstract_customer")}
        if not old_cols & set(ENTITIES):
            print("Уже мигрировано, ничего не делаю")
            return
        migrate(conn)
        print("Готово")


if __name__ == "__main__":
    main()
