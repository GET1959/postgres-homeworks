import json
from typing import Any

import psycopg2
import psycopg2.extras
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from config import config


def main():
    script_file = "fill_db.sql"
    json_file = "suppliers.json"
    json_file_1 = "suppliers_1.json"
    db_name = "my_new_db"

    params = config()
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({"dbname": db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur, json_file)
                print("Таблица suppliers успешно создана")

                # suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, json_file)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur, json_file_1)
                print(f"FOREIGN KEY успешно добавлены")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params: Any, db_name: str) -> None:
    """Создает новую базу данных."""
    try:
        connection = psycopg2.connect(**params)
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with connection.cursor() as cursor:
            sql_create_database = f"create database {db_name}"
            cursor.execute(sql_create_database)
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection is not None:
            connection.close()
            print("Соединение с PostgreSQL закрыто")


def execute_sql_script(cur: Any, script_file: Any) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    with open(script_file, "r") as file:
        sqlFile = file.read()
        try:
            cur.execute(sqlFile)
        except (Exception, Error) as error:
            print("Command skipped: ", error)


def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и создает новый файл
    suppliers_1.json, содержащий столбец supplier_id."""
    with open(json_file) as f:
        sup_list = json.load(f)
    sup_id_list = [{"supplier_id": i + 1} | sup_list[i] for i in range(len(sup_list))]
    with open("suppliers_1.json", "w", encoding="utf-8") as file:
        json.dump(sup_id_list, file, ensure_ascii=False, indent=2)
    return sup_id_list


def get_columns_to_query(json_file: str) -> str:
    """Извлекает из json файла строку колонок для запроса SQL"""
    columns_to_query = (
        " varchar, ".join(list(get_suppliers_data(json_file)[0].keys())) + " text[])"
    )
    return columns_to_query


def get_data_to_query(json_file: str) -> Any:
    data = get_suppliers_data(json_file)
    return json.dumps(data)


def create_suppliers_table(cur: Any, json_file: str) -> None:
    """Создает таблицу suppliers."""
    columns = get_columns_to_query(json_file)
    try:
        cur.execute("CREATE TABLE suppliers(" + columns)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def insert_suppliers_data(cur: Any, json_file: str) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    data = get_data_to_query(json_file)

    query_sql = """INSERT INTO suppliers
    SELECT * FROM json_populate_recordset(NULL::suppliers, %s);
    """
    query_sup_id = """
    ALTER TABLE suppliers ALTER COLUMN supplier_id TYPE integer USING (supplier_id::integer);
    ALTER TABLE suppliers ADD CONSTRAINT pk_supplier PRIMARY KEY (supplier_id);
    """
    try:
        cur.execute(query_sql, (data,))
        cur.execute(query_sup_id)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def add_foreign_keys(cur: Any, json_file_1: str) -> None:
    """Пересоздает таблицу products в соответсвии кодировкой suppliers,
    восстанавливает связи навой таблицы с базой данных
    и добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    products_query = """SELECT * FROM products;"""
    drop_products_query = """DROP TABLE products CASCADE;"""
    columns = """(product_id int, product_name varchar(50), category_id int,
            quantity_per_unit varchar(50), unit_price real, units_in_stock int,
            units_on_order int, reorder_level int, discontinued int, supplier_id int)"""
    fill_in_query = """
        INSERT INTO products SELECT * FROM json_populate_recordset(NULL::products, %s);
        """
    query_pk_prod = (
        """ALTER TABLE products ADD CONSTRAINT pk_products PRIMARY KEY (product_id);"""
    )
    query_fk_prod_sup = """ALTER TABLE products ADD CONSTRAINT fk_suppliers_products FOREIGN KEY(supplier_id)
        REFERENCES suppliers(supplier_id);"""
    query_fk_prod_cat = """ALTER TABLE products ADD CONSTRAINT fk_categories_products FOREIGN KEY(category_id)
        REFERENCES categories(category_id);"""
    query_fk_order_details = """ALTER TABLE order_details ADD CONSTRAINT fk_products_orderdetails
        FOREIGN KEY(product_id) REFERENCES products(product_id);"""
    with open(json_file_1, encoding="utf-8") as f:
        sup_list = json.load(f)
    try:
        cur.execute(products_query)
        results = cur.fetchall()
        products_list = [dict(row) for row in results]
        print(products_list)
        with open("products.json", "w", encoding="windows-1252") as file:
            json.dump(products_list, file, ensure_ascii=False, indent=2)
        with open("products.json", encoding="utf-8") as f:
            prod_list = json.load(f)
        for product in prod_list:
            for supplier in sup_list:
                if product["product_name"] in supplier["products"]:
                    product["supplier_id"] = supplier["supplier_id"]
        with open("products_1.json", "w", encoding="utf-8") as file:
            json.dump(prod_list, file, ensure_ascii=False, indent=2)
        with open("products_1.json", encoding="utf-8") as file:
            data = file.read()
        cur.execute(drop_products_query)
        cur.execute("CREATE TABLE products" + columns)
        cur.execute(fill_in_query, (data,))
        cur.execute(query_pk_prod)
        cur.execute(query_fk_prod_sup)
        cur.execute(query_fk_prod_cat)
        cur.execute(query_fk_order_details)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


if __name__ == "__main__":
    main()
