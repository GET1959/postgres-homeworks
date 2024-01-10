"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv

file = 'orders_data.csv'

conn = psycopg2.connect(host='localhost',
            port=5433,
            database='north',
            user='postgres',
            password='my_password')
try:
    with conn:
        with conn.cursor() as cur:
            with open('north_data/' + file) as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", row)
            cur.execute("SELECT * FROM orders")

            rows = cur.fetchall()
            print(rows)
            for row in rows:
                print(row)
finally:
    conn.close()