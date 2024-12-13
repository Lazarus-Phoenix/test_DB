import psycopg2
import sys
from config import config

class PostgresChecker:
    def __init__(self):
        self.conn = None

    def connect(self):
        try:
            params = config()
            self.conn = psycopg2.connect(
                dbname=params['dbname'],
                user=params['user'],
                password=params['password'],
                host=params['host'],
                port=params['port']
            )
            print("Подключение к PostgreSQL успешно установлено.")
        except psycopg2.Error as e:
            print(f"Ошибка при подключении к PostgreSQL: {e}")
            sys.exit(1)

    def reset_table(self):
        try:
            cur = self.conn.cursor()
            cur.execute("DROP TABLE IF EXISTS test_table CASCADE;")
            cur.execute("""
                CREATE TABLE test_table (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50),
                    age INTEGER
                );
            """)
            self.conn.commit()
            print("Таблица test_table была удалена и пересоздана.")
        except psycopg2.Error as e:
            print(f"Ошибка при очистке таблицы: {e}")

    def create_table(self):
        try:
            cur = self.conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50),
                    age INTEGER
                );
            """)
            self.conn.commit()
            print("Таблица test_table создана или уже существует.")
        except psycopg2.Error as e:
            print(f"Ошибка при создании таблицы: {e}")

    def insert_data(self):
        try:
            cur = self.conn.cursor()
            cur.execute("INSERT INTO test_table (name, age) VALUES (%s, %s);", ("John Doe", 30))
            cur.execute("INSERT INTO test_table (name, age) VALUES (%s, %s);", ("Jane Smith", 25))
            self.conn.commit()
            print("Данные успешно вставлены в таблицу.")
        except psycopg2.Error as e:
            print(f"Ошибка при вставке данных: {e}")

    def query_data(self):
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT * FROM test_table ORDER BY id DESC LIMIT 5;")
            rows = cur.fetchall()
            print("\nПоследние 5 записей из таблицы:")
            for row in rows:
                print(row)
        except psycopg2.Error as e:
            print(f"Ошибка при выполнении запроса: {e}")

    def close_connection(self):
        if self.conn:
            self.conn.close()
            print("Соединение с PostgreSQL закрыто.")

def main():
    checker = PostgresChecker()

    # Подключение к базе данных
    checker.connect()

    # Очистка и создание таблицы
    checker.reset_table()

    # Вставка данных
    checker.insert_data()

    # Запрос данных
    checker.query_data()

    # Закрытие соединения
    checker.close_connection()

if __name__ == "__main__":
    main()