import sqlite3
import time
import random
import datetime

# Константы
DB_NAME = "test_db.sqlite"
TABLE_NAME = "data_table"
NUM_RECORDS = 1500  # Количество записей для вставки
NUM_QUERIES = 200 # 200 # 400 # 600 # 800 # 1000 # 1200   # Количество запросов для тестирования

# Функция для генерации случайных данных
def generate_data():
    return {
        "name": f"User_{random.randint(1, 1000)}",
        "age": random.randint(18, 65),
        "city": random.choice(["Moscow", "London", "New York", "Tokyo"]),
        "date_joined": datetime.date(2020, 1, 1) + datetime.timedelta(days=random.randint(0, 1000)),
        "score": random.uniform(0, 100),
    }

# ООП подход

class DatabaseHandler:
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = sqlite3.connect(self.db_name)
            self.cursor = self.conn.cursor()
        except sqlite3.Error as e:
            print(f"Ошибка подключения к базе данных: {e}")
            exit()

    def disconnect(self):
        if self.conn:
            self.cursor.close()
            self.conn.close()

    def create_table(self, table_name):
        try:
            self.connect()
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    age INTEGER,
                    city TEXT,
                    date_joined DATE,
                    score REAL
                )
            """)
            self.conn.commit()
            self.disconnect()
        except sqlite3.Error as e:
            print(f"Ошибка создания таблицы: {e}")

    def insert_data(self, table_name, data):
        try:
            self.connect()
            self.cursor.execute(f"""
                INSERT INTO {table_name} (name, age, city, date_joined, score)
                VALUES (?, ?, ?, ?, ?)
            """, (data["name"], data["age"], data["city"], data["date_joined"], data["score"]))
            self.conn.commit()
            self.disconnect()
        except sqlite3.Error as e:
            print(f"Ошибка вставки данных: {e}")

    def execute_query(self, query):
        try:
            self.connect()
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            self.disconnect()
            return result
        except sqlite3.Error as e:
            print(f"Ошибка выполнения запроса: {e}")
            return None

def oop_example():
    db_handler = DatabaseHandler(DB_NAME)

    # Создание таблицы
    db_handler.create_table(TABLE_NAME)

    # Вставка данных
    start_time = time.time()
    for _ in range(NUM_RECORDS):
        data = generate_data()
        db_handler.insert_data(TABLE_NAME, data)
    end_time = time.time()

    # Выполнение запросов
    start_time = time.time()
    for _ in range(NUM_QUERIES):
        age = random.randint(20, 60)
        query = f"SELECT AVG(score) FROM {TABLE_NAME} WHERE age = {age}"
        db_handler.execute_query(query)
    end_time = time.time()
    print(f"ООП: Время исполнения {NUM_QUERIES} запросов: {end_time - start_time:.4f} сек")


# Дата-ориентированный подход

def create_connection(db_name):
    conn = None
    try:
        conn = sqlite3.connect(db_name)
        return conn
    except sqlite3.Error as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return None

def create_table(conn, table_name):
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                age INTEGER,
                city TEXT,
                date_joined DATE,
                score REAL
            )
        """)
        conn.commit()
    except sqlite3.Error as e:
        print(f"Ошибка создания таблицы: {e}")

def insert_data(conn, table_name, data):
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            INSERT INTO {table_name} (name, age, city, date_joined, score)
            VALUES (?, ?, ?, ?, ?)
        """, (data["name"], data["age"], data["city"], data["date_joined"], data["score"]))
        conn.commit()
    except sqlite3.Error as e:
        print(f"Ошибка вставки данных: {e}")

def execute_query(conn, query):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except sqlite3.Error as e:
        print(f"Ошибка выполнения запроса: {e}")
        return None


def data_oriented_example():
    conn = create_connection(DB_NAME)
    if not conn:
        return

    # Создание таблицы
    create_table(conn, TABLE_NAME)

    # Вставка данных
    start_time = time.time()
    for _ in range(NUM_RECORDS):
        data = generate_data()
        insert_data(conn, TABLE_NAME, data)
    end_time = time.time()

    # Выполнение запросов
    start_time = time.time()
    for _ in range(NUM_QUERIES):
        age = random.randint(20, 60)
        query = f"SELECT AVG(score) FROM {TABLE_NAME} WHERE age = {age}"
        execute_query(conn, query)
    end_time = time.time()
    print(f"ДОП: Время исполнения {NUM_QUERIES} запросов: {end_time - start_time:.4f} сек")

    conn.close()

# Запуск

if __name__ == "__main__":
    oop_example()
    data_oriented_example()
