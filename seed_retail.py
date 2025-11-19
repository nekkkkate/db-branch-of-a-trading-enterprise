import os
import time
import random
from typing import List, Dict, Any, Generator
from datetime import datetime, timedelta
from faker import Faker
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

DBS_TO_SEED = [db.strip() for db in os.getenv("DBS_TO_SEED", "filial_west,filial_east").split(",") if db.strip()]

SEED_COUNT = int(os.getenv("SEED_COUNT", "1000"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))

APP_ENV = os.getenv("APP_ENV", "dev")
if APP_ENV != "dev":
    print("Сидирование отключено (APP_ENV != 'dev')")
    exit(0)

fake = Faker('ru_RU')

CATEGORIES = [
    "Электроника и техника",
    "Одежда и обувь",
    "Продукты питания",
    "Книги и канцтовары",
    "Спорт и отдых",
    "Дом и сад",
    "Красота и здоровье",
    "Автотовары",
    "Детские товары",
    "Мебель",
    "Строительство и ремонт",
    "Зоотовары"
]

PRODUCTS_BY_CATEGORY = {
    "Электроника и техника": [
        ("Смартфон", "Samsung Galaxy", 35000, 85000),
        ("Смартфон", "iPhone", 45000, 120000),
        ("Смартфон", "Xiaomi", 15000, 45000),
        ("Ноутбук", "Lenovo ThinkPad", 45000, 150000),
        ("Ноутбук", "MacBook", 80000, 250000),
        ("Планшет", "iPad", 25000, 80000),
        ("Наушники", "AirPods", 8000, 25000),
        ("Телевизор", "Samsung", 25000, 150000),
        ("Микроволновка", "LG", 8000, 25000),
        ("Холодильник", "Bosch", 35000, 120000)
    ],
    "Одежда и обувь": [
        ("Куртка", "зимняя", 3000, 15000),
        ("Джинсы", "классические", 1500, 8000),
        ("Кроссовки", "Nike", 5000, 15000),
        ("Кроссовки", "Adidas", 4000, 12000),
        ("Платье", "летнее", 1200, 5000),
        ("Рубашка", "деловая", 1000, 4000),
        ("Ботинки", "кожаные", 3000, 12000),
        ("Свитер", "шерстяной", 2000, 8000),
        ("Сумка", "женская", 1500, 10000),
        ("Шарф", "кашемировый", 1000, 5000)
    ],
    "Продукты питания": [
        ("Молоко", "3.2%", 60, 120),
        ("Хлеб", "черный", 40, 80),
        ("Мясо", "говядина", 450, 800),
        ("Рыба", "семга", 600, 1200),
        ("Сыр", "российский", 300, 600),
        ("Масло", "сливочное", 150, 300),
        ("Яйца", "куриные С1", 80, 150),
        ("Рис", "круглозерный", 80, 200),
        ("Кофе", "растворимый", 200, 800),
        ("Шоколад", "молочный", 100, 300)
    ],
    "Книги и канцтовары": [
        ("Книга", "художественная", 300, 1500),
        ("Учебник", "школьный", 500, 2000),
        ("Тетрадь", "общая", 30, 100),
        ("Ручка", "шариковая", 20, 150),
        ("Карандаш", "простой", 15, 50),
        ("Блокнот", "А5", 100, 500),
        ("Календарь", "настольный", 200, 800),
        ("Папка", "файловая", 50, 300),
        ("Степлер", "офисный", 300, 1000),
        ("Клей", "ПВА", 50, 200)
    ],
    "Спорт и отдых": [
        ("Мяч", "футбольный", 800, 3000),
        ("Гантели", "разборные", 2000, 8000),
        ("Коврик", "для йоги", 500, 2000),
        ("Велосипед", "горный", 15000, 80000),
        ("Лыжи", "беговые", 5000, 25000),
        ("Палатка", "туристическая", 3000, 15000),
        ("Рюкзак", "походный", 2000, 8000),
        ("Термос", "1л", 800, 3000),
        ("Удочка", "спиннинг", 1500, 8000),
        ("Ракетка", "теннисная", 2000, 10000)
    ],
    "Дом и сад": [
        ("Лампочка", "LED", 100, 500),
        ("Розетка", "евро", 150, 600),
        ("Выключатель", "одинарный", 200, 800),
        ("Краска", "акриловая", 300, 1500),
        ("Кисть", "малярная", 100, 500),
        ("Лейка", "садовая", 300, 1200),
        ("Семена", "томаты", 50, 200),
        ("Горшок", "цветочный", 100, 800),
        ("Лопата", "штыковая", 800, 3000),
        ("Шланг", "поливочный", 500, 2500)
    ],
    "Красота и здоровье": [
        ("Шампунь", "для волос", 200, 800),
        ("Крем", "для лица", 300, 2000),
        ("Зубная паста", "отбеливающая", 100, 400),
        ("Витамины", "комплекс", 400, 1500),
        ("Мыло", "жидкое", 80, 300),
        ("Дезодорант", "спрей", 150, 500),
        ("Помада", "матовая", 300, 1200),
        ("Тушь", "для ресниц", 400, 2000),
        ("Духи", "женские", 1500, 8000),
        ("Лосьон", "для тела", 250, 1000)
    ],
    "Автотовары": [
        ("Масло", "моторное", 800, 3000),
        ("Фильтр", "воздушный", 300, 1200),
        ("Свечи", "зажигания", 200, 800),
        ("Коврики", "салона", 1000, 4000),
        ("Чехлы", "на сиденья", 2000, 8000),
        ("Антифриз", "зимний", 400, 1500),
        ("Щетки", "стеклоочистителя", 300, 1200),
        ("Лампа", "фары", 150, 600),
        ("Аккумулятор", "60Ah", 3000, 8000),
        ("Шины", "летние", 3000, 15000)
    ],
    "Детские товары": [
        ("Подгузники", "размер 3", 800, 2000),
        ("Игрушка", "конструктор", 500, 5000),
        ("Соска", "силиконовая", 100, 400),
        ("Бутылочка", "для кормления", 200, 800),
        ("Коляска", "прогулочная", 8000, 35000),
        ("Автокресло", "детское", 5000, 25000),
        ("Одежда", "боди", 300, 1200),
        ("Обувь", "первые шаги", 800, 3000),
        ("Книжка", "с картинками", 150, 600),
        ("Пазл", "детский", 200, 1000)
    ],
    "Мебель": [
        ("Стул", "офисный", 3000, 15000),
        ("Стол", "письменный", 5000, 25000),
        ("Шкаф", "платяной", 8000, 50000),
        ("Кровать", "двуспальная", 10000, 80000),
        ("Диван", "двухместный", 15000, 100000),
        ("Комод", "4 ящика", 4000, 20000),
        ("Полка", "книжная", 2000, 8000),
        ("Табурет", "барный", 1500, 6000),
        ("Тумба", "прикроватная", 2000, 10000),
        ("Зеркало", "настенное", 1000, 8000)
    ],
    "Строительство и ремонт": [
        ("Дрель", "ударная", 2000, 15000),
        ("Отвертка", "крестовая", 100, 500),
        ("Молоток", "слесарный", 300, 1200),
        ("Гвозди", "строительные", 50, 300),
        ("Саморезы", "по дереву", 80, 400),
        ("Плитка", "керамическая", 200, 1500),
        ("Обои", "виниловые", 500, 3000),
        ("Клей", "для плитки", 200, 800),
        ("Шпатель", "широкий", 150, 600),
        ("Уровень", "строительный", 500, 2500)
    ],
    "Зоотовары": [
        ("Корм", "для собак", 300, 2000),
        ("Корм", "для кошек", 200, 1500),
        ("Игрушка", "для собак", 150, 800),
        ("Миска", "керамическая", 200, 1000),
        ("Поводок", "кожаный", 400, 2000),
        ("Ошейник", "с именной биркой", 300, 1500),
        ("Лоток", "для кошек", 300, 1200),
        ("Наполнитель", "древесный", 150, 600),
        ("Шампунь", "для животных", 200, 800),
        ("Переноска", "пластиковая", 1000, 5000)
    ]
}


def make_engine(database: str):
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{database}"
    return create_engine(url, pool_pre_ping=True)


def wait_for_db(engine, max_retries=12, retry_delay=3):
    for i in range(max_retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            print(f"[{engine.url.database}] Ожидание БД... (попытка {i + 1}/{max_retries}) -> {e}")
            time.sleep(retry_delay)
    raise Exception(f"Не удалось подключиться к базе {engine.url.database} после {max_retries} попыток")


def table_exists(engine, table_name: str) -> bool:
    try:
        with engine.connect() as conn:
            exists = conn.execute(
                text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name = :table_name
                )
                """),
                {"table_name": table_name.lower()}
            ).scalar()
            return bool(exists)
    except Exception as e:
        print(f"[{engine.url.database}] Ошибка при проверке таблицы {table_name}: {e}")
        return False


def batch_insert(engine, table: str, data: List[Dict[str, Any]], batch_size: int = BATCH_SIZE) -> int:
    if not data:
        return 0
    if not table_exists(engine, table):
        print(f"[{engine.url.database}] Таблица {table} не существует, пропускаем вставку")
        return 0

    inserted = 0
    with engine.begin() as conn:
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            try:
                stmt = text(f"""
                    INSERT INTO {table} ({', '.join(batch[0].keys())})
                    VALUES ({', '.join(':' + k for k in batch[0].keys())})
                    ON CONFLICT DO NOTHING
                """)
                result = conn.execute(stmt, batch)
                if result.rowcount is not None and result.rowcount >= 0:
                    inserted += result.rowcount
                else:
                    inserted += len(batch)
            except Exception as e:
                print(f"[{engine.url.database}] Ошибка при вставке в {table}: {e}")
                continue
    return inserted


def batch_insert_generator(engine, table: str, data_generator: Generator, batch_size: int = BATCH_SIZE) -> int:
    if not table_exists(engine, table):
        print(f"[{engine.url.database}] Таблица {table} не существует, пропускаем вставку")
        return 0

    inserted = 0
    batch = []

    with engine.begin() as conn:
        for item in data_generator:
            batch.append(item)

            if len(batch) >= batch_size:
                try:
                    if batch:
                        stmt = text(f"""
                            INSERT INTO {table} ({', '.join(batch[0].keys())})
                            VALUES ({', '.join(':' + k for k in batch[0].keys())})
                            ON CONFLICT DO NOTHING
                        """)
                        result = conn.execute(stmt, batch)
                        if result.rowcount is not None and result.rowcount >= 0:
                            inserted += result.rowcount
                        else:
                            inserted += len(batch)
                except Exception as e:
                    print(f"[{engine.url.database}] Ошибка при вставке в {table}: {e}")
                batch = []

        if batch:
            try:
                stmt = text(f"""
                    INSERT INTO {table} ({', '.join(batch[0].keys())})
                    VALUES ({', '.join(':' + k for k in batch[0].keys())})
                    ON CONFLICT DO NOTHING
                """)
                result = conn.execute(stmt, batch)
                if result.rowcount is not None and result.rowcount >= 0:
                    inserted += result.rowcount
                else:
                    inserted += len(batch)
            except Exception as e:
                print(f"[{engine.url.database}] Ошибка при вставке в {table}: {e}")

    return inserted


def table_count(engine, table_name: str) -> int:
    if not table_exists(engine, table_name):
        return 0
    with engine.connect() as conn:
        try:
            return conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar() or 0
        except Exception as e:
            print(f"[{engine.url.database}] Ошибка подсчета {table_name}: {e}")
            return 0


def generate_realistic_customers(count: int) -> Generator[Dict[str, str], None, None]:
    for _ in range(count):
        if random.choice([True, False]):
            name = fake.name_male()
        else:
            name = fake.name_female()

        yield {'customer_name': name}


def generate_realistic_products(count: int) -> Generator[Dict[str, Any], None, None]:
    used_skus = set()

    for i in range(count):
        category = random.choice(list(PRODUCTS_BY_CATEGORY.keys()))
        product_type, brand, min_price, max_price = random.choice(PRODUCTS_BY_CATEGORY[category])

        while True:
            sku = fake.ean13()
            if sku not in used_skus:
                used_skus.add(sku)
                break

        product_name = f"{product_type} {brand}"

        if random.random() < 0.3:
            extra = random.choice(['Premium', 'Eco', 'Pro', 'Lite', 'Max', 'Mini'])
            product_name = f"{product_name} {extra}"

        price = round(random.uniform(min_price, max_price), 2)

        yield {
            'sku': sku,
            'product_name': product_name,
            'price': price
        }


def seed_database(engine):
    dbname = engine.url.database
    print("=" * 60)
    print(f"[{dbname}] Начало сидирования реалистичными данными")
    print("=" * 60)

    if table_exists(engine, "category"):
        categories = [{'category_name': cat} for cat in CATEGORIES]
        added = batch_insert(engine, "category", categories)
        print(f"[{dbname}] Category: добавлено {added}, всего={table_count(engine, 'category')}")
    else:
        print(f"[{dbname}] category отсутствует — пропуск")

    if table_exists(engine, "customer"):
        customer_count = min(SEED_COUNT * 2, 5000)
        added = batch_insert_generator(engine, "customer",
                                       generate_realistic_customers(customer_count))
        print(f"[{dbname}] Customer: добавлено ~{added}, всего={table_count(engine, 'customer')}")
    else:
        print(f"[{dbname}] customer отсутствует — пропуск")

    if table_exists(engine, "product"):
        product_count = min(SEED_COUNT * 3, 10000)
        added = batch_insert_generator(engine, "product",
                                       generate_realistic_products(product_count))
        print(f"[{dbname}] Product: добавлено ~{added}, всего={table_count(engine, 'product')}")
    else:
        print(f"[{dbname}] product отсутствует — пропуск")

    if table_exists(engine, "product_category") and table_exists(engine, "product") and table_exists(engine,
                                                                                                     "category"):
        with engine.connect() as conn:
            products = conn.execute(text("SELECT id, product_name FROM product")).fetchall()
            categories = {row[1]: row[0] for row in
                          conn.execute(text("SELECT id, category_name FROM category")).fetchall()}

        pc_data = []
        for prod_id, prod_name in products:
            assigned_category = None
            for category_name in categories.keys():
                for product_type, brand, _, _ in PRODUCTS_BY_CATEGORY.get(category_name, []):
                    if product_type.lower() in prod_name.lower():
                        assigned_category = categories[category_name]
                        break
                if assigned_category:
                    break

            if not assigned_category:
                assigned_category = random.choice(list(categories.values()))

            pc_data.append({'product_id': prod_id, 'category_id': assigned_category})

            if random.random() < 0.1:
                second_category = random.choice(list(categories.values()))
                if second_category != assigned_category:
                    pc_data.append({'product_id': prod_id, 'category_id': second_category})

        added = batch_insert(engine, "product_category", pc_data)
        print(f"[{dbname}] Product_Category: добавлено ~{added}, всего={table_count(engine, 'product_category')}")
    else:
        print(f"[{dbname}] product_category/product/category отсутствуют — пропуск связей")

    if table_exists(engine, "sale") and table_exists(engine, "sale_item") and table_exists(engine,
                                                                                           "customer") and table_exists(
            engine, "product"):
        with engine.connect() as conn:
            customers = [r[0] for r in conn.execute(text("SELECT id FROM customer")).fetchall()]
            products = [(r[0], float(r[1]), r[2]) for r in
                        conn.execute(text("SELECT id, price, product_name FROM product")).fetchall()]

        if not customers or not products:
            print(f"[{dbname}] Нет customers или products для создания продаж — пропуск")
        else:
            sales_count = min(SEED_COUNT, 3000)

            popular_keywords = ['смартфон', 'молоко', 'хлеб', 'кофе', 'мыло', 'шампунь']
            product_weights = []
            for prod_id, price, name in products:
                weight = 1.0
                for keyword in popular_keywords:
                    if keyword.lower() in name.lower():
                        weight *= 3.0
                        break
                if price < 500:
                    weight *= 2.0
                elif price > 10000:
                    weight *= 0.3
                product_weights.append(weight)

            sales = []
            for _ in range(sales_count):
                customer = random.choice(customers)

                sale_date = datetime.now() - timedelta(days=random.randint(0, 365))
                if sale_date.weekday() in [5, 6]:
                    if random.random() < 0.3:
                        continue

                sales.append({'customer_id': customer, 'sale_date': sale_date})

            sale_ids = []
            with engine.begin() as conn:
                for s in sales:
                    res = conn.execute(
                        text("""
                            INSERT INTO sale (customer_id, sale_date)
                            VALUES (:customer_id, :sale_date)
                            ON CONFLICT DO NOTHING
                            RETURNING id
                        """),
                        s
                    )
                    if res.returns_rows:
                        row = res.fetchone()
                        if row:
                            sale_ids.append(row[0])

            if not sale_ids:
                with engine.connect() as conn:
                    sale_ids = [r[0] for r in conn.execute(
                        text("SELECT id FROM sale ORDER BY random() LIMIT :l"),
                        {'l': sales_count}
                    ).fetchall()]

            sale_items = []
            for sale_id in sale_ids:
                if random.random() < 0.7:
                    num_items = random.randint(1, 3)
                elif random.random() < 0.9:
                    num_items = random.randint(4, 7)
                else:
                    num_items = random.randint(8, 15)

                chosen_products = random.choices(
                    products,
                    weights=product_weights,
                    k=min(num_items, len(products))
                )

                unique_products = {}
                for prod_id, price, name in chosen_products:
                    if prod_id not in unique_products:
                        unique_products[prod_id] = (price, name)

                for prod_id, (price, name) in unique_products.items():
                    if any(keyword in name.lower() for keyword in ['молоко', 'хлеб', 'яйца']):
                        quantity = random.randint(1, 3)
                    elif price < 100:
                        quantity = random.randint(1, 10)
                    elif price > 10000:
                        quantity = 1
                    else:
                        quantity = random.randint(1, 5)

                    unit_price = round(price * random.uniform(0.7, 1.1), 2)

                    sale_items.append({
                        'sale_id': sale_id,
                        'product_id': prod_id,
                        'quantity': quantity,
                        'unit_price': unit_price
                    })

            added_items = batch_insert(engine, "sale_item", sale_items)
            print(
                f"[{dbname}] Sale: создано {len(sale_ids)} (всего {table_count(engine, 'sale')}); Sale_item: добавлено ~{added_items} (всего {table_count(engine, 'sale_item')})")

            if table_exists(engine, "sale") and table_exists(engine, "sale_item"):
                try:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            UPDATE sale
                            SET in_total = sub.sum
                            FROM (
                                SELECT sale_id, SUM(line_total) AS sum
                                FROM sale_item
                                GROUP BY sale_id
                            ) sub
                            WHERE sale.id = sub.sale_id
                        """))
                        print(f"[{dbname}] Обновлены итоговые суммы продаж")
                except Exception as e:
                    print(f"[{dbname}] Ошибка при обновлении in_total: {e}")
    else:
        print(f"[{dbname}] sale/sale_item/customer/product отсутствуют — пропуск создания продаж")

    print("=" * 60)
    print(f"[{dbname}] Сидирование реалистичными данными завершено")
    print("=" * 60)


def main():
    if not DBS_TO_SEED:
        print("Список баз пуст. Задай DBS_TO_SEED в окружении.")
        return

    for db in DBS_TO_SEED:
        try:
            engine = make_engine(db)
            wait_for_db(engine)
            seed_database(engine)
        except Exception as e:
            print(f"[{db}] Ошибка: {e}")


if __name__ == "__main__":
    main()