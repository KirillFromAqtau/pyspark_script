import os
import glob
import random
import shutil
from datetime import datetime, timedelta
from faker import Faker

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType,
    StringType, DoubleType, DateType
)

# ==============================================================================
# 1. Инициализация Spark и подавление логов
# ==============================================================================
spark = SparkSession.builder \
    .appName("DevSyntheticDataGenerator_Faker") \
    .master("local[*]") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ==============================================================================
# 2. Ввод количества строк
# ==============================================================================
while True:
    try:
        user_input = input("Введите количество генерируемых строк: ").strip()
        num_rows = int(user_input)
        if num_rows <= 0:
            print("Число должно быть строго больше 0.")
            continue
        break
    except ValueError:
        print("Введите корректное целое число.")

# ==============================================================================
# 3. Генерация данных с помощью Faker
# ==============================================================================
fake = Faker("ru_RU")
raw_data = []

current_date = datetime.now().date()

for row_id in range(1, num_rows + 1):
    # Условие: длина имени >= 5
    while True:
        first_name = fake.first_name()
        if len(first_name) >= 5:
            break

    # Email на базе имени с доменами .ru / .com
    domain = random.choice(["yandex.ru", "mail.ru", "gmail.com", "company.com"])
    # Транслитерация или простое приведение к латинице для корректного email
    email_user = fake.lexify(text="user_??????")
    email = f"{email_user}_{row_id}@{domain}"

    # Условие: длина названия города >= 7
    while True:
        city_name = fake.city()
        if len(city_name) >= 7:
            break

    # Возраст: от 18 до 95
    age = random.randint(18, 95)

    # Случайная зарплата
    salary = round(random.uniform(30000.0, 350000.0), 2)

    # Дата регистрации: человек не может зарегистрироваться раньше своего 18-летия
    # Вычисляем максимальное число дней с момента 18-летия до текущей даты
    max_registration_days = (age - 18) * 365
    days_offset = random.randint(0, max_registration_days)
    registration_date = current_date - timedelta(days=days_offset)

    raw_data.append((
        row_id,
        first_name,
        email,
        city_name,
        age,
        salary,
        registration_date
    ))

# ==============================================================================
# 4. Создание Spark DataFrame по строгой схеме
# ==============================================================================
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("city", StringType(), True),
    StructField("age", IntegerType(), False),
    StructField("salary", DoubleType(), True),
    StructField("registration_date", DateType(), False)
])

df = spark.createDataFrame(raw_data, schema=schema)

# ==============================================================================
# 5. Обработка условий по NULL (до 5% и только если строк > 10)
# ==============================================================================
if num_rows > 10:
    # Заменяем случайные ~4% значений на NULL в полях city и salary
    df = df.withColumn(
        "city",
        F.when(F.rand() < 0.04, F.lit(None)).otherwise(F.col("city"))
    )
    df = df.withColumn(
        "salary",
        F.when(F.rand() < 0.04, F.lit(None)).otherwise(F.col("salary"))
    )

# --- ПРОВЕРКА КРИТЕРИЯ NULL (Закомментирована по условию ТЗ) ---
# null_city_count = df.filter(F.col("city").isNull()).count()
# null_salary_count = df.filter(F.col("salary").isNull()).count()
# null_salary_pct = (null_salary_count / num_rows) * 100
# if num_rows <= 10:
#     assert null_city_count == 0 and null_salary_count == 0, "При n <= 10 NULL быть не должно!"
# else:
#     assert null_salary_pct <= 5.0, f"Превышен лимит NULL: {null_salary_pct}%"
# -------------------------------------------------------------

# ==============================================================================
# 6. Экспорт в строго один CSV с удалением метаданных
# ==============================================================================
current_date_str = datetime.now().strftime("%Y-%m-%d")
final_filename = f"{current_date_str}-dev_v2.csv"
temp_dir = f"./temp_faker_{current_date_str}"

# Схлопываем данные в один поток и выгружаем во временную директорию
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

# Поиск полученного part-*.csv и очистка
part_files = glob.glob(os.path.join(temp_dir, "part-*.csv"))

if part_files:
    shutil.move(part_files[0], os.path.join(".", final_filename))
    shutil.rmtree(temp_dir)
    print(f"\nФайл успешно сгенерирован через Faker: {final_filename} (строк: {num_rows})")
else:
    print("\nОшибка при экспорте CSV.")

spark.stop()