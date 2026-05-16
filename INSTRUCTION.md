# Лабораторная работа №3 — Streaming Processing с Apache Flink

## Оглавление

1. [Общее описание](#1-общее-описание)
2. [Архитектура решения](#2-архитектура-решения)
3. [Модель данных «звезда»](#3-модель-данных-звезда)
4. [Структура проекта](#4-структура-проекта)
5. [Предварительные требования](#5-предварительные-требования)
6. [Пошаговый запуск](#6-пошаговый-запуск)
7. [Проверка результатов](#7-проверка-результатов)
8. [Остановка и очистка](#8-остановка-и-очистка)
9. [Описание компонентов](#9-описание-компонентов)

---

## 1. Общее описание

Цель работы — реализовать потоковую обработку данных (streaming processing) с помощью Apache Flink. Система читает данные из CSV-файлов, отправляет их в Apache Kafka в формате JSON, а Flink в реальном времени трансформирует эти данные в аналитическую модель «звезда» (star schema) и сохраняет результат в PostgreSQL.

Исходные данные — 10 CSV-файлов (`MOCK_DATA.csv`, `MOCK_DATA (1).csv` — `MOCK_DATA (9).csv`), каждый содержит 1000 строк с информацией о продажах зоомагазина: покупатели, продавцы, товары, магазины, поставщики и сами продажи.

---

## 2. Архитектура решения

```
CSV (10 файлов) --> Kafka Producer (Python) --> Kafka topic --> Flink (PyFlink) --> PostgreSQL (star schema)
```

Поток данных:

1. **Kafka Producer** (Python-скрипт) читает CSV-файлы, преобразует каждую строку в JSON и отправляет сообщением в Kafka-топик `pet_store_sales`. При этом генерируются глобально уникальные ID (так как в каждом CSV-файле ID идут от 1 до 1000, продюсер добавляет смещение: файл 0 → ID 1-1000, файл 1 → ID 1001-2000, ..., файл 9 → ID 9001-10000).

2. **Apache Kafka** выступает как брокер сообщений — принимает JSON-сообщения от продюсера и хранит их в топике до тех пор, пока Flink их не прочитает.

3. **Apache Flink** (PyFlink, streaming mode) подключается к Kafka-топику, читает каждое JSON-сообщение, разбирает его на составные части (покупатель, продавец, товар, магазин, поставщик, продажа), приводит типы данных и записывает в 6 таблиц PostgreSQL параллельно (через StatementSet).

4. **PostgreSQL** хранит итоговую модель «звезда» — 5 таблиц измерений и 1 таблицу фактов.

---

## 3. Модель данных «звезда»

Модель «звезда» (star schema) — это способ организации данных в хранилище, где одна центральная таблица фактов окружена таблицами измерений.

### Таблица фактов

**`fact_sales`** — каждая строка = одна продажа:

| Поле | Тип | Описание |
|------|-----|----------|
| sale_id | INT PK | Уникальный ID продажи |
| sale_date | DATE | Дата продажи |
| customer_id | INT | → dim_customer |
| seller_id | INT | → dim_seller |
| product_id | INT | → dim_product |
| store_id | INT | → dim_store |
| supplier_id | INT | → dim_supplier |
| sale_quantity | INT | Количество единиц |
| sale_total_price | NUMERIC(12,2) | Общая сумма |

### Таблицы измерений

**`dim_customer`** — покупатели (11 полей): имя, фамилия, возраст, email, страна, почтовый индекс, тип питомца, имя питомца, порода, категория питомца.

**`dim_seller`** — продавцы (6 полей): имя, фамилия, email, страна, почтовый индекс.

**`dim_product`** — товары (15 полей): название, категория, цена, количество, вес, цвет, размер, бренд, материал, описание, рейтинг, отзывы, даты выпуска/истечения.

**`dim_store`** — магазины (8 полей): название, адрес, город, штат, страна, телефон, email.

**`dim_supplier`** — поставщики (8 полей): название, контакт, email, телефон, адрес, город, страна.

> **Примечание:** `dim_date` не создаётся — дата продажи хранится напрямую в `fact_sales` как поле типа DATE. Создание отдельной таблицы-измерения для дат избыточно для данного объёма данных.

---

## 4. Структура проекта

```
BigDataFlink/
├── README.md                        # Описание задания (из репозитория)
├── INSTRUCTION.md                   # Эта инструкция
├── docker-compose.yml               # Вся инфраструктура в одном файле
├── init.sql                         # SQL-скрипт создания таблиц
│
├── исходные данные/                 # 10 CSV-файлов с данными
│   ├── MOCK_DATA.csv
│   ├── MOCK_DATA (1).csv
│   ├── ...
│   └── MOCK_DATA (9).csv
│
├── kafka_producer/                  # Компонент: отправка данных в Kafka
│   ├── Dockerfile
│   ├── producer.py
│   └── requirements.txt
│
└── flink_job/                       # Компонент: Flink streaming job
    ├── Dockerfile
    ├── flink_star_schema.py
    ├── submit_job.sh
    └── requirements.txt
```

---

## 5. Предварительные требования

- **Docker** версии 20.10+ и **Docker Compose** версии 2.0+
- **~6 ГБ свободной оперативной памяти**
- **~3 ГБ свободного места на диске** (Docker-образы)
- **Доступ в интернет** при первом запуске (скачивание образов и JAR-файлов)

---

## 6. Пошаговый запуск

### Шаг 1. Клонировать репозиторий

```bash
git clone <URL_репозитория>
cd BigDataFlink
```

### Шаг 2. Запустить инфраструктуру

```bash
docker compose up -d --build
```

Что произойдёт:
- Соберутся два кастомных Docker-образа (Kafka Producer и Flink с Python)
- Запустятся 6 контейнеров: Zookeeper, Kafka, PostgreSQL, Flink JobManager, Flink TaskManager, Kafka Producer
- PostgreSQL автоматически выполнит `init.sql` и создаст все 6 таблиц
- Kafka Producer дождётся готовности Kafka (healthcheck) и начнёт отправлять данные

Первый запуск займёт **5-10 минут** (скачивание образов и сборка). Последующие запуски — несколько секунд.

### Шаг 3. Проверить, что всё поднялось

```bash
docker compose ps
```

Все контейнеры должны быть в статусе `Up` или `Up (healthy)`. Если kafka-producer завершился с кодом 0 — это нормально, он отправил все данные и остановился.

### Шаг 4. Убедиться, что данные попали в Kafka

```bash
docker exec -it kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic pet_store_sales \
    --from-beginning \
    --max-messages 3
```

Должно вывести 3 JSON-сообщения с данными из CSV.

### Шаг 5. Запустить Flink-джобу

```bash
docker exec -it jobmanager bash /opt/flink-job/submit_job.sh
```

Скрипт проверит наличие JAR-коннекторов, подождёт 30 секунд и запустит PyFlink-джобу. Джоба работает в streaming-режиме — она будет висеть и ждать новых сообщений. Когда все данные обработаны, остановить через **Ctrl+C**.

---

## 6. Проверка результатов

### Подключение к PostgreSQL

```bash
docker exec -it postgres psql -U admin -d pet_store
```

Параметры подключения: хост `localhost`, порт `5432`, база `pet_store`, пользователь `admin`, пароль `admin`.

### Проверочные SQL-запросы

**Количество записей в каждой таблице:**

```sql
SELECT 'dim_customer' AS tbl, COUNT(*) AS cnt FROM dim_customer
UNION ALL SELECT 'dim_seller', COUNT(*) FROM dim_seller
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_store', COUNT(*) FROM dim_store
UNION ALL SELECT 'dim_supplier', COUNT(*) FROM dim_supplier
UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales;
```

Ожидаемый результат: 10 000 записей в каждой таблице (10 файлов × 1000 строк).

**ТОП-5 покупателей по сумме покупок:**

```sql
SELECT
    c.first_name || ' ' || c.last_name AS customer,
    SUM(f.sale_total_price) AS total_spent
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.first_name, c.last_name
ORDER BY total_spent DESC
LIMIT 5;
```

**Продажи по категориям товаров:**

```sql
SELECT
    p.category,
    COUNT(*) AS sales_count,
    SUM(f.sale_total_price) AS revenue
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY revenue DESC;
```

---

## 7. Остановка и очистка

**Остановить контейнеры (данные PostgreSQL сохранятся):**

```bash
docker compose down
```

**Полная очистка (удаление данных и volumes):**

```bash
docker compose down -v
```

---

## 8. Описание компонентов

### 8.1. Kafka Producer (`kafka_producer/producer.py`)

Эмулирует источник данных: ждёт готовности Kafka, читает все CSV-файлы из `/data`, генерирует уникальные ID через смещение (`original_id + file_index * 1000`), конвертирует строки в JSON и отправляет в топик `pet_store_sales` с задержкой 5мс между сообщениями.

### 8.2. Flink Job (`flink_job/flink_star_schema.py`)

Streaming-джоба на PyFlink: создаёт Table Environment, объявляет Kafka-source (все поля STRING), 6 JDBC-sink таблиц (с PRIMARY KEY для upsert), промежуточный VIEW с приведением типов и через StatementSet параллельно пишет во все 6 таблиц PostgreSQL. Checkpointing настроен на EXACTLY_ONCE с интервалом 10 сек.

### 8.3. Docker Compose

| Сервис | Образ | Порт | Назначение |
|--------|-------|------|------------|
| zookeeper | confluentinc/cp-zookeeper:7.5.0 | 2181 | Координация Kafka |
| kafka | confluentinc/cp-kafka:7.5.0 | 9092, 29092 | Брокер сообщений |
| postgres | postgres:15 | 5432 | Хранилище star schema |
| jobmanager | Flink 1.18.1 + Python | 8081 | Flink менеджер задач |
| taskmanager | Flink 1.18.1 + Python | — | Flink исполнитель |
| kafka-producer | Python 3.11 | — | CSV → JSON → Kafka |
