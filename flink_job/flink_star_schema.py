import os
from pyflink.table import EnvironmentSettings, TableEnvironment


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "pet_store_sales")
PG_URL = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/pet_store")
PG_USER = os.getenv("PG_USER", "admin")
PG_PASSWORD = os.getenv("PG_PASSWORD", "admin")


def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    t_env.get_config().set("execution.checkpointing.interval", "10000")
    t_env.get_config().set("execution.checkpointing.mode", "EXACTLY_ONCE")
    t_env.get_config().set("restart-strategy", "fixed-delay")
    t_env.get_config().set("restart-strategy.fixed-delay.attempts", "3")
    t_env.get_config().set("restart-strategy.fixed-delay.delay", "10s")

    t_env.get_config().set("parallelism.default", "1")

    t_env.execute_sql(f"""
        CREATE TABLE kafka_source (
            `id`                     STRING,
            `customer_first_name`    STRING,
            `customer_last_name`     STRING,
            `customer_age`           STRING,
            `customer_email`         STRING,
            `customer_country`       STRING,
            `customer_postal_code`   STRING,
            `customer_pet_type`      STRING,
            `customer_pet_name`      STRING,
            `customer_pet_breed`     STRING,
            `seller_first_name`      STRING,
            `seller_last_name`       STRING,
            `seller_email`           STRING,
            `seller_country`         STRING,
            `seller_postal_code`     STRING,
            `product_name`           STRING,
            `product_category`       STRING,
            `product_price`          STRING,
            `product_quantity`       STRING,
            `sale_date`              STRING,
            `sale_customer_id`       STRING,
            `sale_seller_id`         STRING,
            `sale_product_id`        STRING,
            `sale_quantity`          STRING,
            `sale_total_price`       STRING,
            `store_name`             STRING,
            `store_location`         STRING,
            `store_city`             STRING,
            `store_state`            STRING,
            `store_country`          STRING,
            `store_phone`            STRING,
            `store_email`            STRING,
            `pet_category`           STRING,
            `product_weight`         STRING,
            `product_color`          STRING,
            `product_size`           STRING,
            `product_brand`          STRING,
            `product_material`       STRING,
            `product_description`    STRING,
            `product_rating`         STRING,
            `product_reviews`        STRING,
            `product_release_date`   STRING,
            `product_expiry_date`    STRING,
            `supplier_name`          STRING,
            `supplier_contact`       STRING,
            `supplier_email`         STRING,
            `supplier_phone`         STRING,
            `supplier_address`       STRING,
            `supplier_city`          STRING,
            `supplier_country`       STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id' = 'flink-star-schema',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE dim_customer_sink (
            `customer_id`  INT,
            `first_name`   STRING,
            `last_name`    STRING,
            `age`          INT,
            `email`        STRING,
            `country`      STRING,
            `postal_code`  STRING,
            `pet_type`     STRING,
            `pet_name`     STRING,
            `pet_breed`    STRING,
            `pet_category` STRING,
            PRIMARY KEY (customer_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'dim_customer',
            'username' = '{PG_USER}',
            'password' = '{PG_PASSWORD}',
            'sink.buffer-flush.max-rows' = '500',
            'sink.buffer-flush.interval' = '5s'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE dim_seller_sink (
            `seller_id`    INT,
            `first_name`   STRING,
            `last_name`    STRING,
            `email`        STRING,
            `country`      STRING,
            `postal_code`  STRING,
            PRIMARY KEY (seller_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'dim_seller',
            'username' = '{PG_USER}',
            'password' = '{PG_PASSWORD}',
            'sink.buffer-flush.max-rows' = '500',
            'sink.buffer-flush.interval' = '5s'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE dim_product_sink (
            `product_id`    INT,
            `name`          STRING,
            `category`      STRING,
            `price`         DECIMAL(10,2),
            `quantity`       INT,
            `weight`        DECIMAL(10,2),
            `color`         STRING,
            `size`          STRING,
            `brand`         STRING,
            `material`      STRING,
            `description`   STRING,
            `rating`        DECIMAL(3,1),
            `reviews`       INT,
            `release_date`  DATE,
            `expiry_date`   DATE,
            PRIMARY KEY (product_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'dim_product',
            'username' = '{PG_USER}',
            'password' = '{PG_PASSWORD}',
            'sink.buffer-flush.max-rows' = '500',
            'sink.buffer-flush.interval' = '5s'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE dim_store_sink (
            `store_id`   INT,
            `name`       STRING,
            `location`   STRING,
            `city`       STRING,
            `state`      STRING,
            `country`    STRING,
            `phone`      STRING,
            `email`      STRING,
            PRIMARY KEY (store_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'dim_store',
            'username' = '{PG_USER}',
            'password' = '{PG_PASSWORD}',
            'sink.buffer-flush.max-rows' = '500',
            'sink.buffer-flush.interval' = '5s'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE dim_supplier_sink (
            `supplier_id`  INT,
            `name`         STRING,
            `contact`      STRING,
            `email`        STRING,
            `phone`        STRING,
            `address`      STRING,
            `city`         STRING,
            `country`      STRING,
            PRIMARY KEY (supplier_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'dim_supplier',
            'username' = '{PG_USER}',
            'password' = '{PG_PASSWORD}',
            'sink.buffer-flush.max-rows' = '500',
            'sink.buffer-flush.interval' = '5s'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE fact_sales_sink (
            `sale_id`          INT,
            `sale_date`        DATE,
            `customer_id`      INT,
            `seller_id`        INT,
            `product_id`       INT,
            `store_id`         INT,
            `supplier_id`      INT,
            `sale_quantity`     INT,
            `sale_total_price`  DECIMAL(12,2),
            PRIMARY KEY (sale_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'fact_sales',
            'username' = '{PG_USER}',
            'password' = '{PG_PASSWORD}',
            'sink.buffer-flush.max-rows' = '500',
            'sink.buffer-flush.interval' = '5s'
        )
    """)

    t_env.execute_sql("""
        CREATE TEMPORARY VIEW parsed AS
        SELECT
            CAST(`id` AS INT)                              AS id,
            `customer_first_name`,
            `customer_last_name`,
            CAST(`customer_age` AS INT)                    AS customer_age,
            `customer_email`,
            `customer_country`,
            `customer_postal_code`,
            `customer_pet_type`,
            `customer_pet_name`,
            `customer_pet_breed`,
            `seller_first_name`,
            `seller_last_name`,
            `seller_email`,
            `seller_country`,
            `seller_postal_code`,
            `product_name`,
            `product_category`,
            CAST(`product_price` AS DECIMAL(10,2))         AS product_price,
            CAST(`product_quantity` AS INT)                 AS product_quantity,
            `sale_date`,
            CAST(`sale_customer_id` AS INT)                AS sale_customer_id,
            CAST(`sale_seller_id` AS INT)                  AS sale_seller_id,
            CAST(`sale_product_id` AS INT)                 AS sale_product_id,
            CAST(`sale_quantity` AS INT)                    AS sale_quantity,
            CAST(`sale_total_price` AS DECIMAL(12,2))      AS sale_total_price,
            `store_name`,
            `store_location`,
            `store_city`,
            `store_state`,
            `store_country`,
            `store_phone`,
            `store_email`,
            `pet_category`,
            CAST(`product_weight` AS DECIMAL(10,2))        AS product_weight,
            `product_color`,
            `product_size`,
            `product_brand`,
            `product_material`,
            `product_description`,
            CAST(`product_rating` AS DECIMAL(3,1))         AS product_rating,
            CAST(`product_reviews` AS INT)                 AS product_reviews,
            `product_release_date`,
            `product_expiry_date`,
            `supplier_name`,
            `supplier_contact`,
            `supplier_email`,
            `supplier_phone`,
            `supplier_address`,
            `supplier_city`,
            `supplier_country`
        FROM kafka_source
    """)

    stmt_set = t_env.create_statement_set()

    stmt_set.add_insert_sql("""
        INSERT INTO dim_customer_sink
        SELECT
            sale_customer_id,
            customer_first_name,
            customer_last_name,
            customer_age,
            customer_email,
            customer_country,
            customer_postal_code,
            customer_pet_type,
            customer_pet_name,
            customer_pet_breed,
            pet_category
        FROM parsed
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO dim_seller_sink
        SELECT
            sale_seller_id,
            seller_first_name,
            seller_last_name,
            seller_email,
            seller_country,
            seller_postal_code
        FROM parsed
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO dim_product_sink
        SELECT
            sale_product_id,
            product_name,
            product_category,
            product_price,
            product_quantity,
            product_weight,
            product_color,
            product_size,
            product_brand,
            product_material,
            product_description,
            product_rating,
            product_reviews,
            CASE
                WHEN product_release_date IS NOT NULL AND product_release_date <> ''
                THEN TO_DATE(product_release_date, 'M/d/yyyy')
                ELSE NULL
            END,
            CASE
                WHEN product_expiry_date IS NOT NULL AND product_expiry_date <> ''
                THEN TO_DATE(product_expiry_date, 'M/d/yyyy')
                ELSE NULL
            END
        FROM parsed
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO dim_store_sink
        SELECT
            id,
            store_name,
            store_location,
            store_city,
            store_state,
            store_country,
            store_phone,
            store_email
        FROM parsed
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO dim_supplier_sink
        SELECT
            id,
            supplier_name,
            supplier_contact,
            supplier_email,
            supplier_phone,
            supplier_address,
            supplier_city,
            supplier_country
        FROM parsed
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO fact_sales_sink
        SELECT
            id,
            CASE
                WHEN sale_date IS NOT NULL AND sale_date <> ''
                THEN TO_DATE(sale_date, 'M/d/yyyy')
                ELSE NULL
            END,
            sale_customer_id,
            sale_seller_id,
            sale_product_id,
            id,
            id,
            sale_quantity,
            sale_total_price
        FROM parsed
    """)

    print("[flink-job] Запускаем трансформацию Kafka -> Star Schema -> PostgreSQL...")
    stmt_set.execute().wait()
    print("[flink-job] Джоба завершена.")


if __name__ == "__main__":
    main()
