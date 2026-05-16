CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id   INT PRIMARY KEY,
    first_name    VARCHAR(100),
    last_name     VARCHAR(100),
    age           INT,
    email         VARCHAR(255),
    country       VARCHAR(100),
    postal_code   VARCHAR(20),
    pet_type      VARCHAR(50),
    pet_name      VARCHAR(100),
    pet_breed     VARCHAR(100),
    pet_category  VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id     INT PRIMARY KEY,
    first_name    VARCHAR(100),
    last_name     VARCHAR(100),
    email         VARCHAR(255),
    country       VARCHAR(100),
    postal_code   VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id       INT PRIMARY KEY,
    name             VARCHAR(255),
    category         VARCHAR(100),
    price            NUMERIC(10,2),
    quantity         INT,
    weight           NUMERIC(10,2),
    color            VARCHAR(50),
    size             VARCHAR(50),
    brand            VARCHAR(100),
    material         VARCHAR(100),
    description      TEXT,
    rating           NUMERIC(3,1),
    reviews          INT,
    release_date     DATE,
    expiry_date      DATE
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id      INT PRIMARY KEY,
    name          VARCHAR(255),
    location      VARCHAR(255),
    city          VARCHAR(100),
    state         VARCHAR(100),
    country       VARCHAR(100),
    phone         VARCHAR(50),
    email         VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id   INT PRIMARY KEY,
    name          VARCHAR(255),
    contact       VARCHAR(255),
    email         VARCHAR(255),
    phone         VARCHAR(50),
    address       VARCHAR(255),
    city          VARCHAR(100),
    country       VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id          INT PRIMARY KEY,
    sale_date        DATE,
    customer_id      INT,
    seller_id        INT,
    product_id       INT,
    store_id         INT,
    supplier_id      INT,
    sale_quantity     INT,
    sale_total_price  NUMERIC(12,2)
);
