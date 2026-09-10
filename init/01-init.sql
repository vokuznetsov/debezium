-- =============================================================================
--  Инициализация мастера. Выполняется автоматически ОДИН РАЗ — при первом
--  старте pg-master на пустом томе (каталог /docker-entrypoint-initdb.d).
--
--  Поменяли этот файл -> нужен чистый том:
--      docker compose down -v && docker compose up -d
--
--  Всё, что делается ПОСЛЕ старта (добавить таблицу, залить данные), делается
--  руками через psql — см. README.
-- =============================================================================

-- --- роли --------------------------------------------------------------------
-- replicator: под ней работают pg-replica-1/2 (пароль совпадает с .env).
-- debezium:   логическое декодирование. REPLICATION нужен обязательно,
--             прав на CREATE (слоты, публикации) сознательно не даём.
CREATE ROLE replicator WITH LOGIN REPLICATION PASSWORD 'replicator';
CREATE ROLE debezium   WITH LOGIN REPLICATION PASSWORD 'debezium';

-- --- схемы и таблицы ---------------------------------------------------------
-- Три схемы с межсхемными FK: currencies/regions -> suppliers/products ->
-- customers/orders/order_items. Цепочка нужна, чтобы было видно: порядок
-- событий в топике совпадает с порядком в WAL, родитель всегда раньше ребёнка.
CREATE SCHEMA ref;
CREATE SCHEMA catalog;
CREATE SCHEMA sales;

CREATE TABLE ref.currencies (
    code        char(3)      PRIMARY KEY,
    name        text         NOT NULL,
    minor_unit  smallint     NOT NULL DEFAULT 2
);

CREATE TABLE ref.regions (
    id             int          PRIMARY KEY,
    code           text         NOT NULL UNIQUE,
    name           text         NOT NULL,
    currency_code  char(3)      NOT NULL REFERENCES ref.currencies(code)
);

CREATE TABLE catalog.suppliers (
    id          bigserial    PRIMARY KEY,
    name        text         NOT NULL,
    region_id   int          NOT NULL REFERENCES ref.regions(id),
    created_at  timestamptz  NOT NULL DEFAULT now()
);

CREATE TABLE catalog.products (
    id             bigserial      PRIMARY KEY,
    sku            text           NOT NULL UNIQUE,
    name           text           NOT NULL,
    supplier_id    bigint         NOT NULL REFERENCES catalog.suppliers(id),
    currency_code  char(3)        NOT NULL REFERENCES ref.currencies(code),
    -- Денежное поле: numeric(18,4). В Avro уезжает как bytes с
    -- logicalType=decimal (decimal.handling.mode=precise).
    price          numeric(18,4)  NOT NULL,
    description    text,
    updated_at     timestamptz    NOT NULL DEFAULT now()
);

CREATE TABLE sales.customers (
    id          bigserial    PRIMARY KEY,
    email       text         NOT NULL UNIQUE,
    name        text         NOT NULL,
    region_id   int          NOT NULL REFERENCES ref.regions(id),
    created_at  timestamptz  NOT NULL DEFAULT now()
);

CREATE TABLE sales.orders (
    id             bigserial      PRIMARY KEY,
    customer_id    bigint         NOT NULL REFERENCES sales.customers(id),
    currency_code  char(3)        NOT NULL REFERENCES ref.currencies(code),
    total          numeric(18,4)  NOT NULL DEFAULT 0,
    status         text           NOT NULL DEFAULT 'new',
    created_at     timestamptz    NOT NULL DEFAULT now()
);

CREATE TABLE sales.order_items (
    id          bigserial      PRIMARY KEY,
    order_id    bigint         NOT NULL REFERENCES sales.orders(id),
    product_id  bigint         NOT NULL REFERENCES catalog.products(id),
    qty         int            NOT NULL CHECK (qty > 0),
    price       numeric(18,4)  NOT NULL
);

-- REPLICA IDENTITY оставлен дефолтным (DEFAULT = только PK): FULL пишет в WAL
-- полный старый образ строки на каждый UPDATE/DELETE.

-- --- права для Debezium ------------------------------------------------------
GRANT USAGE ON SCHEMA ref, catalog, sales TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA ref, catalog, sales TO debezium;
-- Чтобы новые таблицы в этих схемах тоже были ему видны:
ALTER DEFAULT PRIVILEGES IN SCHEMA ref, catalog, sales GRANT SELECT ON TABLES TO debezium;

-- --- публикация --------------------------------------------------------------
-- FOR TABLES IN SCHEMA (PG 15+): состав ведётся по схемам, а не списком из
-- 500 таблиц. Новая таблица в ref/catalog/sales попадает в CDC сама.
CREATE PUBLICATION dbz_cdc_pub FOR TABLES IN SCHEMA ref, catalog, sales;

-- --- слоты репликации -------------------------------------------------------
-- Физические — под реплики: pg_basebackup на них ссылается через -S.
SELECT pg_create_physical_replication_slot('replica_1_slot');
SELECT pg_create_physical_replication_slot('replica_2_slot');

-- Логический слот CDC. Создаём его МЫ, а не Debezium: у Debezium нет на это
-- прав, и сам он не выставляет failover => true (последний аргумент), без
-- которого промоушен реплики потерял бы слот вместе со всей позицией CDC.
--
-- Момент создания слота = граница потока: WAL удерживается с этой точки,
-- поэтому изменения, сделанные до старта коннектора, не теряются.
SELECT pg_create_logical_replication_slot('dbz_cdc_slot', 'pgoutput',
                                          false,  -- temporary
                                          false,  -- two_phase
                                          true);  -- failover
