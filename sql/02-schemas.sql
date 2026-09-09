-- =============================================================================
--  02-schemas.sql — тестовая схема данных
--
--  Три схемы одной базы с МЕЖСХЕМНЫМИ внешними ключами. Цепочка FK намеренно
--  глубокая, чтобы при перемешанном порядке применения consumer из части 2
--  ГАРАНТИРОВАННО упёрся в constraint — это и есть доказательство ценности
--  инварианта №1 (единый топик, одна партиция, порядок WAL).
--
--  Глубина цепочек:
--    ref.currencies -> ref.regions -> catalog.suppliers -> catalog.products
--                                                       -> sales.order_items
--    ref.currencies -> ref.regions -> sales.customers -> sales.orders
--                                                     -> sales.order_items
--  То есть sales.order_items достижим двумя путями длиной 5. Вставить его
--  раньше родителей нельзя ни при каком порядке применения.
--
--  Идемпотентен: CREATE SCHEMA/TABLE IF NOT EXISTS.
--  На реальном контуре здесь ~500 таблиц в разных схемах; состав CDC живёт в
--  публикации (04) и в schema.include.list коннектора, а не в этом файле.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS ref;
CREATE SCHEMA IF NOT EXISTS catalog;
CREATE SCHEMA IF NOT EXISTS sales;

-- Схема, НЕ попадающая в CDC. Нужна, чтобы проверить, что schema.include.list
-- действительно фильтрует, и чтобы heartbeat имел смысл: трафик в этой схеме
-- двигает WAL, но не даёт событий в cdc.events.
CREATE SCHEMA IF NOT EXISTS internal;

-- -----------------------------------------------------------------------------
--  ref — справочники, корень всех цепочек FK
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ref.currencies (
    code        char(3)      PRIMARY KEY,
    name        text         NOT NULL,
    minor_unit  smallint     NOT NULL DEFAULT 2
);

CREATE TABLE IF NOT EXISTS ref.regions (
    id             int          PRIMARY KEY,
    code           text         NOT NULL UNIQUE,
    name           text         NOT NULL,
    currency_code  char(3)      NOT NULL REFERENCES ref.currencies(code)
);

-- -----------------------------------------------------------------------------
--  catalog
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS catalog.suppliers (
    id          bigserial    PRIMARY KEY,
    name        text         NOT NULL,
    region_id   int          NOT NULL REFERENCES ref.regions(id),   -- межсхемный FK
    created_at  timestamptz  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS catalog.products (
    id             bigserial      PRIMARY KEY,
    sku            text           NOT NULL UNIQUE,
    name           text           NOT NULL,
    supplier_id    bigint         NOT NULL REFERENCES catalog.suppliers(id),
    currency_code  char(3)        NOT NULL REFERENCES ref.currencies(code), -- межсхемный FK
    -- Денежное поле. decimal.handling.mode остаётся precise, в Avro это
    -- bytes с logicalType=decimal. НЕ переключать на double: это банковские
    -- данные, double молча портит значения. См. README, раздел 5.
    price          numeric(18,4)  NOT NULL,
    -- Тяжёлая колонка — демонстрация column.exclude.list в конфиге
    -- коннектора. В WAL она попадает (пока не сделана публикация с
    -- column list), но в Kafka не уезжает.
    spec_blob      bytea,
    description    text,
    updated_at     timestamptz    NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
--  sales
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sales.customers (
    id          bigserial    PRIMARY KEY,
    email       text         NOT NULL UNIQUE,
    name        text         NOT NULL,
    region_id   int          NOT NULL REFERENCES ref.regions(id),   -- межсхемный FK
    created_at  timestamptz  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sales.orders (
    id             bigserial      PRIMARY KEY,
    customer_id    bigint         NOT NULL REFERENCES sales.customers(id),
    currency_code  char(3)        NOT NULL REFERENCES ref.currencies(code), -- межсхемный FK
    total          numeric(18,4)  NOT NULL DEFAULT 0,
    status         text           NOT NULL DEFAULT 'new',
    created_at     timestamptz    NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sales.order_items (
    id          bigserial      PRIMARY KEY,
    order_id    bigint         NOT NULL REFERENCES sales.orders(id),
    product_id  bigint         NOT NULL REFERENCES catalog.products(id), -- межсхемный FK
    qty         int            NOT NULL CHECK (qty > 0),
    price       numeric(18,4)  NOT NULL
);

-- Архив позиций. В публикацию попадает (она ведётся по схемам ref/catalog/
-- sales), но коннектор исключает её через table.exclude.list. Это рабочая
-- демонстрация механизма точечного исключения: строки в таблицу пишутся,
-- WAL по ней идёт, а событий в cdc.events нет — verify-order.sh это
-- показывает в статистике по таблицам.
CREATE TABLE IF NOT EXISTS sales.order_items_archive (
    id          bigserial      PRIMARY KEY,
    order_id    bigint         NOT NULL,
    product_id  bigint         NOT NULL,
    qty         int            NOT NULL,
    price       numeric(18,4)  NOT NULL,
    archived_at timestamptz    NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
--  internal — вне CDC
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS internal.audit_log (
    id       bigserial    PRIMARY KEY,
    ts       timestamptz  NOT NULL DEFAULT now(),
    payload  text
);

-- -----------------------------------------------------------------------------
--  Права роли Debezium: только USAGE + SELECT, никаких DDL
-- -----------------------------------------------------------------------------
-- (интерполяция :'dbz_user' внутри DO $$ не работает — см. комментарий
-- в 01-roles.sql, поэтому значение кладётся в GUC)
SELECT set_config('cdcinit.dbz_user',    :'dbz_user',    false),
       set_config('cdcinit.cdc_schemas', :'cdc_schemas', false);

DO $$
DECLARE
    v_user text := current_setting('cdcinit.dbz_user');
    v_sch  text;
BEGIN
    -- Список схем приходит из CDC_SCHEMAS в .env — единственное место правки.
    FOREACH v_sch IN ARRAY string_to_array(current_setting('cdcinit.cdc_schemas'), ',') LOOP
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', v_sch, v_user);
        EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO %I', v_sch, v_user);
        -- Чтобы новые таблицы (их будет много) не требовали ручного GRANT
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO %I',
            v_sch, v_user);
    END LOOP;
END $$;

\echo '>>> 02-schemas.sql: таблицы и внешние ключи'
SELECT n.nspname || '.' || c.relname AS "таблица",
       (SELECT count(*) FROM pg_constraint k
         WHERE k.conrelid = c.oid AND k.contype = 'f') AS "внешних ключей",
       c.relreplident AS "replica identity"
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE (n.nspname = ANY (string_to_array(:'cdc_schemas', ',')) OR n.nspname = 'internal')
  AND c.relkind = 'r'
ORDER BY 1;
