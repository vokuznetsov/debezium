-- =============================================================================
--  05-seed.sql — НАЧАЛЬНЫЕ ДАННЫЕ, КОТОРЫЕ СУЩЕСТВУЮТ ДО ЗАПУСКА CDC
--
--  Это не «тестовые данные для удобства». Это модель реального положения дел:
--  базы (А) и (Б) синхронизированы на момент T другим способом (bulk-заливкой,
--  бэкапом, чем угодно), и эти строки в (Б) УЖЕ ЕСТЬ.
--
--  Отсюда главное утверждение теста scenario-cold-start.sh:
--  этих строк в топике cdc.events быть НЕ ДОЛЖНО. Они были записаны до
--  создания слота, значит их WAL никем не удерживался и в поток не попадёт.
--  А при snapshot.mode=no_data Debezium их и не вычитывает.
--
--  Идемпотентен: явные id + ON CONFLICT DO NOTHING, последовательности
--  выравниваются setval в конце. Повторный запуск ничего не меняет — а значит
--  и в WAL ничего не пишет.
-- =============================================================================

INSERT INTO ref.currencies (code, name, minor_unit) VALUES
    ('USD', 'US Dollar', 2),
    ('EUR', 'Euro', 2),
    ('RUB', 'Russian Ruble', 2),
    ('JPY', 'Japanese Yen', 0)
ON CONFLICT (code) DO NOTHING;

INSERT INTO ref.regions (id, code, name, currency_code) VALUES
    (1, 'NA',   'North America', 'USD'),
    (2, 'EU',   'Europe',        'EUR'),
    (3, 'CIS',  'CIS',           'RUB'),
    (4, 'APAC', 'Asia Pacific',  'JPY')
ON CONFLICT (id) DO NOTHING;

INSERT INTO catalog.suppliers (id, name, region_id) VALUES
    (1, 'Acme Industrial',  1),
    (2, 'Nordwind GmbH',    2),
    (3, 'Uralmash Trading', 3)
ON CONFLICT (id) DO NOTHING;

INSERT INTO catalog.products (id, sku, name, supplier_id, currency_code, price, description) VALUES
    (1, 'SKU-0001', 'Widget A',  1, 'USD',   19.9900, 'начальный ассортимент'),
    (2, 'SKU-0002', 'Widget B',  1, 'USD',   29.5000, 'начальный ассортимент'),
    (3, 'SKU-0003', 'Gadget C',  2, 'EUR',  149.0000, 'начальный ассортимент'),
    (4, 'SKU-0004', 'Gizmo D',   3, 'RUB', 4999.9900, 'начальный ассортимент')
ON CONFLICT (id) DO NOTHING;

INSERT INTO sales.customers (id, email, name, region_id) VALUES
    (1, 'alice@example.com', 'Alice',   1),
    (2, 'bob@example.com',   'Bob',     2),
    (3, 'carol@example.com', 'Carol',   3)
ON CONFLICT (id) DO NOTHING;

INSERT INTO sales.orders (id, customer_id, currency_code, total, status) VALUES
    (1, 1, 'USD',   49.4800, 'paid'),
    (2, 2, 'EUR',  149.0000, 'paid')
ON CONFLICT (id) DO NOTHING;

INSERT INTO sales.order_items (id, order_id, product_id, qty, price) VALUES
    (1, 1, 1, 1,  19.9900),
    (2, 1, 2, 1,  29.5000),
    (3, 2, 3, 1, 149.0000)
ON CONFLICT (id) DO NOTHING;

-- Последовательности после вставок с явными id: иначе следующий INSERT без id
-- упадёт на конфликте PK.
SELECT setval(pg_get_serial_sequence('catalog.suppliers',  'id'), (SELECT max(id) FROM catalog.suppliers),  true);
SELECT setval(pg_get_serial_sequence('catalog.products',   'id'), (SELECT max(id) FROM catalog.products),   true);
SELECT setval(pg_get_serial_sequence('sales.customers',    'id'), (SELECT max(id) FROM sales.customers),    true);
SELECT setval(pg_get_serial_sequence('sales.orders',       'id'), (SELECT max(id) FROM sales.orders),       true);
SELECT setval(pg_get_serial_sequence('sales.order_items',  'id'), (SELECT max(id) FROM sales.order_items),  true);

\echo '>>> 05-seed.sql: строк в таблицах (это состояние ДО CDC)'
SELECT 'ref.currencies'     AS "таблица", count(*) AS "строк" FROM ref.currencies
UNION ALL SELECT 'ref.regions',        count(*) FROM ref.regions
UNION ALL SELECT 'catalog.suppliers',  count(*) FROM catalog.suppliers
UNION ALL SELECT 'catalog.products',   count(*) FROM catalog.products
UNION ALL SELECT 'sales.customers',    count(*) FROM sales.customers
UNION ALL SELECT 'sales.orders',       count(*) FROM sales.orders
UNION ALL SELECT 'sales.order_items',  count(*) FROM sales.order_items
ORDER BY 1;
