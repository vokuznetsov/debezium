#!/usr/bin/env bash
# =============================================================================
#  load.sh — генератор изменений в БД (А).
#
#  Режимы:
#    ./scripts/load.sh mixed [батчей] [пауза_сек]
#        Смешанная нагрузка: транзакции, затрагивающие несколько схем в
#        порядке FK (ref -> catalog/sales -> order_items), UPDATE, DELETE,
#        плюс трафик ВНЕ CDC (internal.audit_log) и в исключённую таблицу
#        (sales.order_items_archive). Это основной режим для проверок порядка.
#
#    ./scripts/load.sh bulk [строк] [транзакций]
#        Быстрая массовая вставка через generate_series — для benchmark.sh.
#        Одна транзакция = один батч строк, чтобы мерить пропускную
#        способность, а не задержку клиента.
#
#    ./scripts/load.sh fk-chain [цепочек]
#        Пишет ПОЛНУЮ цепочку FK в одной транзакции: currency -> region ->
#        supplier -> product -> customer -> order -> order_item.
#        Именно этот режим доказывает ценность инварианта №1: если consumer
#        части 2 применит эти события не в порядке WAL, он упрётся в
#        constraint на первой же дочерней строке.
# =============================================================================
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

MODE="${1:-mixed}"

mixed() {
    local batches="${1:-20}" pause="${2:-0}"
    info "смешанная нагрузка: батчей=${batches}, пауза=${pause}с"
    local i
    for i in $(seq 1 "$batches"); do
        pgm -q <<SQL
BEGIN;
-- 1) родитель в sales
INSERT INTO sales.customers (email, name, region_id)
VALUES ('load-${i}-' || md5(random()::text) || '@example.com', 'Load Customer ${i}',
        1 + (random()*3)::int);
-- 2) заказ на только что созданного покупателя (FK)
INSERT INTO sales.orders (customer_id, currency_code, total, status)
SELECT id, 'USD', round((random()*1000)::numeric, 4), 'new'
FROM sales.customers ORDER BY id DESC LIMIT 1;
-- 3) позиции заказа: FK на заказ И на catalog.products (межсхемный)
INSERT INTO sales.order_items (order_id, product_id, qty, price)
SELECT o.id, p.id, 1 + (random()*5)::int, p.price
FROM (SELECT id FROM sales.orders ORDER BY id DESC LIMIT 1) o
CROSS JOIN (SELECT id, price FROM catalog.products ORDER BY random() LIMIT 2) p;
-- 4) UPDATE в другой схеме
UPDATE catalog.products
SET price = round((price * (0.98 + random()*0.04))::numeric, 4), updated_at = now()
WHERE id = 1 + (random()*3)::int;
-- 5) трафик ВНЕ CDC: двигает WAL, событий не даёт. Без heartbeat.interval.ms
--    подтверждённый LSN слота на таком трафике стоял бы на месте.
INSERT INTO internal.audit_log (payload) VALUES ('batch ${i}');
-- 6) запись в ИСКЛЮЧЁННУЮ таблицу: в публикации есть, в cdc.events быть не должно
INSERT INTO sales.order_items_archive (order_id, product_id, qty, price)
SELECT order_id, product_id, qty, price FROM sales.order_items ORDER BY id DESC LIMIT 1;
COMMIT;
SQL
        [ "$pause" != "0" ] && sleep "$pause"
    done
    # DELETE отдельной транзакцией: в профиле slim он приезжает строкой с
    # __deleted=true (delete.handling.mode=rewrite), а не пустым значением,
    # и БЕЗ tombstone (tombstones.on.delete=false + drop.tombstones=true).
    pgm -q -c "DELETE FROM sales.order_items WHERE id IN (
                 SELECT id FROM sales.order_items ORDER BY id DESC LIMIT 2);"
    ok "смешанная нагрузка завершена (${batches} батчей + DELETE)"
}

bulk() {
    local rows="${1:-5000}" txs="${2:-10}"
    info "массовая вставка: ${txs} транзакций x ${rows} строк = $((rows*txs)) событий"
    local i
    for i in $(seq 1 "$txs"); do
        pgm -q <<SQL
INSERT INTO sales.order_items (order_id, product_id, qty, price)
SELECT (SELECT id FROM sales.orders ORDER BY id DESC LIMIT 1),
       1 + (g % 4),
       1 + (g % 5),
       round((100 + g % 900)::numeric, 4)
FROM generate_series(1, ${rows}) g;
SQL
    done
    ok "массовая вставка завершена: $((rows*txs)) строк"
}

fk_chain() {
    local n="${1:-5}"
    info "полные цепочки FK: ${n}"
    local i
    for i in $(seq 1 "$n"); do
        pgm -q <<SQL
BEGIN;
-- Порядок внутри транзакции = порядок в WAL = порядок в cdc.events.
-- Каждая следующая строка ссылается на предыдущую.
INSERT INTO ref.currencies (code, name, minor_unit)
VALUES ('X' || lpad(((${i} * 7) % 100)::text, 2, '0'), 'Test Currency ${i}', 2)
ON CONFLICT (code) DO NOTHING;
INSERT INTO ref.regions (id, code, name, currency_code)
VALUES (1000 + ${i}, 'TST${i}', 'Test Region ${i}',
        (SELECT code FROM ref.currencies ORDER BY code DESC LIMIT 1))
ON CONFLICT (id) DO NOTHING;
INSERT INTO catalog.suppliers (name, region_id)
VALUES ('Chain Supplier ${i}', 1000 + ${i});
INSERT INTO catalog.products (sku, name, supplier_id, currency_code, price, description)
SELECT 'CHAIN-${i}-' || s.id, 'Chain Product ${i}', s.id,
       (SELECT currency_code FROM ref.regions WHERE id = 1000 + ${i}),
       round((random()*500)::numeric, 4), 'из цепочки FK'
FROM (SELECT id FROM catalog.suppliers ORDER BY id DESC LIMIT 1) s;
INSERT INTO sales.customers (email, name, region_id)
VALUES ('chain-${i}-' || md5(random()::text) || '@example.com', 'Chain Customer ${i}', 1000 + ${i});
INSERT INTO sales.orders (customer_id, currency_code, total, status)
SELECT c.id, (SELECT currency_code FROM ref.regions WHERE id = 1000 + ${i}), 0, 'new'
FROM (SELECT id FROM sales.customers ORDER BY id DESC LIMIT 1) c;
INSERT INTO sales.order_items (order_id, product_id, qty, price)
SELECT o.id, p.id, 1, p.price
FROM (SELECT id FROM sales.orders ORDER BY id DESC LIMIT 1) o
CROSS JOIN (SELECT id, price FROM catalog.products ORDER BY id DESC LIMIT 1) p;
COMMIT;
SQL
    done
    ok "цепочек FK записано: ${n}"
}

case "$MODE" in
    mixed)    mixed    "${2:-20}" "${3:-0}" ;;
    bulk)     bulk     "${2:-5000}" "${3:-10}" ;;
    fk-chain) fk_chain "${2:-5}" ;;
    *) fail "режимы: mixed | bulk | fk-chain" ;;
esac
