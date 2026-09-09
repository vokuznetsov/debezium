#!/usr/bin/env bash
# =============================================================================
#  scenario-cold-start.sh — ГЛАВНЫЙ ТЕСТ.
#
#  Воспроизводит реальный запуск: в БД (А) УЖЕ ЕСТЬ ДАННЫЕ, потом поднимается
#  Debezium, потом идёт live-поток.
#
#  Ключевое утверждение, которое доказывает тест:
#  граница «что попадёт в топик» проходит ПО МОМЕНТУ СОЗДАНИЯ СЛОТА, а не по
#  моменту старта коннектора. Слот создан — WAL с этой точки удерживается.
#  Значит devops может создать слот в момент T (когда (А) и (Б) синхронны), а
#  коннектор поднять позже, и изменения за это время не потеряются.
#  На этом держится вся схема «БД синхронны на момент T».
#
#  ВНИМАНИЕ: скрипт начинает с ПОЛНОГО СБРОСА ТОМОВ (docker compose down -v).
#  Иначе доказать границу невозможно: нужен старт без слота.
#
#  Падает с ненулевым кодом на первом несоответствии.
# =============================================================================
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PAUSE_SECONDS="${PAUSE_SECONDS:-60}"

# Сколько сообщений в топике; 0 если топика ещё нет
count_or_zero() { topic_count "$1" 2>/dev/null || echo 0; }

# Ждём, пока в топике станет не меньше N сообщений
wait_count_at_least() {
    local topic="$1" want="$2" timeout="${3:-60}" t=0 have
    while :; do
        have=$(count_or_zero "$topic")
        [ "${have:-0}" -ge "$want" ] && { echo "$have"; return 0; }
        t=$((t+1)); [ "$t" -ge "$timeout" ] && { echo "$have"; return 1; }
        sleep 1
    done
}
# Ждём, пока поток остановится (два одинаковых замера подряд)
wait_settled() {
    local topic="$1" prev=-1 cur t=0
    while :; do
        cur=$(count_or_zero "$topic")
        [ "$cur" = "$prev" ] && { echo "$cur"; return 0; }
        prev="$cur"; t=$((t+1))
        [ "$t" -ge 30 ] && { echo "$cur"; return 0; }
        sleep 2
    done
}

# =============================================================================
step "Поднять инфраструктуру с нуля. Данные залить. СЛОТА НЕТ, КОННЕКТОРА НЕТ"
# =============================================================================
info "сброс томов и подъём с CREATE_CDC_SLOT=false, SEED_DATA=true"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
CREATE_CDC_SLOT=false SEED_DATA=true docker compose up -d >/dev/null 2>&1 \
    || fail "не удалось поднять инфраструктуру"

wait_for "connect healthy" 240 bash -c '[ "$(docker compose ps connect --format "{{.Health}}")" = healthy ]' \
    || fail "connect не поднялся"
ok "инфраструктура поднята (pg-master, 2 реплики, kafka, schema-registry, connect)"

head2 "физическая репликация"
pgm -c "SELECT application_name, state, sync_state FROM pg_stat_replication ORDER BY 1;"
REPL_N=$(pgq "SELECT count(*) FROM pg_stat_replication")
[ "$REPL_N" = "2" ] || fail "реплик в pg_stat_replication: ${REPL_N}, ожидалось 2"
ok "обе реплики стримят"

head2 "слотов CDC быть не должно"
SLOT_N=$(pgq "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$CDC_SLOT'")
[ "$SLOT_N" = "0" ] || fail "слот ${CDC_SLOT} уже существует — тест не может доказать границу"
ok "логического слота ${CDC_SLOT} нет"
CONN_CODE=$(capi_code "/connectors/${CONNECTOR_NAME}")
[ "$CONN_CODE" = "404" ] || fail "коннектор ${CONNECTOR_NAME} уже существует (HTTP ${CONN_CODE})"
ok "коннектора ${CONNECTOR_NAME} нет"

head2 "топик cdc.events создан заранее и пуст"
CNT0=$(topic_count "$CDC_TOPIC")
[ "$CNT0" = "0" ] || fail "в ${CDC_TOPIC} уже ${CNT0} сообщений"
ok "${CDC_TOPIC}: 0 сообщений, партиций $(ktopics --describe --topic "$CDC_TOPIC" | awk -F'PartitionCount: ' 'NR==1{split($2,a," ");print a[1]}')"

# =============================================================================
step "Зафиксировать текущий LSN и число строк по таблицам"
# =============================================================================
LSN_BEFORE=$(pgq "SELECT pg_current_wal_lsn()")
LSN_BEFORE_NUM=$(lsn_num "$LSN_BEFORE")
info "LSN до создания слота: ${LSN_BEFORE} (числом ${LSN_BEFORE_NUM})"
head2 "строки, залитые 05-seed.sql (это состояние, на котором (А) и (Б) синхронны)"
pgm -c "SELECT 'ref.currencies' t, count(*) FROM ref.currencies
        UNION ALL SELECT 'ref.regions', count(*) FROM ref.regions
        UNION ALL SELECT 'catalog.suppliers', count(*) FROM catalog.suppliers
        UNION ALL SELECT 'catalog.products', count(*) FROM catalog.products
        UNION ALL SELECT 'sales.customers', count(*) FROM sales.customers
        UNION ALL SELECT 'sales.orders', count(*) FROM sales.orders
        UNION ALL SELECT 'sales.order_items', count(*) FROM sales.order_items
        ORDER BY 1;"
SEED_ROWS=$(pgq "SELECT (SELECT count(*) FROM ref.currencies)+(SELECT count(*) FROM ref.regions)
                      +(SELECT count(*) FROM catalog.suppliers)+(SELECT count(*) FROM catalog.products)
                      +(SELECT count(*) FROM sales.customers)+(SELECT count(*) FROM sales.orders)
                      +(SELECT count(*) FROM sales.order_items)")
ok "всего строк до CDC: ${SEED_ROWS}"

# =============================================================================
step "СОЗДАТЬ СЛОТ (failover => true). Коннектор всё ещё не поднят"
# =============================================================================
info "SQL, который выполняет devops:"
cat <<'SQLDOC' | sed 's/^/      /'
SELECT pg_create_logical_replication_slot('dbz_cdc_slot', 'pgoutput',
                                          false,   -- temporary
                                          false,   -- two_phase
                                          true);   -- failover
SQLDOC
pgq "SELECT pg_create_logical_replication_slot('$CDC_SLOT','pgoutput',false,false,true)" >/dev/null \
    || fail "не удалось создать слот"
pgm -c "SELECT slot_name, plugin, failover, synced, active, restart_lsn, confirmed_flush_lsn
        FROM pg_replication_slots WHERE slot_name='$CDC_SLOT';"
FO=$(pgq "SELECT failover FROM pg_replication_slots WHERE slot_name='$CDC_SLOT'")
case "$FO" in t|true) ok "слот создан, failover=true — с этого момента WAL удерживается";;
              *) fail "слот создан с failover=${FO}";; esac
SLOT_LSN=$(pgq "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name='$CDC_SLOT'")
SLOT_LSN_NUM=$(lsn_num "$SLOT_LSN")
info "restart_lsn слота: ${SLOT_LSN} (числом ${SLOT_LSN_NUM})"

CONN_CODE=$(capi_code "/connectors/${CONNECTOR_NAME}")
[ "$CONN_CODE" = "404" ] || fail "коннектор появился раньше времени"
ok "коннектор всё ещё не создан (HTTP 404) — это принципиально для шага 6"

# =============================================================================
step "Внести изменения в нескольких схемах, в том числе связанные FK"
# =============================================================================
info "коннектора нет, слот есть — эти изменения обязаны сохраниться в WAL"
./scripts/load.sh fk-chain 3
./scripts/load.sh mixed 4
CHANGES_LSN=$(pgq "SELECT pg_current_wal_lsn()")
info "LSN после изменений: ${CHANGES_LSN}"
SLOT_LAG=$(pgq "SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))
                FROM pg_replication_slots WHERE slot_name='$CDC_SLOT'")
ok "слот удерживает WAL: отставание ${SLOT_LAG} (никто ещё не читал)"
CNT_BEFORE_CONNECTOR=$(topic_count "$CDC_TOPIC")
[ "$CNT_BEFORE_CONNECTOR" = "0" ] || fail "в топике ${CNT_BEFORE_CONNECTOR} сообщений, а коннектора нет"
ok "в ${CDC_TOPIC} по-прежнему 0 сообщений — писать в него некому"

# =============================================================================
step "ТОЛЬКО ТЕПЕРЬ поднять коннектор (snapshot.mode=no_data)"
# =============================================================================
./scripts/create-connector.sh slim || fail "коннектор не поднялся"

# =============================================================================
step "ГЛАВНАЯ ПРОВЕРКА: изменения шага 4 приехали в топик"
# =============================================================================
info "то есть сделанные ПОСЛЕ создания слота, но ДО старта коннектора"
GOT=$(wait_count_at_least "$CDC_TOPIC" 1 90) || fail "в ${CDC_TOPIC} не появилось ни одного сообщения"
GOT=$(wait_settled "$CDC_TOPIC")
ok "в ${CDC_TOPIC} приехало сообщений: ${GOT}"
[ "$GOT" -ge 30 ] || fail "сообщений слишком мало (${GOT}): изменения шага 4, похоже, потеряны"

head2 "первые события в топике — из шага 4"
avro_consume "$CDC_TOPIC" 3 --property print.offset=true \
    | cut -c1-190 | sed 's/^/      /'

MIN_LSN=$(avro_consume "$CDC_TOPIC" 1 | head -1 | JQ -r '.__lsn.long // .__lsn // 0')
info "минимальный __lsn в топике: ${MIN_LSN}"
info "LSN до создания слота:      ${LSN_BEFORE_NUM}"
case "${MIN_LSN:-}" in ''|*[!0-9]*) fail "не удалось прочитать __lsn первого события (получено: '${MIN_LSN}')";; esac
if [ "$MIN_LSN" -gt "$LSN_BEFORE_NUM" ]; then
    ok "все события НОВЕЕ точки создания слота — граница проходит по слоту"
else
    fail "в топике есть событие с LSN ${MIN_LSN} <= ${LSN_BEFORE_NUM}: попали изменения, сделанные ДО слота"
fi

# =============================================================================
step "ПРОВЕРКА: исходных данных из 05-seed.sql в топике НЕТ"
# =============================================================================
info "они были записаны до создания слота и синхронизированы в (Б) отдельно"
RAW="$(mktemp)"; trap 'rm -f "$RAW"' EXIT
CNT=$(topic_count "$CDC_TOPIC")
avro_consume "$CDC_TOPIC" "$CNT" \
    --property print.key=true --property "key.separator=|@#@|" > "$RAW"

SEED_HITS=$(awk -F'\\|@#@\\|' '{print $1}' "$RAW" \
            | grep -cE '"code":"(USD|EUR|RUB|JPY)"' || true)
if [ "${SEED_HITS:-0}" = "0" ]; then
    ok "событий по seed-строкам ref.currencies (USD/EUR/RUB/JPY) в топике нет"
else
    printf '      найдено %s событий по seed-валютам\n' "$SEED_HITS" >&2
    fail "seed-данные попали в топик — значит слот был создан раньше заливки"
fi
# ВАЖНО про критерий: искать в топике сами seed-строки нельзя. Live-нагрузка
# делает UPDATE по seed-строкам catalog.products (id 1..4), и в событии
# приезжает вся строка, включая неизменённую колонку description. Это
# ЗАКОННОЕ событие изменения — оно и должно быть в потоке.
# Отсутствовать должны именно события ВСТАВКИ (op=c) по seed-строкам: их
# наличие означало бы, что коннектор вычитал начальное состояние (снапшот)
# или что слот создали раньше заливки.
SEED_INS=$(grep -E '"начальный ассортимент"' "$RAW" | grep -c '"__op":{"string":"c"}' || true)
SEED_UPD=$(grep -E '"начальный ассортимент"' "$RAW" | grep -c '"__op":{"string":"u"}' || true)
info "события по seed-строкам catalog.products: вставок ${SEED_INS}, обновлений ${SEED_UPD}"
if [ "${SEED_INS:-0}" = "0" ]; then
    ok "событий ВСТАВКИ по seed-строкам нет — начальное состояние в топик не попало"
    info "обновления (${SEED_UPD}) — это live-изменения тех же строк, они и должны быть в потоке"
else
    fail "в топике ${SEED_INS} событий op=c по seed-строкам: сработал снапшот или слот создан раньше заливки"
fi

# =============================================================================
step "Live-нагрузка: поток непрерывен, LSN монотонен"
# =============================================================================
BEFORE_LIVE=$(topic_count "$CDC_TOPIC")
./scripts/load.sh mixed 6 >/dev/null
./scripts/load.sh fk-chain 2 >/dev/null
AFTER_LIVE=$(wait_settled "$CDC_TOPIC")
info "было ${BEFORE_LIVE}, стало ${AFTER_LIVE} (+$((AFTER_LIVE-BEFORE_LIVE)))"
[ "$AFTER_LIVE" -gt "$BEFORE_LIVE" ] || fail "поток не идёт: число сообщений не выросло"
ok "поток идёт непрерывно"
./scripts/verify-order.sh "$CDC_TOPIC" 0 >/dev/null || fail "verify-order.sh не прошёл под live-нагрузкой"
ok "verify-order.sh: LSN монотонен, дубликатов нет, всё в партиции 0"

CONFIRMED=$(pgq "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='$CDC_SLOT'")
info "confirmed_flush_lsn слота: ${CONFIRMED} (коннектор подтверждает вычитанное)"

# =============================================================================
step "Остановить коннектор, продолжая писать в БД. Поднять обратно"
# =============================================================================
BEFORE_PAUSE=$(topic_count "$CDC_TOPIC")
MAX_ID_BEFORE=$(pgq "SELECT coalesce(max(id),0) FROM sales.order_items")
info "останавливаю контейнер connect (проверяем и сохранность offset'ов, и удержание WAL слотом)"
docker compose stop connect >/dev/null
ok "connect остановлен, в топике ${BEFORE_PAUSE} сообщений"

info "пишу в БД ${PAUSE_SECONDS} секунд, пока CDC стоит"
./scripts/load.sh mixed 5 >/dev/null
./scripts/load.sh fk-chain 2 >/dev/null
MAX_ID_AFTER=$(pgq "SELECT coalesce(max(id),0) FROM sales.order_items")
WROTE=$((MAX_ID_AFTER-MAX_ID_BEFORE))
info "за паузу добавлено строк в sales.order_items: ${WROTE} (id ${MAX_ID_BEFORE}..${MAX_ID_AFTER})"
DURING=$(topic_count "$CDC_TOPIC")
[ "$DURING" = "$BEFORE_PAUSE" ] || fail "топик рос при остановленном коннекторе: ${BEFORE_PAUSE} -> ${DURING}"
ok "топик не менялся, пока коннектор стоял"
head2 "слот удерживает WAL за время паузы"
pgm -c "SELECT slot_name, active,
               pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS \"не вычитано\"
        FROM pg_replication_slots WHERE slot_name='$CDC_SLOT';"
sleep "$PAUSE_SECONDS"

info "поднимаю connect"
docker compose start connect >/dev/null
wait_for "connect healthy" 180 bash -c '[ "$(docker compose ps connect --format "{{.Health}}")" = healthy ]' \
    || fail "connect не поднялся обратно"
wait_connector_running "$CONNECTOR_NAME" 120 || fail "коннектор не вернулся в RUNNING"
AFTER_RESUME=$(wait_settled "$CDC_TOPIC")
info "в топике стало ${AFTER_RESUME} (+$((AFTER_RESUME-BEFORE_PAUSE)) за паузу)"
[ "$AFTER_RESUME" -gt "$BEFORE_PAUSE" ] || fail "изменения за паузу НЕ доехали"

head2 "проверка, что за паузу не потеряно ни одной строки"
# Ищем в топике события по КАЖДОМУ id из диапазона, записанного во время паузы.
CNT=$(topic_count "$CDC_TOPIC")
avro_consume "$CDC_TOPIC" "$CNT" \
    --property print.key=true --property "key.separator=|@#@|" > "$RAW"
MISSING=0
MISSING_IDS=""
if [ "$WROTE" -le 0 ]; then
    fail "за паузу не записано ни одной строки в sales.order_items — проверять нечего"
fi
for id in $(seq $((MAX_ID_BEFORE+1)) "$MAX_ID_AFTER"); do
    # Ключ события — PK: {"id":N}. Требуем, чтобы в этой же строке значение
    # ссылалось на order_items: ключ {"id":N} есть и у других таблиц.
    if ! grep -qE "^\{\"id\":${id}\}\|@#@\|.*\"order_items\"" "$RAW"; then
        MISSING=$((MISSING+1)); MISSING_IDS="${MISSING_IDS} ${id}"
    fi
done
if [ "$MISSING" = "0" ]; then
    ok "все ${WROTE} строк sales.order_items, записанные за паузу, есть в топике"
else
    printf '      отсутствуют id:%s\n' "$MISSING_IDS" >&2
    fail "не найдено событий для ${MISSING} строк из ${WROTE}, записанных за паузу"
fi
./scripts/verify-order.sh "$CDC_TOPIC" 0 >/dev/null || fail "после паузы порядок нарушен"
ok "порядок после паузы не нарушен"

# =============================================================================
step "Порядок под сбоем: перезапуск брокера во время активной записи"
# =============================================================================
info "это единственный способ показать связку idempotence+acks+max.in.flight в работе"
BEFORE_CRASH=$(topic_count "$CDC_TOPIC")
( ./scripts/load.sh mixed 25 >/dev/null 2>&1 ) &
LOAD_PID=$!
sleep 3
info "перезапускаю брокер kafka, пока идёт запись в БД"
docker compose restart kafka >/dev/null
wait_for "kafka healthy" 180 bash -c '[ "$(docker compose ps kafka --format "{{.Health}}")" = healthy ]' \
    || fail "kafka не поднялась"
ok "брокер поднялся"
wait "$LOAD_PID" 2>/dev/null || true
info "нагрузка завершена, ждём восстановления коннектора"
wait_connector_running "$CONNECTOR_NAME" 180 || fail "коннектор не восстановился после перезапуска брокера"
AFTER_CRASH=$(wait_settled "$CDC_TOPIC")
info "в топике: ${BEFORE_CRASH} -> ${AFTER_CRASH}"
[ "$AFTER_CRASH" -gt "$BEFORE_CRASH" ] || fail "после перезапуска брокера поток не возобновился"
ok "поток возобновился без вмешательства"

head2 "ГЛАВНОЕ: порядок и отсутствие дубликатов ПОСЛЕ СБОЯ"
./scripts/verify-order.sh "$CDC_TOPIC" 0 || fail "после перезапуска брокера порядок или уникальность LSN нарушены"

# =============================================================================
step "Итог"
# =============================================================================
pgm -c "SELECT slot_name, failover, synced, active, restart_lsn, confirmed_flush_lsn
        FROM pg_replication_slots ORDER BY slot_type, slot_name;"
info "сообщений в ${CDC_TOPIC}: $(topic_count "$CDC_TOPIC")"
info "сообщений в ${TOPIC_PREFIX}.transaction: $(topic_count "${TOPIC_PREFIX}.transaction")"
info "сообщений в __debezium-heartbeat.${TOPIC_PREFIX}: $(topic_count "__debezium-heartbeat.${TOPIC_PREFIX}")"
printf '\n%sСЦЕНАРИЙ ПРОЙДЕН ЦЕЛИКОМ%s\n' "$C_G" "$C_RESET"
