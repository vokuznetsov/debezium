#!/usr/bin/env bash
# =============================================================================
#  test-failover.sh — промоушен реплики и переключение CDC на новый мастер.
#
#  Проверяет то, ради чего слот создаётся с failover => true: что переключение
#  мастера НЕ уничтожает логический слот и не требует полной пересинхронизации
#  CDC.
#
#  Что делает:
#    1. показывает исходное состояние слотов на мастере и обеих репликах;
#    2. фиксирует последний LSN в топике;
#    3. пишет изменения, останавливает мастер (имитация отказа);
#    4. промоутит pg-replica-1 (pg_promote);
#    5. показывает slot_name / synced / failover / invalidation_reason;
#    6. переключает коннектор на новый мастер (database.hostname);
#    7. пишет изменения в НОВЫЙ мастер и проверяет, что поток продолжился
#       БЕЗ РАЗРЫВА LSN и без нарушения монотонности.
#
#  ПОСЛЕ ТЕСТА стенд остаётся в переключённом состоянии: pg-master
#  остановлен, коннектор смотрит на pg-replica-1, pg-replica-2 следует за
#  мёртвым мастером. Вернуть исходную топологию можно только `make clean`:
#  промоутнутый узел обратно в standby без пересборки не превращается.
# =============================================================================
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

NEW_MASTER="${NEW_MASTER:-pg-replica-1}"

slots_everywhere() {
    for node in pg-master pg-replica-1 pg-replica-2; do
        printf '\n    --- %s ---\n' "$node"
        docker compose exec -T "$node" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" --no-psqlrc -c \
          "SELECT slot_name, slot_type, synced, failover, active,
                  coalesce(invalidation_reason,'-') AS invalidation, restart_lsn
           FROM pg_replication_slots ORDER BY slot_type, slot_name;" 2>&1 \
          | sed 's/^/    /' || printf '    (узел недоступен)\n'
    done
}

# __lsn ПОСЛЕДНЕГО события в топике.
# Читаем ровно одно сообщение с offset = high watermark - 1: вычитывать топик
# целиком нельзя, на стенде после benchmark в нём миллионы сообщений и это
# заняло бы минуты.
last_lsn_in_topic() {
    local cnt; cnt=$(topic_count "$CDC_TOPIC")
    [ "${cnt:-0}" -gt 0 ] || { echo 0; return; }
    avro_consume "$CDC_TOPIC" 1 --partition 0 --offset $((cnt-1)) \
        | head -1 | JQ -r '.__lsn.long // .__lsn // 0'
}

# =============================================================================
step "Исходное состояние"
# =============================================================================
wait_connector_running "$CONNECTOR_NAME" 30 || fail "коннектор не в RUNNING — сначала make connector"
info "коннектор смотрит на: $(capi GET "/connectors/${CONNECTOR_NAME}/config" | JQ -r '."database.hostname"')"
head2 "слоты на всех узлах"
slots_everywhere
SYNCED=$(pgq_on "$NEW_MASTER" "SELECT synced FROM pg_replication_slots WHERE slot_name='$CDC_SLOT'")
case "$SYNCED" in t|true) ok "${NEW_MASTER}: слот ${CDC_SLOT} синхронизирован (synced=${SYNCED})";;
                  *) fail "${NEW_MASTER}: слот не синхронизирован (synced=${SYNCED}) — промоушен потеряет CDC";; esac

# =============================================================================
step "Нагрузка до отказа, фиксация последнего LSN в топике"
# =============================================================================
./scripts/load.sh fk-chain 2 >/dev/null
./scripts/load.sh mixed 3 >/dev/null
sleep 5
CNT_BEFORE=$(topic_count "$CDC_TOPIC")
LSN_BEFORE=$(last_lsn_in_topic)
info "сообщений в топике: ${CNT_BEFORE}, последний __lsn: ${LSN_BEFORE}"
case "$LSN_BEFORE" in ''|*[!0-9]*) fail "не удалось прочитать последний __lsn";; esac
CONFIRMED=$(pgq "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='$CDC_SLOT'")
ok "мастер подтвердил вычитку до ${CONFIRMED}"

# =============================================================================
step "Отказ мастера"
# =============================================================================
info "останавливаю pg-master (имитация потери узла)"
docker compose stop pg-master >/dev/null
ok "pg-master остановлен"
info "коннектор сейчас потеряет соединение — это ожидаемо"

# =============================================================================
step "Промоушен ${NEW_MASTER}"
# =============================================================================
pgq_on "$NEW_MASTER" "SELECT pg_promote(wait => true, wait_seconds => 60)" | sed 's/^/    pg_promote: /'
for _ in $(seq 1 30); do
    [ "$(pgq_on "$NEW_MASTER" "SELECT pg_is_in_recovery()")" = "f" ] && break
    sleep 2
done
IN_REC=$(pgq_on "$NEW_MASTER" "SELECT pg_is_in_recovery()")
[ "$IN_REC" = "f" ] || fail "${NEW_MASTER} всё ещё в recovery"
ok "${NEW_MASTER} промоутнут: pg_is_in_recovery() = f"
# Таймлайн берём из имени текущего WAL-файла: pg_control_checkpoint() до
# первого чекпоинта после промоушена ещё показывает СТАРЫЙ таймлайн, и это
# выглядит как «промоушен не произошёл».
info "текущий WAL-файл (первые 8 знаков — таймлайн): $(pgq_on "$NEW_MASTER" "SELECT pg_walfile_name(pg_current_wal_lsn())")"

head2 "СОСТОЯНИЕ СЛОТОВ ПОСЛЕ ПРОМОУШЕНА"
docker compose exec -T "$NEW_MASTER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" --no-psqlrc -c \
  "SELECT slot_name, synced, failover, coalesce(invalidation_reason,'-') AS invalidation_reason,
          active, restart_lsn, confirmed_flush_lsn
   FROM pg_replication_slots ORDER BY slot_name;" | sed 's/^/    /'
INVAL=$(pgq_on "$NEW_MASTER" "SELECT coalesce(invalidation_reason,'-') FROM pg_replication_slots WHERE slot_name='$CDC_SLOT'")
[ "$INVAL" = "-" ] && ok "слот НЕ инвалидирован — его можно использовать дальше" \
                   || fail "слот инвалидирован: ${INVAL}. CDC требует полной пересинхронизации."

head2 "ВАЖНО про synchronized_standby_slots на новом мастере"
info "в pg/replica/postgresql.conf этот параметр СОЗНАТЕЛЬНО не задан."
info "Если бы он был прописан, новый мастер придерживал бы walsender'ы до"
info "подтверждения от слотов replica_1_slot/replica_2_slot, которых на нём нет,"
info "и поток CDC встал бы насовсем без внятной ошибки."
info "В контуре после промоушена его нужно выставить заново под новую топологию:"
info "  ALTER SYSTEM SET synchronized_standby_slots = '<слоты новых реплик>';"
info "  SELECT pg_reload_conf();"

# =============================================================================
step "Переключение коннектора на новый мастер"
# =============================================================================
CFG=$(capi GET "/connectors/${CONNECTOR_NAME}/config")
NEWCFG=$(printf '%s' "$CFG" | JQ -c --arg h "$NEW_MASTER" '. + {"database.hostname":$h}')
capi PUT "/connectors/${CONNECTOR_NAME}/config" "$NEWCFG" >/dev/null
ok "database.hostname = ${NEW_MASTER}"
capi POST "/connectors/${CONNECTOR_NAME}/restart?includeTasks=true&onlyFailed=false" >/dev/null 2>&1 || true
wait_connector_running "$CONNECTOR_NAME" 120 || fail "коннектор не поднялся на новом мастере"
ok "коннектор RUNNING на ${NEW_MASTER}"
docker compose exec -T "$NEW_MASTER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" --no-psqlrc -c \
  "SELECT slot_name, active, active_pid, restart_lsn, confirmed_flush_lsn
   FROM pg_replication_slots WHERE slot_name='$CDC_SLOT';" | sed 's/^/    /'

# =============================================================================
step "Поток продолжился без разрыва LSN"
# =============================================================================
# Нагрузка теперь идёт в НОВЫЙ мастер: pgm ходит в pg-master, поэтому пишем
# напрямую в промоутнутый узел.
info "пишу изменения в новый мастер ${NEW_MASTER}"
docker compose exec -T "$NEW_MASTER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -q --no-psqlrc <<SQL
BEGIN;
INSERT INTO sales.customers (email, name, region_id)
VALUES ('after-failover-' || md5(random()::text) || '@example.com', 'After Failover', 1);
INSERT INTO sales.orders (customer_id, currency_code, total, status)
SELECT id, 'USD', 777.7700, 'after-failover' FROM sales.customers ORDER BY id DESC LIMIT 1;
INSERT INTO sales.order_items (order_id, product_id, qty, price)
SELECT o.id, 1, 1, 777.7700 FROM (SELECT id FROM sales.orders ORDER BY id DESC LIMIT 1) o;
COMMIT;
SQL
prev=-1; for _ in $(seq 1 40); do
    cur=$(topic_count "$CDC_TOPIC"); [ "$cur" = "$prev" ] && [ "$cur" -gt "$CNT_BEFORE" ] && break
    prev="$cur"; sleep 2
done
CNT_AFTER=$(topic_count "$CDC_TOPIC")
LSN_AFTER=$(last_lsn_in_topic)
info "сообщений: ${CNT_BEFORE} -> ${CNT_AFTER}; последний __lsn: ${LSN_BEFORE} -> ${LSN_AFTER}"
[ "$CNT_AFTER" -gt "$CNT_BEFORE" ] || fail "после переключения новых событий нет — поток не продолжился"
ok "новые события приехали: +$((CNT_AFTER-CNT_BEFORE))"
if [ "$LSN_AFTER" -gt "$LSN_BEFORE" ]; then
    ok "LSN продолжил расти через переключение (${LSN_BEFORE} -> ${LSN_AFTER})"
else
    fail "LSN не вырос: ${LSN_BEFORE} -> ${LSN_AFTER}. Похоже на разрыв позиции."
fi

head2 "проверка порядка по ВСЕМУ топику, включая переход через failover"
./scripts/verify-order.sh "$CDC_TOPIC" 0 || fail "после failover монотонность LSN или уникальность нарушены"

# =============================================================================
step "Итог и в каком состоянии остался стенд"
# =============================================================================
head2 "слоты на новом мастере"
docker compose exec -T "$NEW_MASTER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" --no-psqlrc -c \
  "SELECT slot_name, slot_type, synced, failover, active FROM pg_replication_slots ORDER BY 1;" | sed 's/^/    /'
warn "стенд остался в переключённом состоянии:"
info "  - pg-master остановлен;"
info "  - коннектор читает ${NEW_MASTER};"
info "  - pg-replica-2 следует за мёртвым мастером (в контуре её надо перенаправить"
info "    на новый мастер: изменить primary_conninfo и primary_slot_name);"
info "  - synchronized_standby_slots на новом мастере не задан (см. выше)."
info "Вернуть исходную топологию: make clean && make up && make connector."
printf '\n%sFAILOVER-ТЕСТ ПРОЙДЕН%s\n' "$C_G" "$C_RESET"
