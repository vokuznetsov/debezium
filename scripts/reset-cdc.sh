#!/usr/bin/env bash
# =============================================================================
#  reset-cdc.sh — сброс ТОЛЬКО состояния CDC при живых данных.
#
#  Зачем отдельная операция: состояние PostgreSQL и Kafka описывает ОДНУ точку
#  (подтверждённый LSN в слоте + offset'ы в connect-offsets). Сбросить их
#  по отдельности нельзя (см. README, раздел 3), а `make clean` сносит и
#  данные БД. Эта операция сбрасывает согласованно ровно то, что относится к
#  CDC, и оставляет данные:
#
#    1. удалить коннектор через REST;
#    2. остановить воркер Connect (иначе он держит connect-offsets);
#    3. дропнуть и заново создать логический слот — С failover => true;
#    4. пересоздать cdc.events и connect-offsets;
#    5. поднять воркер.
#
#  ПОСЛЕ СБРОСА состояние приёмника (Б) СТАНОВИТСЯ НЕСОГЛАСОВАННЫМ с (А):
#  новый слот начинает с текущего LSN, а изменения между старым
#  подтверждённым LSN и новым потеряны безвозвратно. Значит после reset-cdc
#  базы нужно синхронизировать заново любым внешним способом — ровно как в
#  момент T. Скрипт об этом предупреждает и требует подтверждения.
# =============================================================================
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

printf '\n%sreset-cdc сбросит состояние CDC:%s\n' "$C_R" "$C_RESET"
printf '  - коннектор %s будет удалён\n' "$CONNECTOR_NAME"
printf '  - слот %s будет ПЕРЕСОЗДАН (позиция чтения потеряется)\n' "$CDC_SLOT"
printf '  - топики %s и connect-offsets будут пересозданы\n' "$CDC_TOPIC"
printf '  - данные в PostgreSQL ОСТАНУТСЯ\n\n'
printf '%sПосле этого (Б) придётся синхронизировать с (А) заново: новый слот\n' "$C_Y"
printf 'начнёт с текущего LSN, изменения за время сброса не восстановимы.%s\n\n' "$C_RESET"
if [ "${RESET_CDC_FORCE:-}" != "1" ]; then
    read -r -p 'Продолжить? [yes/NO] ' a
    [ "$a" = "yes" ] || { echo "отменено"; exit 1; }
fi

step "Удаляю коннектор"
CODE=$(capi_code "/connectors/${CONNECTOR_NAME}")
if [ "$CODE" = "200" ]; then
    capi DELETE "/connectors/${CONNECTOR_NAME}" >/dev/null || true
    ok "коннектор ${CONNECTOR_NAME} удалён"
else
    info "коннектора ${CONNECTOR_NAME} нет (HTTP ${CODE})"
fi

step "Останавливаю воркер Connect"
docker compose stop connect >/dev/null
ok "connect остановлен (он держал connect-offsets)"

step "Пересоздаю логический слот с failover => true"
pgm -c "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn
        FROM pg_replication_slots WHERE slot_name='$CDC_SLOT';"
if [ "$(pgq "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$CDC_SLOT'")" != "0" ]; then
    # Слот нельзя дропнуть, пока он активен; коннектор удалён, поэтому ждём
    # освобождения, а не спим наугад.
    for _ in $(seq 1 30); do
        [ "$(pgq "SELECT active FROM pg_replication_slots WHERE slot_name='$CDC_SLOT'")" = "f" ] && break
        sleep 1
    done
    pgq "SELECT pg_drop_replication_slot('$CDC_SLOT')" >/dev/null || fail "не удалось дропнуть слот (он ещё активен?)"
    ok "слот ${CDC_SLOT} удалён"
fi
pgq "SELECT pg_create_logical_replication_slot('$CDC_SLOT','pgoutput',false,false,true)" >/dev/null \
    || fail "не удалось создать слот"
pgm -c "SELECT slot_name, plugin, failover, restart_lsn FROM pg_replication_slots WHERE slot_name='$CDC_SLOT';"
ok "слот пересоздан с failover=true"

step "Пересоздаю топики ${CDC_TOPIC} и connect-offsets"
for t in "$CDC_TOPIC" connect-offsets; do
    ktopics --delete --topic "$t" >/dev/null 2>&1 || true
    info "удаление ${t} запрошено"
done
for t in "$CDC_TOPIC" connect-offsets; do
    for _ in $(seq 1 60); do
        ktopics --describe --topic "$t" >/dev/null 2>&1 || break
        sleep 1
    done
done
ok "топики удалены"
docker compose up -d kafka-init >/dev/null
wait_for "kafka-init отработал" 120 bash -c \
    '[ "$(docker inspect kafka-init --format "{{.State.Status}}")" = exited ] && [ "$(docker inspect kafka-init --format "{{.State.ExitCode}}")" = 0 ]' \
    || fail "kafka-init не смог пересоздать топики"
ok "топики созданы заново (kafka-init, идемпотентно)"
ktopics --list | sort | sed 's/^/    /'

step "Поднимаю воркер Connect"
docker compose up -d connect >/dev/null
wait_for "connect healthy" 180 bash -c '[ "$(docker compose ps connect --format "{{.Health}}")" = healthy ]' \
    || fail "connect не поднялся"
ok "connect поднят"

printf '\n%sСостояние CDC сброшено.%s Дальше:\n' "$C_G" "$C_RESET"
printf '  1. синхронизировать (Б) с (А) внешним способом — это новый момент T;\n'
printf '  2. make connector — поднять коннектор на новом слоте.\n\n'
