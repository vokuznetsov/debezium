#!/usr/bin/env bash
# =============================================================================
#  verify-infra.sh — проверка ОГРАНИЧЕНИЙ ЭКСПЛУАТАЦИИ.
#
#  Проверяет не «работает ли CDC», а «работает ли он в том режиме, который
#  согласован с эксплуатацией». Все проверки — по ФАКТИЧЕСКОМУ состоянию
#  стенда (pg_replication_slots, REST коннектора, конфиг брокера, лог воркера),
#  а не по файлам в репозитории: расходятся именно они.
#
#  Скрипт собирает все нарушения и выходит с ненулевым кодом, если их больше
#  нуля. Флаг --no-negative отключает активные негативные тесты (они создают
#  и удаляют временный коннектор).
# =============================================================================
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

NEGATIVE=1
[ "${1:-}" = "--no-negative" ] && NEGATIVE=0

# psql при конкатенации через || приводит boolean к тексту как 'true'/'false',
# а в табличном выводе тот же столбец выглядит как t/f. Принимаем оба.
istrue() { case "$1" in t|true|on|1|yes) return 0;; *) return 1;; esac; }

# =============================================================================
step "Слот CDC: существует, создан заранее, failover = true"
# =============================================================================
SLOT_ROW=$(pgq "SELECT slot_type||'|'||coalesce(plugin,'-')||'|'||failover||'|'||active||'|'||coalesce(invalidation_reason,'-')
                FROM pg_replication_slots WHERE slot_name = '$CDC_SLOT'")
[ -n "$SLOT_ROW" ] || fail "логического слота ${CDC_SLOT} нет на мастере. Его должен создать devops (sql/03-slots.sql). Debezium его не создаст — и не должен."
IFS='|' read -r S_TYPE S_PLUGIN S_FAILOVER S_ACTIVE S_INVAL <<<"$SLOT_ROW"
info "тип=${S_TYPE} plugin=${S_PLUGIN} failover=${S_FAILOVER} active=${S_ACTIVE} invalidation=${S_INVAL}"
[ "$S_TYPE" = "logical" ]   || softfail "слот ${CDC_SLOT} не логический"
[ "$S_PLUGIN" = "pgoutput" ] || softfail "плагин слота ${S_PLUGIN}, ожидался pgoutput"
if istrue "$S_FAILOVER"; then
    ok "failover = true — слот синхронизируется на реплики, промоушен не убивает CDC"
else
    softfail "failover = false! Переключение на реплику потеряет слот и потребует полной пересинхронизации. Debezium этот флаг сам не выставляет (DBZ-8412) — значит слот создан не тем скриптом или создан коннектором."
fi
[ "$S_INVAL" = "-" ] || softfail "слот инвалидирован: ${S_INVAL} — CDC мёртв, нужна пересинхронизация"

head2 "кто создавал слот: репликационные команды в логе мастера"
info "(на мастере log_replication_commands = on)"
if logs_have pg-master "CREATE_REPLICATION_SLOT \"?${CDC_SLOT}\"?"; then
    softfail "в логе мастера есть CREATE_REPLICATION_SLOT ${CDC_SLOT} — слот создавал КОННЕКТОР, а не devops. Такой слот получается без failover."
else
    ok "CREATE_REPLICATION_SLOT ${CDC_SLOT} в логе мастера отсутствует — слот создан заранее через SQL"
fi
if logs_have pg-master "START_REPLICATION SLOT \"?${CDC_SLOT}\"?"; then
    ok "START_REPLICATION SLOT ${CDC_SLOT} есть — коннектор только ЧИТАЕТ готовый слот"
else
    warn "START_REPLICATION SLOT ${CDC_SLOT} в логе не найден — коннектор ещё не стартовал?"
fi

# =============================================================================
step "Реплики: слот виден как synced = true и читать его нельзя"
# =============================================================================
for R in pg-replica-1 pg-replica-2; do
    ROW=$(pgq_on "$R" "SELECT synced||'|'||failover||'|'||coalesce(invalidation_reason,'-')
                       FROM pg_replication_slots WHERE slot_name='$CDC_SLOT'" || true)
    if [ -z "$ROW" ]; then
        softfail "${R}: слота ${CDC_SLOT} нет. Проверить sync_replication_slots=on, hot_standby_feedback=on, primary_slot_name и dbname в primary_conninfo."
        continue
    fi
    IFS='|' read -r R_SYNCED R_FAILOVER R_INVAL <<<"$ROW"
    if istrue "$R_SYNCED"; then
        ok "${R}: synced=t failover=${R_FAILOVER} invalidation=${R_INVAL}"
    else
        softfail "${R}: synced=${R_SYNCED} — slotsync-воркер не подхватил слот"
    fi
done

head2 "чтение синхронизированного слота на реплике должно быть ЗАПРЕЩЕНО"
ERR=$(docker compose exec -T pg-replica-1 psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
        -c "SELECT count(*) FROM pg_logical_slot_peek_changes('$CDC_SLOT', NULL, 1);" 2>&1 || true)
# Формулировка отказа зависит от того, что происходит со слотом в этот
# момент, и обе одинаково означают «читать нельзя»:
#   cannot use replication slot "..." for logical decoding
#       (слот помечен как синхронизируемый с primary)
#   replication slot "..." is active for PID N
#       (слот прямо сейчас держит slotsync-воркер реплики)
# Поэтому ожидаем ЛЮБУЮ ошибку, а успешное чтение считаем нарушением.
if printf '%s' "$ERR" | grep -q 'ERROR:'; then
    ok "PostgreSQL отказал, как и ожидалось:"
    printf '%s\n' "$ERR" | grep -E 'ERROR|DETAIL' | sed 's/^/      /'
    info "следствие: разгрузить CDC на реплику НЕВОЗМОЖНО, Debezium ходит только на мастер"
else
    printf '%s\n' "$ERR" | head -5 >&2
    softfail "чтение синхронизированного слота на реплике НЕ было отклонено — проверить, не промоутнута ли реплика"
fi

# =============================================================================
step "Права роли ${DEBEZIUM_USER}: не может создавать слоты и публикации"
# =============================================================================
ATTRS=$(pgq "SELECT rolsuper||'|'||rolcreatedb||'|'||rolcreaterole||'|'||rolreplication
             FROM pg_roles WHERE rolname='$DEBEZIUM_USER'")
IFS='|' read -r A_SUPER A_CREATEDB A_CREATEROLE A_REPL <<<"$ATTRS"
istrue "$A_SUPER"      && softfail "${DEBEZIUM_USER} — СУПЕРПОЛЬЗОВАТЕЛЬ: все ограничения прав бессмысленны"
istrue "$A_CREATEDB"   && softfail "${DEBEZIUM_USER} имеет CREATEDB"
istrue "$A_CREATEROLE" && softfail "${DEBEZIUM_USER} имеет CREATEROLE"
istrue "$A_REPL"       || softfail "${DEBEZIUM_USER} без REPLICATION — поток pgoutput читать не сможет"
ok "атрибуты: super=${A_SUPER} createdb=${A_CREATEDB} createrole=${A_CREATEROLE} replication=${A_REPL}"

head2 "негативная проверка: создание слота через SQL"
ERR=$(pgdbz -c "SELECT pg_create_logical_replication_slot('probe_slot','pgoutput',false,false,true);" 2>&1 || true)
if printf '%s' "$ERR" | grep -q 'permission denied for function'; then
    ok "отказ получен: $(printf '%s' "$ERR" | grep ERROR | head -1)"
else
    softfail "роль ${DEBEZIUM_USER} СМОГЛА создать слот через SQL — отозвать EXECUTE (sql/01-roles.sql)"
    pgq "SELECT pg_drop_replication_slot('probe_slot')" >/dev/null 2>&1 || true
fi

head2 "негативная проверка: создание публикации"
ERR=$(pgdbz -c "CREATE PUBLICATION probe_pub FOR TABLE ref.currencies;" 2>&1 || true)
if printf '%s' "$ERR" | grep -q 'permission denied for database'; then
    ok "отказ получен: $(printf '%s' "$ERR" | grep ERROR | head -1)"
else
    softfail "роль ${DEBEZIUM_USER} СМОГЛА создать публикацию — снять CREATE на базе"
    pgq "DROP PUBLICATION IF EXISTS probe_pub" >/dev/null 2>&1 || true
fi

head2 "ЧЕСТНАЯ ОГОВОРКА про запрет создания слотов"
ERR=$(docker compose exec -T -e PGPASSWORD="$DEBEZIUM_PASSWORD" pg-master \
        psql "host=pg-master user=${DEBEZIUM_USER} dbname=${POSTGRES_DB} replication=database" \
        -c "CREATE_REPLICATION_SLOT probe_proto LOGICAL pgoutput" 2>&1 || true)
if printf '%s' "$ERR" | grep -q 'probe_proto'; then
    warn "роль ${DEBEZIUM_USER} создала слот probe_proto ПО РЕПЛИКАЦИОННОМУ ПРОТОКОЛУ."
    info "Это НЕ дефект стенда, а свойство PostgreSQL: команда CREATE_REPLICATION_SLOT"
    info "проверяет только атрибут REPLICATION, а без него Debezium не прочитает поток."
    info "Отдельного права «читать готовый слот, но не создавать» в PostgreSQL нет."
    info "Поэтому запрет держится на конфиге коннектора (slot.name на готовый слот,"
    info "publication.autocreate.mode=disabled, slot.drop.on.stop=false) и на ревью."
    info "Обратите внимание: созданный так слот получился бы БЕЗ failover:"
    pgq "SELECT '      probe_proto: failover = '||failover FROM pg_replication_slots WHERE slot_name='probe_proto'"
    pgq "SELECT pg_drop_replication_slot('probe_proto')" >/dev/null 2>&1 && info "probe_proto удалён"
else
    ok "протокольный путь тоже закрыт (в этой версии PostgreSQL): $(printf '%s' "$ERR" | grep -i error | head -1)"
fi

# =============================================================================
step "Топики: auto-create выключен, лишних нет, cdc.events не сжимается"
# =============================================================================
BROKER_AC=$(kconfigs --describe --entity-type brokers --entity-name 1 --all 2>/dev/null \
            | grep -o 'auto.create.topics.enable=[a-z]*' | head -1 | cut -d= -f2)
[ "$BROKER_AC" = "false" ] && ok "брокер: auto.create.topics.enable=false" \
    || softfail "брокер: auto.create.topics.enable=${BROKER_AC:-неизвестно} — отсутствующий топик появится сам, с дефолтной политикой очистки"

WORKER_TC=$(docker compose exec -T connect printenv CONNECT_TOPIC_CREATION_ENABLE 2>/dev/null | tr -d '\r' || echo "")
[ "$WORKER_TC" = "false" ] && ok "воркер Connect: topic.creation.enable=false" \
    || softfail "воркер Connect: topic.creation.enable=${WORKER_TC:-не задан}"

head2 "список топиков"
ACTUAL=$(ktopics --list | tr -d '\r' | grep -v '^$' | sort)
printf '%s\n' "$ACTUAL" | sed 's/^/    /'
EXPECTED=$(printf '%s\n' \
    "$CDC_TOPIC" "${TOPIC_PREFIX}.transaction" "__debezium-heartbeat.${TOPIC_PREFIX}" \
    connect-configs connect-offsets connect-status _schemas \
    __consumer_offsets \
    "$CDC_TOPIC_FULL" "${TOPIC_PREFIX_FULL}.transaction" "__debezium-heartbeat.${TOPIC_PREFIX_FULL}" \
    | sort)
EXTRA=$(comm -23 <(printf '%s\n' "$ACTUAL") <(printf '%s\n' "$EXPECTED"))
MISSING=$(comm -13 <(printf '%s\n' "$ACTUAL") <(printf '%s\n' "$EXPECTED") \
          | grep -vE "^(${CDC_TOPIC_FULL}|${TOPIC_PREFIX_FULL}\.transaction|__debezium-heartbeat\.${TOPIC_PREFIX_FULL})$" || true)
[ -z "$EXTRA" ] && ok "лишних топиков нет" \
    || softfail "лишние топики (кто-то создал их в обход kafka-init): $(printf '%s' "$EXTRA" | tr '\n' ' ')"
[ -z "$MISSING" ] && ok "все обязательные топики на месте" \
    || softfail "отсутствуют обязательные топики: $(printf '%s' "$MISSING" | tr '\n' ' ')"
info "(топики профиля full — ${CDC_TOPIC_FULL} и префикс ${TOPIC_PREFIX_FULL} — необязательные, их создаёт benchmark.sh)"

head2 "cdc.events: партиции и политика очистки"
PCOUNT=$(ktopics --describe --topic "$CDC_TOPIC" | awk -F'PartitionCount: ' 'NR==1{split($2,a," ");print a[1]}')
[ "$PCOUNT" = "1" ] && ok "партиций: 1 (инвариант №1)" \
    || softfail "партиций: ${PCOUNT}. Порядок Kafka гарантирует только внутри партиции — инвариант №1 нарушен, и уменьшить число партиций нельзя."
POLICY=$(kconfigs --describe --entity-type topics --entity-name "$CDC_TOPIC" \
         | grep -o 'cleanup.policy=[a-z,]*' | head -1 | cut -d= -f2)
POLICY="${POLICY:-delete(дефолт брокера)}"
case "$POLICY" in
    delete*) ok "cleanup.policy=${POLICY}" ;;
    *) softfail "cleanup.policy=${POLICY}! compaction схлопнет промежуточные версии строк и уничтожит саму идею последовательного применения. Допустимо только delete." ;;
esac

# =============================================================================
step "Конфиг коннектора: ЖЁСТКАЯ проверка связки гарантий порядка"
# =============================================================================
info "источник — REST воркера (GET /connectors/${CONNECTOR_NAME}/config), а не файл в репозитории"
CODE=$(capi_code "/connectors/${CONNECTOR_NAME}/config")
[ "$CODE" = "200" ] || fail "коннектор ${CONNECTOR_NAME} не найден на воркере (HTTP ${CODE}). Сначала make connector."
CFG=$(capi GET "/connectors/${CONNECTOR_NAME}/config")

g() { printf '%s' "$CFG" | JQ -r --arg k "$1" '.[$k] // "ОТСУТСТВУЕТ"'; }
C_IDEMP=$(g 'producer.override.enable.idempotence')
C_ACKS=$(g  'producer.override.acks')
C_MIF=$(g   'producer.override.max.in.flight.requests.per.connection')
C_TASKS=$(g 'tasks.max')
info "enable.idempotence=${C_IDEMP}  acks=${C_ACKS}  max.in.flight=${C_MIF}  tasks.max=${C_TASKS}"

[ "$C_IDEMP" = "true" ] && ok "enable.idempotence=true" \
    || softfail "enable.idempotence=${C_IDEMP}. Без идемпотентности несколько батчей в полёте переставляют события, и отказ ТИХИЙ — до момента, когда consumer части 2 упрётся в foreign key."
[ "$C_ACKS" = "all" ] && ok "acks=all" \
    || softfail "acks=${C_ACKS}. При acks=1 подтверждённая запись может быть потеряна при смене лидера — в потоке будет провал LSN."
if [ "$C_MIF" = "ОТСУТСТВУЕТ" ]; then
    softfail "max.in.flight не задан — берётся дефолт воркера (5)"
elif [ "$C_MIF" -le 5 ] 2>/dev/null; then
    ok "max.in.flight=${C_MIF} (<= 5)"
else
    softfail "max.in.flight=${C_MIF} > 5. При enable.idempotence=true Kafka не запустит producer; при выключенной идемпотентности порядок сломается молча."
fi
if [ "$C_IDEMP" != "true" ] && [ "$C_MIF" != "1" ]; then
    softfail "РАСЦЕПЛЕНА ПАРА: idempotence выключена, а max.in.flight=${C_MIF}. Эти настройки имеют смысл только вместе."
fi
[ "$C_TASKS" = "1" ] && ok "tasks.max=1" || softfail "tasks.max=${C_TASKS}, ожидается 1"

head2 "прочие обязательные свойства"
for pair in "slot.name=$CDC_SLOT" "slot.drop.on.stop=false" \
            "publication.name=$CDC_PUBLICATION" "publication.autocreate.mode=disabled" \
            "plugin.name=pgoutput" "snapshot.mode=no_data" \
            "decimal.handling.mode=precise" "tombstones.on.delete=false"; do
    k="${pair%%=*}"; want="${pair#*=}"; got=$(g "$k")
    [ "$got" = "$want" ] && ok "${k}=${got}" || softfail "${k}=${got}, ожидалось ${want}"
done
HB=$(g 'heartbeat.interval.ms')
[ "$HB" != "ОТСУТСТВУЕТ" ] && [ "$HB" != "0" ] && ok "heartbeat.interval.ms=${HB}" \
    || softfail "heartbeat не настроен: при затишье по отслеживаемым таблицам WAL будет расти до исчерпания диска"

if printf '%s' "$CFG" | JQ -e 'keys[] | select(startswith("topic.creation."))' >/dev/null 2>&1; then
    softfail "в конфиге коннектора есть topic.creation.* — топики создаются заранее, auto-create выключен"
else
    ok "свойств topic.creation.* в конфиге нет"
fi

# =============================================================================
step "producer.override.* ДЕЙСТВИТЕЛЬНО применяется"
# =============================================================================
POLICY_ENV=$(docker compose exec -T connect printenv CONNECT_CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY 2>/dev/null | tr -d '\r' || echo "")
if [ "$POLICY_ENV" = "All" ]; then
    ok "на воркере connector.client.config.override.policy=All"
else
    softfail "connector.client.config.override.policy=${POLICY_ENV:-не задан}. Connect МОЛЧА проигнорирует producer.override.* и возьмёт настройки воркера: конфиг выглядит правильным, гарантий нет."
fi

head2 "фактический ProducerConfig задачи (из лога воркера)"
info "ищем блок ProducerConfig с client.id = connector-producer-${CONNECTOR_NAME}-0"
EFF=$(docker compose logs connect --no-log-prefix 2>&1 | awk -v want="connector-producer-${CONNECTOR_NAME}-0" '
    /ProducerConfig values/ { inblk=1; blk=""; hit=0; next }
    inblk {
        blk = blk $0 "\n"
        if (index($0, "client.id = " want)) hit=1
        if ($0 ~ /ProducerConfig\)/ || $0 ~ /^\[/) { if (hit) last=blk; inblk=0 }
    }
    END { printf "%s", last }')
if [ -z "$EFF" ]; then
    warn "блок ProducerConfig задачи не найден в логе (лог мог быть усечён). Перезапустите задачу: make connector-restart"
else
    printf '%s' "$EFF" | grep -E '^\s+(acks|batch\.size|compression\.type|enable\.idempotence|linger\.ms|max\.in\.flight\.requests\.per\.connection) ' | sed 's/^/      /'
    e() { printf '%s' "$EFF" | grep -E "^\s+$1 = " | head -1 | sed 's/.*= //' | tr -d ' \r'; }
    E_IDEMP=$(e 'enable\.idempotence'); E_ACKS=$(e 'acks'); E_MIF=$(e 'max\.in\.flight\.requests\.per\.connection')
    E_COMP=$(e 'compression\.type');    E_LINGER=$(e 'linger\.ms'); E_BATCH=$(e 'batch\.size')
    # acks=all в логе печатается как -1
    [ "$E_IDEMP" = "true" ] && ok "фактически enable.idempotence=true" || softfail "фактически enable.idempotence=${E_IDEMP} — override НЕ применился"
    { [ "$E_ACKS" = "-1" ] || [ "$E_ACKS" = "all" ]; } && ok "фактически acks=${E_ACKS} (-1 = all)" || softfail "фактически acks=${E_ACKS}"
    [ "$E_MIF" = "$MAX_IN_FLIGHT" ] && ok "фактически max.in.flight=${E_MIF} (= MAX_IN_FLIGHT из .env)" || softfail "фактически max.in.flight=${E_MIF}, в .env ${MAX_IN_FLIGHT}"
    [ "$E_COMP" = "$PRODUCER_COMPRESSION" ] && ok "фактически compression.type=${E_COMP}" || softfail "фактически compression.type=${E_COMP}, ожидался ${PRODUCER_COMPRESSION}"
    [ "$E_LINGER" = "$PRODUCER_LINGER_MS" ] && ok "фактически linger.ms=${E_LINGER}" || softfail "фактически linger.ms=${E_LINGER}, ожидался ${PRODUCER_LINGER_MS}"
    [ "$E_BATCH" = "$PRODUCER_BATCH_SIZE" ] && ok "фактически batch.size=${E_BATCH}" || softfail "фактически batch.size=${E_BATCH}"
    info "совпадение фактических значений с .env и есть доказательство, что override работает"
fi

# =============================================================================
if [ "$NEGATIVE" = "1" ]; then
step "Негативный тест: что РЕАЛЬНО делает коннектор без слота"
# =============================================================================
# ВАЖНЫЙ РЕЗУЛЬТАТ СТЕНДА, И ОН ПРОТИВОРЕЧИТ ОЖИДАНИЮ.
#
# Ожидалось: коннектор со slot.name на несуществующий слот падает с внятной
# ошибкой и слот не создаёт.
# Фактически: Debezium 2.6 СОЗДАЁТ отсутствующий слот и штатно стартует.
#
# Почему так:
#   - publication.autocreate.mode=disabled управляет ТОЛЬКО публикациями,
#     на слоты не влияет; опции «не создавать слот» в Debezium 2.6 нет;
#   - слот создаётся командой CREATE_REPLICATION_SLOT по репликационному
#     протоколу, а она проверяет только атрибут REPLICATION, без которого
#     коннектор не прочитает поток вообще (см. ШАГ 3).
#
# Чем это опасно: созданный Debezium'ом слот получается БЕЗ failover
# (DBZ-8412), то есть при переключении мастера он теряется и CDC требует
# полной пересинхронизации. Никакой ошибки при этом не возникает.
#
# Поэтому единственная надёжная защита — не запрет, а КОНТРОЛЬ СОСТОЯНИЯ:
#   1) ШАГ 1 требует failover=true у рабочего слота;
#   2) проверка ниже требует, чтобы посторонних логических слотов не было.
PROBE="${CONNECTOR_NAME}-probe-noslot"
PROBE_SLOT="probe_noslot_$$"
# GET /connectors/{name}/config возвращает конфиг ВМЕСТЕ с полем name, и PUT
# на другое имя отвечает 400 "Connector name ... doesn't match" — поэтому
# name перебиваем явно.
PROBE_CFG=$(printf '%s' "$CFG" | JQ -c \
    --arg slot "$PROBE_SLOT" --arg name "$PROBE" \
    '. + {"slot.name":$slot, "name":$name, "heartbeat.interval.ms":"0"}')
PUT_RESP=$(capi PUT "/connectors/${PROBE}/config" "$PROBE_CFG" 2>&1 || true)
if printf '%s' "$PUT_RESP" | grep -q '"error_code"'; then
    softfail "не удалось создать временный коннектор: $(printf '%s' "$PUT_RESP" | head -c 200)"
else
    info "создан временный коннектор ${PROBE} со slot.name=${PROBE_SLOT}"
fi

ST=NONE
for _ in $(seq 1 30); do
    ST=$(task_state "$PROBE")
    { [ "$ST" = "FAILED" ] || [ "$ST" = "RUNNING" ]; } && break
    sleep 2
done
CREATED=$(pgq "SELECT count(*) FROM pg_replication_slots WHERE slot_name='${PROBE_SLOT}'")
case "$ST" in
  FAILED)
    TR=$(capi GET "/connectors/${PROBE}/status" | JQ -r '.tasks[0].trace // ""' | head -6)
    ok "задача упала, слот не создан — поведение соответствует ожиданию контура:"
    printf '%s\n' "$TR" | grep -iE 'slot|replication|Exception' | head -3 | sed 's/^/      /'
    ;;
  RUNNING)
    if [ "$CREATED" = "1" ]; then
        FO=$(pgq "SELECT failover FROM pg_replication_slots WHERE slot_name='${PROBE_SLOT}'")
        warn "коннектор НЕ упал: он СОЗДАЛ слот ${PROBE_SLOT} и стартовал (failover=${FO})."
        info "Это подтверждённое поведение Debezium 2.6, а не дефект стенда:"
        info "  запретить создание слота роли с REPLICATION в PostgreSQL нельзя,"
        info "  а опции «не создавать слот» у коннектора нет."
        info "Опасность: такой слот БЕЗ failover, промоушен мастера его потеряет,"
        info "и это никак не проявится до самого переключения."
        info "Защита — контроль состояния (ШАГ 1 и проверка посторонних слотов ниже),"
        info "плюс ревью конфига: slot.name должен указывать на слот из sql/03-slots.sql."
    else
        softfail "задача RUNNING, но слота ${PROBE_SLOT} нет — непонятное состояние, смотреть логи воркера"
    fi
    ;;
  *)
    softfail "задача не пришла ни в RUNNING, ни в FAILED за 60 с (состояние: ${ST})"
    ;;
esac
capi DELETE "/connectors/${PROBE}" >/dev/null 2>&1 || true
sleep 2
pgq "SELECT pg_drop_replication_slot('${PROBE_SLOT}')" >/dev/null 2>&1 \
    && info "временный коннектор и слот ${PROBE_SLOT} удалены" \
    || info "временный коннектор удалён"
fi

# =============================================================================
step "Посторонних логических слотов нет"
# =============================================================================
# Единственная надёжная защита от слотов, созданных коннектором втихую.
# Ожидаемые: рабочий слот CDC и (опционально) слот профиля full для замеров.
STRAY=$(pgq "SELECT string_agg(slot_name, ' ')
             FROM pg_replication_slots
             WHERE slot_type='logical'
               AND slot_name NOT IN ('${CDC_SLOT}', '${CDC_SLOT_FULL}')")
if [ -z "$STRAY" ]; then
    ok "логические слоты только ожидаемые"
    pgm -c "SELECT slot_name, failover, synced, active, restart_lsn
            FROM pg_replication_slots WHERE slot_type='logical' ORDER BY 1;"
else
    softfail "посторонние логические слоты: ${STRAY}. Скорее всего их создал коннектор с неверным slot.name — они БЕЗ failover и удерживают WAL."
fi

finish
