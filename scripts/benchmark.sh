#!/usr/bin/env bash
# =============================================================================
#  benchmark.sh — замер ОБЪЁМА и ПРОПУСКНОЙ СПОСОБНОСТИ.
#
#  Отвечает на два вопроса, от которых зависит применимость всей архитектуры:
#    1. сколько байт стоит одно событие в профилях slim и full (и что даёт
#       компрессия);
#    2. сколько событий в секунду проходит через ОДНУ ПАРТИЦИЮ — и хватает ли
#       этого на суточный объём.
#
#  МЕТОДИКА (важно, иначе цифры ничего не значат).
#  Мерить «пишем в БД и смотрим, как быстро появляются сообщения» нельзя:
#  получится скорость записи в PostgreSQL, а не пропускная способность
#  конвейера. Поэтому замер строится на РАЗБОРЕ НАКОПЛЕННОГО ОТСТАВАНИЯ:
#
#    1) коннектор ставится на паузу (слот продолжает удерживать WAL);
#    2) в БД записывается N изменений — они копятся в WAL;
#    3) коннектор снимается с паузы и разбирает отставание на максимальной
#       скорости;
#    4) время считается по ТАЙМСТЕМПАМ СООБЩЕНИЙ В KAFKA (первое и последнее
#       из партии), а не по wall-clock скрипта.
#
#  Объём считается тремя числами:
#    - сумма размеров сериализованных сообщений (kcat -f '%S %K') —
#      это то, что видит consumer;
#    - размер партиции на диске брокера (kafka-log-dirs) — это то, что
#      реально занято ПОСЛЕ компрессии zstd и с накладными расходами батчей;
#    - их отношение = фактический коэффициент сжатия на этом потоке.
#
#  Профили сравниваются на ОДНИХ И ТЕХ ЖЕ изменениях: слот профиля full
#  создаётся ДО нагрузки, поэтому оба коннектора видят одинаковый поток.
#
#  Параметры (env):
#    BENCH_ROWS=2000  строк в одной транзакции
#    BENCH_TXS=10     транзакций (всего событий = BENCH_ROWS * BENCH_TXS)
#    BENCH_LINGER="0 50 200"  значения linger.ms для подбора (пусто = не гонять)
# =============================================================================
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

BENCH_ROWS="${BENCH_ROWS:-5000}"
BENCH_TXS="${BENCH_TXS:-20}"
# Повторы: окно замера на 20 тыс. событий получалось 1-1.5 с, и на таком окне
# разница между max.in.flight=1 и 5 тонет в разбросе. Меряем каждую
# конфигурацию несколько раз и смотрим на разброс, а не на одно число.
BENCH_REPEATS="${BENCH_REPEATS:-2}"
EVENTS=$((BENCH_ROWS*BENCH_TXS))
REPORT="$(mktemp)"

fmt_bytes() { awk -v b="$1" 'BEGIN{ if(b>=1048576) printf "%.1f МБ", b/1048576; else if(b>=1024) printf "%.1f КБ", b/1024; else printf "%d Б", b }'; }

# Размер партиции топика на диске брокера (после компрессии)
partition_bytes() {
    docker compose exec -T kafka kafka-log-dirs --bootstrap-server "$KAFKA_INTERNAL" \
        --describe --topic-list "$1" 2>/dev/null | grep '^{' \
        | JQ -r --arg p "${1}-0" '[.brokers[].logDirs[].partitions[] | select(.partition==$p) | .size] | add // 0'
}

pause_connector()  { capi PUT "/connectors/$1/pause"  >/dev/null 2>&1 || true; }
resume_connector() { capi PUT "/connectors/$1/resume" >/dev/null 2>&1 || true; }
wait_state() {
    local name="$1" want="$2" t=0
    while [ "$(connector_state "$name")" != "$want" ]; do
        t=$((t+1)); [ "$t" -ge 60 ] && return 1; sleep 1
    done
}

# measure <имя коннектора> <топик> <подпись>
# Печатает строку отчёта: подпись|события|сек|событий/с|байт_wire|байт_диск|средний_размер
measure() {
    local name="$1" topic="$2" label="$3"
    head2 "замер: ${label}"

    pause_connector "$name"
    wait_state "$name" PAUSED || warn "коннектор ${name} не встал в PAUSED (состояние: $(connector_state "$name"))"
    local w0 d0
    w0=$(topic_count "$topic"); d0=$(partition_bytes "$topic")
    info "коннектор на паузе; в топике ${w0} сообщений, на диске $(fmt_bytes "$d0")"

    info "пишу ${EVENTS} изменений в БД (${BENCH_TXS} транзакций x ${BENCH_ROWS} строк)"
    local t_db0 t_db1
    t_db0=$(date +%s)
    ./scripts/load.sh bulk "$BENCH_ROWS" "$BENCH_TXS" >/dev/null
    t_db1=$(date +%s)
    info "запись в PostgreSQL заняла $((t_db1-t_db0)) с (это НЕ измеряемая величина)"
    info "слот удерживает: $(pgq "SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) FROM pg_replication_slots WHERE slot_name = (SELECT CASE WHEN '$name' LIKE '%-full' THEN '$CDC_SLOT_FULL' ELSE '$CDC_SLOT' END)")"

    info "снимаю с паузы, разбираю отставание"
    resume_connector "$name"
    wait_state "$name" RUNNING || fail "коннектор ${name} не вернулся в RUNNING"

    # Ждём, пока high watermark дойдёт до w0+EVENTS (или перестанет расти)
    local target=$((w0+EVENTS)) prev=-1 cur stall=0
    while :; do
        cur=$(topic_count "$topic")
        [ "$cur" -ge "$target" ] && break
        if [ "$cur" = "$prev" ]; then
            stall=$((stall+1))
            [ "$stall" -ge 15 ] && { warn "поток встал на ${cur} из ${target}"; break; }
        else stall=0; fi
        prev="$cur"; sleep 2
    done
    local w1 d1 got
    w1=$(topic_count "$topic"); d1=$(partition_bytes "$topic"); got=$((w1-w0))
    [ "$got" -gt 0 ] || fail "не приехало ни одного сообщения"

    # Время — по таймстемпам сообщений в Kafka. kcat здесь надёжен: он не
    # десериализует значение, только читает метаданные и размеры.
    local stats
    stats=$(kc -b "$KAFKA_INTERNAL" -t "$topic" -p 0 -o "$w0" -c "$got" -C -e \
              -f '%T %S %K\n' 2>/dev/null \
            | awk 'NR==1{min=$1} {if($1<min)min=$1; if($1>max)max=$1; s+=$2; k+=$3; n++}
                   END{printf "%d %d %d %d", (max-min), s, k, n}')
    local span_ms wire_bytes key_bytes n
    read -r span_ms wire_bytes key_bytes n <<<"$stats"
    [ "${n:-0}" -gt 0 ] || fail "kcat не смог прочитать партию сообщений"
    local disk_delta=$((d1-d0))
    awk -v label="$label" -v n="$n" -v span="$span_ms" -v wire="$wire_bytes" \
        -v keyb="$key_bytes" -v disk="$disk_delta" 'BEGIN{
        sec = (span>0 ? span/1000.0 : 0.001);
        printf "%s|%d|%.2f|%.0f|%d|%d|%.1f|%.1f|%.2f\n",
               label, n, sec, n/sec, wire+keyb, disk, (wire+keyb)/n, disk/n,
               (disk>0 ? (wire+keyb)/disk : 0);
    }' >> "$REPORT"

    info "разобрано ${n} сообщений за $(awk -v s="$span_ms" 'BEGIN{printf "%.2f", s/1000}') с"
    info "wire (сериализованные сообщения): $(fmt_bytes $((wire_bytes+key_bytes)))"
    info "на диске брокера (после zstd):    $(fmt_bytes "$disk_delta")"
    ok "${label}: $(awk -v n="$n" -v s="$span_ms" 'BEGIN{printf "%.0f", n/(s>0?s/1000:0.001)}') событий/с"
}

# =============================================================================
step "Подготовка: слот и топики профиля full создаются ДО нагрузки"
# =============================================================================
# Иначе профили увидят разные изменения и сравнение объёмов будет ложью.
# Слот создаём вручную под postgres — Debezium слотов не создаёт и здесь.
if [ "$(pgq "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$CDC_SLOT_FULL'")" = "0" ]; then
    pgq "SELECT pg_create_logical_replication_slot('$CDC_SLOT_FULL','pgoutput',false,false,true)" >/dev/null
    ok "слот ${CDC_SLOT_FULL} создан (failover=true)"
else
    ok "слот ${CDC_SLOT_FULL} уже есть"
fi
# Топики профиля full: auto-create выключен, поэтому создаём заранее.
for t in "$CDC_TOPIC_FULL" "${TOPIC_PREFIX_FULL}.transaction" "__debezium-heartbeat.${TOPIC_PREFIX_FULL}"; do
    if ktopics --describe --topic "$t" >/dev/null 2>&1; then
        info "топик ${t} уже есть"
    else
        ktopics --create --topic "$t" --partitions 1 --replication-factor 1 \
            --config cleanup.policy=delete --config retention.ms=604800000 \
            --config compression.type=producer >/dev/null
        ok "создан топик ${t} (1 партиция, delete)"
    fi
done

capi_code "/connectors/${CONNECTOR_NAME}" | grep -q 200 || ./scripts/create-connector.sh slim >/dev/null
ok "коннектор slim на месте"

# =============================================================================
step "Замер 1: профиль slim, MAX_IN_FLIGHT=${MAX_IN_FLIGHT}"
# =============================================================================
for r in $(seq 1 "$BENCH_REPEATS"); do
    measure "$CONNECTOR_NAME" "$CDC_TOPIC" "slim, mif=${MAX_IN_FLIGHT}, прогон ${r}"
done

# =============================================================================
step "Замер 2: профиль slim, MAX_IN_FLIGHT=1"
# =============================================================================
info "решение о значении принимается по цифрам, а не по ощущению «1 безопаснее»:"
info "при enable.idempotence=true гарантии порядка на 1 и на 5 ОДИНАКОВЫ"
CFG=$(capi GET "/connectors/${CONNECTOR_NAME}/config")
# ГРАБЛИ: конструкция  printf '%s' "$X" | { read -r c; ... }  ломается под
# set -e: без завершающего перевода строки read возвращает 1 и убивает
# подоболочку пайплайна. Поэтому — обычная подстановка команд.
CFG_MIF1=$(printf '%s' "$CFG" | JQ -c '. + {"producer.override.max.in.flight.requests.per.connection":"1"}')
capi PUT "/connectors/${CONNECTOR_NAME}/config" "$CFG_MIF1" >/dev/null
wait_connector_running "$CONNECTOR_NAME" 60 || fail "коннектор не поднялся с max.in.flight=1"
ok "конфиг обновлён: max.in.flight=1"
for r in $(seq 1 "$BENCH_REPEATS"); do
    measure "$CONNECTOR_NAME" "$CDC_TOPIC" "slim, mif=1, прогон ${r}"
done
capi PUT "/connectors/${CONNECTOR_NAME}/config" "$CFG" >/dev/null
wait_connector_running "$CONNECTOR_NAME" 60 || fail "коннектор не вернулся к исходному конфигу"
ok "конфиг возвращён: max.in.flight=${MAX_IN_FLIGHT}"

# =============================================================================
if [ -n "${BENCH_LINGER:-}" ]; then
step "Подбор linger.ms"
# =============================================================================
# zstd жмёт БАТЧ, а не отдельное сообщение. Без linger.ms producer отправляет
# батчи по мере готовности, они получаются мелкими, и компрессия работает
# вполсилы. Смотрим на «Б/диск»: это и есть эффект батчинга.
CFG=$(capi GET "/connectors/${CONNECTOR_NAME}/config")
for L in $BENCH_LINGER; do
    CFG_L=$(printf '%s' "$CFG" | JQ -c --arg l "$L" '. + {"producer.override.linger.ms":$l}')
    capi PUT "/connectors/${CONNECTOR_NAME}/config" "$CFG_L" >/dev/null
    wait_connector_running "$CONNECTOR_NAME" 60 || fail "коннектор не поднялся с linger.ms=${L}"
    measure "$CONNECTOR_NAME" "$CDC_TOPIC" "slim, linger.ms=${L}"
done
capi PUT "/connectors/${CONNECTOR_NAME}/config" "$CFG" >/dev/null
wait_connector_running "$CONNECTOR_NAME" 60 || fail "коннектор не вернулся к исходному конфигу"
ok "linger.ms возвращён к ${PRODUCER_LINGER_MS}"
fi

# =============================================================================
step "Замер 3: профиль full — полный конверт Debezium"
# =============================================================================
./scripts/create-connector.sh full >/dev/null || fail "коннектор профиля full не поднялся"
ok "коннектор ${CONNECTOR_NAME}-full поднят на слоте ${CDC_SLOT_FULL}"

# ВАЖНО ДЛЯ КОРРЕКТНОСТИ ЗАМЕРА: слот профиля full создан до нагрузки и к
# этому моменту накопил ВСЁ отставание (все прогоны замеров slim). Если начать
# мерить сразу, в окно попадут сотни тысяч чужих событий, и сравнение скоростей
# будет ложью: у full окажется больше событий, крупнее батчи и выше скорость.
# Поэтому сначала даём ему разобрать накопленное, и только потом мерим
# одинаковый по объёму цикл.
info "даю профилю full разобрать накопленное отставание (слот создан до нагрузки)"
prev=-1; stall=0
while :; do
    cur=$(topic_count "$CDC_TOPIC_FULL")
    if [ "$cur" = "$prev" ]; then
        stall=$((stall+1)); [ "$stall" -ge 5 ] && break
    else stall=0; fi
    prev="$cur"; sleep 3
done
ok "отставание разобрано, в ${CDC_TOPIC_FULL} сейчас ${prev} сообщений"

measure "${CONNECTOR_NAME}-full" "$CDC_TOPIC_FULL" "full, mif=${MAX_IN_FLIGHT}"

# =============================================================================
step "Сравнение размеров сообщений на ОДНОМ событии"
# =============================================================================
head2 "профиль slim (плоская строка + 6 служебных полей)"
avro_consume "$CDC_TOPIC" 1 | head -1 | cut -c1-400 | sed 's/^/    /'
head2 "профиль full (before + after + source + op + ts_ms + transaction)"
avro_consume "$CDC_TOPIC_FULL" 1 | head -1 | cut -c1-700 | sed 's/^/    /'

# =============================================================================
step "ИТОГОВЫЙ ОТЧЁТ"
# =============================================================================
printf '\n  %-28s %8s %8s %10s %12s %12s %9s %9s %6s\n' \
    "конфигурация" "события" "сек" "событий/с" "wire" "на диске" "Б/сообщ" "Б/диск" "сжатие"
printf '  %s\n' "$(printf '%.0s-' {1..120})"
while IFS='|' read -r label n sec rate wire disk avg avgd ratio; do
    printf '  %-28s %8d %8.1f %10d %12s %12s %9.1f %9.1f %5.1fx\n' \
        "$label" "$n" "$sec" "$rate" "$(fmt_bytes "$wire")" "$(fmt_bytes "$disk")" "$avg" "$avgd" "$ratio"
done < "$REPORT"

head2 "экстраполяция на сутки (по лучшей измеренной скорости)"
# Экстраполируем по ОСНОВНОМУ профилю (slim), а не по лучшей цифре из всех:
# профиль full — только для сравнения объёма, в контур пойдёт slim.
BEST=$(awk -F'|' 'BEGIN{m=0} /^slim,/ {if ($4>m) m=$4} END{printf "%.0f", m}' "$REPORT")
FULL_RATE=$(awk -F'|' '/^full,/ {printf "%.0f", $4; exit}' "$REPORT")
FULL_DISK=$(awk -F'|' '/^full,/ {print $8; exit}' "$REPORT")
SLIM_DISK=$(awk -F'|' '/^slim, mif=[0-9]+,/ {print $8; exit}' "$REPORT")
awk -v rate="$BEST" -v perdisk="${SLIM_DISK:-0}" -v frate="${FULL_RATE:-0}" -v fdisk="${FULL_DISK:-0}" 'BEGIN{
    day = rate*86400;
    printf "    пропускная способность одной партиции (профиль slim): %d событий/с\n", rate;
    printf "    за сутки при этой скорости:             %.1f млн событий\n", day/1e6;
    printf "    объём на диске за сутки (slim):         %.1f ГБ\n", day*perdisk/1024/1024/1024;
    if (fdisk>0) printf "    то же на профиле full:                  %.1f ГБ (в %.1f раза больше)\n", day*fdisk/1024/1024/1024, fdisk/perdisk;
    printf "\n    ЧТО ЭТО ЗНАЧИТ: полная синхронизация должна укладываться в сутки.\n";
    printf "    Значит суточный объём изменений в (А) не должен превышать %.1f млн событий.\n", day/1e6;
    printf "    Если фактический объём выше — архитектура с ОДНОЙ партицией не подходит,\n";
    printf "    и это не настраивается: поднять предел, не сломав инвариант №1, нельзя.\n";
}'

head2 "оговорки к цифрам"
info "1. Замер сделан на одном брокере и одном хосте с PostgreSQL в контейнерах:"
info "   это НИЖНЯЯ оценка. На выделенном кластере число будет выше, но порядок тот же."
info "2. Нагрузка — INSERT в одну таблицу (sales.order_items). Реальный поток из"
info "   500 таблиц даст больше разных Avro-схем и чуть худшую компрессию."
info "3. Числа в README, раздел 5 — из этого запуска; при переносе на контур перемерить."
rm -f "$REPORT"
