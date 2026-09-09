#!/usr/bin/env bash
# =============================================================================
#  kafka-init — одноразовое идемпотентное создание ВСЕХ топиков.
#
#  ОГРАНИЧЕНИЕ КОНТУРА: ни Debezium, ни Kafka Connect, ни Schema Registry не
#  создают топиков. На брокере auto.create.topics.enable=false, на воркере
#  Connect topic.creation.enable=false, в конфиге коннектора нет ни одного
#  свойства topic.creation.*.
#
#  Следствие: ОТСУТСТВУЮЩИЙ ТОПИК = ПАДЕНИЕ СЕРВИСА, а не тихое создание.
#  Поэтому список ниже обязан быть полным, включая служебные топики Connect и
#  Schema Registry. Их настройки (число партиций) обязаны совпадать с
#  переменными воркера CONNECT_*_STORAGE_PARTITIONS, иначе воркер не стартует.
#
#  Единственное исключение — __consumer_offsets: его создаёт координатор
#  групп самого брокера, auto.create.topics.enable на него не влияет.
# =============================================================================
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
CDC_TOPIC="${CDC_TOPIC:-cdc.events}"
TOPIC_PREFIX="${TOPIC_PREFIX:-dbz}"
# 7 суток. Retention на cdc.events — это окно, за которое consumer части 2
# обязан успеть прочитать поток. Меньше ставить опасно: сутки простоя
# consumer'а не должны стоить пересинхронизации.
CDC_RETENTION_MS="${CDC_RETENTION_MS:-604800000}"

log() { echo "[kafka-init] $*"; }

log "ждём брокер ${BOOTSTRAP}"
for i in $(seq 1 60); do
    if kafka-broker-api-versions --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then
        log "брокер отвечает"
        break
    fi
    [ "$i" = 60 ] && { echo "[kafka-init][ОШИБКА] брокер не поднялся" >&2; exit 1; }
    sleep 2
done

# create <топик> <партиций> <config=value>...
create() {
    local topic="$1" parts="$2"; shift 2
    local cfg=()
    for c in "$@"; do cfg+=(--config "$c"); done
    if kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$topic" >/dev/null 2>&1; then
        local have
        have=$(kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$topic" \
               | awk -F'PartitionCount: ' 'NR==1{split($2,a," ");print a[1]}')
        log "топик ${topic} уже есть (партиций: ${have}) — пропуск"
        if [ "$have" != "$parts" ]; then
            echo "[kafka-init][ОШИБКА] у ${topic} партиций ${have}, ожидалось ${parts}." >&2
            echo "  Число партиций уменьшить нельзя. Для cdc.events это нарушение" >&2
            echo "  инварианта №1 — нужен make reset-cdc (или make clean)." >&2
            exit 1
        fi
        return 0
    fi
    log "создаю ${topic}: партиций=${parts}, конфиг: $*"
    kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
        --topic "$topic" --partitions "$parts" --replication-factor 1 "${cfg[@]}"
}

# ---------------------------------------------------------------------------
#  ОСНОВНОЙ ПОТОК CDC
#
#  ОДНА ПАРТИЦИЯ — ЭТО ИНВАРИАНТ №1, А НЕ НАСТРОЙКА. Kafka гарантирует
#  порядок только внутри партиции. Изменения ВСЕХ таблиц ВСЕХ схем должны
#  приезжать consumer'у части 2 строго в порядке WAL, иначе ему придётся
#  самому разбирать порядок foreign key. Добавление партиций необратимо
#  ломает это: --alter --partitions увеличить можно, уменьшить нельзя.
#
#  !!! COMPACTION НА ЭТОМ ТОПИКЕ НЕЛЬЗЯ ВКЛЮЧАТЬ НИ ПРИ КАКИХ УСЛОВИЯХ !!!
#  cleanup.policy=compact оставляет только ПОСЛЕДНЮЮ версию записи по ключу и
#  выбрасывает промежуточные. Для потока CDC это уничтожение самой идеи
#  последовательного применения: приёмник получит не историю изменений, а
#  случайный срез, и никакая дедупликация по LSN этого не восстановит.
#  Единственно допустимое значение — delete.
# ---------------------------------------------------------------------------
create "$CDC_TOPIC" 1 \
    cleanup.policy=delete \
    "retention.ms=${CDC_RETENTION_MS}" \
    max.message.bytes=10485760 \
    compression.type=producer

# Метаданные транзакций (provide.transaction.metadata=true): BEGIN/END с
# числом событий. Consumer части 2 может по ним проверять полноту транзакции.
# Имя — <topic.prefix>.transaction, ровно 2 сегмента, поэтому регексп
# роутера (3 сегмента) его не трогает.
create "${TOPIC_PREFIX}.transaction" 1 \
    cleanup.policy=delete "retention.ms=${CDC_RETENTION_MS}"

# Heartbeat. Обязателен: если по отслеживаемым таблицам затишье, а в базе идёт
# другой трафик (схема internal), подтверждённый LSN слота не двигается и WAL
# растёт до исчерпания диска. Имя фиксировано Debezium'ом:
# __debezium-heartbeat.<topic.prefix>
create "__debezium-heartbeat.${TOPIC_PREFIX}" 1 \
    cleanup.policy=delete retention.ms=3600000

# ---------------------------------------------------------------------------
#  Служебные топики Kafka Connect.
#  compact — обязательно: это key-value состояние, а не поток. С
#  cleanup.policy=delete воркер потеряет конфиги и offset'ы по retention.
#  Число партиций обязано совпадать с CONNECT_OFFSET_STORAGE_PARTITIONS=25 и
#  CONNECT_STATUS_STORAGE_PARTITIONS=5 в docker-compose.yml.
# ---------------------------------------------------------------------------
create connect-configs 1  cleanup.policy=compact
create connect-offsets 25 cleanup.policy=compact
create connect-status  5  cleanup.policy=compact

# Хранилище Schema Registry. Здесь compact жизненно необходим: это лог
# регистраций схем, и он должен жить вечно.
create _schemas 1 cleanup.policy=compact

log "итог:"
kafka-topics --bootstrap-server "$BOOTSTRAP" --list | sort | sed 's/^/  /'
echo ""
log "конфигурация ключевых топиков:"
for t in "$CDC_TOPIC" connect-offsets _schemas; do
    printf '  %-16s ' "$t"
    kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$t" | head -1 \
        | sed 's/\t/ /g'
done
log "kafka-init завершён успешно"
