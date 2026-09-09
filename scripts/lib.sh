# =============================================================================
#  lib.sh — общая часть скриптов проверок.
#
#  Подключается как:  . "$(dirname "$0")/lib.sh"
#
#  ПРИНЦИП: скрипты запускаются С ХОСТА, но все клиенты работают В
#  КОНТЕЙНЕРАХ. На хосте нужны только docker, bash и awk. psql, kafka-topics,
#  kcat и curl берутся из контейнеров стенда — поэтому проверки одинаково
#  работают на любой машине и проверяют ровно те версии клиентов, что стоят
#  в стенде. jq берётся с хоста, если он есть, иначе из контейнера.
# =============================================================================
# shellcheck shell=bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# .env — единственный источник версий и имён
set -a
# shellcheck disable=SC1091
. ./.env
set +a

# --- вывод -------------------------------------------------------------------
C_RESET=$'\033[0m'; C_B=$'\033[1m'; C_R=$'\033[31m'; C_G=$'\033[32m'; C_Y=$'\033[33m'
STEP_N=0
FAILURES=0

step()  { STEP_N=$((STEP_N+1)); printf '\n%s=== ШАГ %s: %s ===%s\n' "$C_B" "$STEP_N" "$*" "$C_RESET"; }
head2() { printf '\n%s--- %s ---%s\n' "$C_B" "$*" "$C_RESET"; }
info()  { printf '    %s\n' "$*"; }
ok()    { printf '  %s[OK]%s   %s\n'  "$C_G" "$C_RESET" "$*"; }
warn()  { printf '  %s[ВНИМАНИЕ]%s %s\n' "$C_Y" "$C_RESET" "$*"; }
# fail печатает и СРАЗУ выходит с ненулевым кодом: скрипт обязан падать на
# первом несоответствии, а не досчитывать до конца.
fail()  { printf '  %s[ПРОВАЛ]%s %s\n' "$C_R" "$C_RESET" "$*" >&2; exit 1; }
# softfail — для verify-*, где полезно увидеть все нарушения сразу
softfail() { FAILURES=$((FAILURES+1)); printf '  %s[ПРОВАЛ]%s %s\n' "$C_R" "$C_RESET" "$*" >&2; }
finish() {
    if [ "$FAILURES" -gt 0 ]; then
        printf '\n%sИТОГ: нарушений — %s%s\n' "$C_R" "$FAILURES" "$C_RESET" >&2
        exit 1
    fi
    printf '\n%sИТОГ: все проверки пройдены%s\n' "$C_G" "$C_RESET"
}

# --- jq: хостовой или контейнерный ------------------------------------------
if command -v jq >/dev/null 2>&1; then
    JQ() { command jq "$@"; }
else
    JQ() { docker run --rm -i ghcr.io/jqlang/jq:1.7.1 "$@"; }
fi

# --- PostgreSQL --------------------------------------------------------------
# psql на мастере под суперпользователем (роль devops)
pgm() {
    docker compose exec -T pg-master \
        psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 --no-psqlrc "$@"
}
# одиночный запрос, только значения (tuples only, unaligned)
pgq() { pgm -tAc "$1"; }
# то же на произвольном узле кластера: pgq_on pg-replica-1 "SELECT ..."
pgq_on() {
    local node="$1"; shift
    docker compose exec -T "$node" \
        psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA --no-psqlrc -c "$1"
}
# psql от имени роли debezium — для проверки ограничений прав
pgdbz() {
    docker compose exec -T -e PGPASSWORD="$DEBEZIUM_PASSWORD" pg-master \
        psql -h pg-master -U "$DEBEZIUM_USER" -d "$POSTGRES_DB" --no-psqlrc "$@"
}

# --- Kafka -------------------------------------------------------------------
ktopics() { docker compose exec -T kafka kafka-topics --bootstrap-server "$KAFKA_INTERNAL" "$@"; }
kconfigs() { docker compose exec -T kafka kafka-configs --bootstrap-server "$KAFKA_INTERNAL" "$@"; }
# В Kafka 3.7 класс kafka.tools.GetOffsetShell переехал в
# org.apache.kafka.tools; вместо возни с классами используем штатную обёртку
# kafka-get-offsets из CP 7.7.
kgetoffsets() { docker compose exec -T kafka kafka-get-offsets --bootstrap-server "$KAFKA_INTERNAL" "$@"; }
# число сообщений в топике (сумма high watermark по партициям; для delete-топика
# без сжатия и без транзакций это фактическое число сообщений)
topic_count() {
    local t="$1"
    kgetoffsets --topic "$t" 2>/dev/null \
        | awk -F: '{s+=$3} END {print s+0}'
}

# kcat из профиля tools. Внутри compose-сети, поэтому адреса ровно такие, как
# в спеке: -b kafka:9092 -r http://schema-registry:8081
kc() { docker compose --profile tools run --rm -T kcat "$@"; }
# То же, но оставляет только строки JSON: docker compose пишет в поток свои
# служебные строки ("Creating ..."), и они ломают разбор через jq.
kcj() { kc "$@" 2>/dev/null | grep '^{'; }

# --- чтение топика с Avro-десериализацией ------------------------------------
# ГРАБЛИ: kafka-avro-console-consumer пишет свой log4j (INFO ConsumerConfig
# values: ... — десятки строк) В STDOUT, а не в stderr, поэтому 2>/dev/null не
# спасает: конфиг клиента попадает в разбираемый вывод и ломает jq. Оставляем
# только строки данных: они начинаются с "{" (чистое значение) или с
# "Partition:" (когда включены print.partition/print.offset).
#
# Почему не kcat: его конвертер Avro->JSON обрывается на Avro-полях bytes
# (decimal.handling.mode=precise) — подробно в шапке verify-order.sh.
#
# avro_consume <топик> <макс_сообщений> [доп. аргументы консьюмера...]
avro_consume() {
    local topic="$1" max="$2"; shift 2
    # --from-beginning несовместим с --offset, поэтому его добавляем только
    # когда вызывающий не задал стартовый offset сам.
    # Строка, а не массив: bash 3.2 (штатный на macOS) под set -u падает на
    # раскрытии ПУСТОГО массива ("${arr[@]}": unbound variable).
    local from="--from-beginning"
    case " $* " in *" --offset "*) from="" ;; esac
    # shellcheck disable=SC2086
    docker compose exec -T schema-registry kafka-avro-console-consumer \
        --bootstrap-server "$KAFKA_INTERNAL" --topic "$topic" $from \
        --property "schema.registry.url=${SCHEMA_REGISTRY_INTERNAL_URL}" \
        --max-messages "$max" --timeout-ms "${AVRO_TIMEOUT_MS:-90000}" "$@" 2>/dev/null \
        | grep -E '^(\{|Partition:)' || true
}

# --- Kafka Connect REST ------------------------------------------------------
# curl исполняется ВНУТРИ контейнера connect
capi() {
    local method="$1" path="$2" body="${3:-}"
    if [ -n "$body" ]; then
        printf '%s' "$body" | docker compose exec -T connect \
            curl -sS -X "$method" -H 'Content-Type: application/json' \
                 --data-binary @- "http://localhost:8083${path}"
    else
        docker compose exec -T connect curl -sS -X "$method" "http://localhost:8083${path}"
    fi
}
# HTTP-код отдельно — нужен, чтобы отличать 404 от пустого ответа
capi_code() {
    docker compose exec -T connect \
        curl -sS -o /dev/null -w '%{http_code}' "http://localhost:8083${1}"
}
sapi() { docker compose exec -T schema-registry curl -sS "http://localhost:8081${1}"; }

connector_state() {
    capi GET "/connectors/${1:-$CONNECTOR_NAME}/status" 2>/dev/null \
        | JQ -r '.connector.state // "ABSENT"' 2>/dev/null || echo ABSENT
}
task_state() {
    capi GET "/connectors/${1:-$CONNECTOR_NAME}/status" 2>/dev/null \
        | JQ -r '.tasks[0].state // "NONE"' 2>/dev/null || echo NONE
}

# --- ожидания (без глухих sleep: ждём условие) -------------------------------
wait_for() {
    local desc="$1" timeout="$2"; shift 2
    local t=0
    while ! "$@" >/dev/null 2>&1; do
        t=$((t+1))
        [ "$t" -ge "$timeout" ] && { printf '  таймаут ожидания: %s\n' "$desc" >&2; return 1; }
        sleep 1
    done
    return 0
}
wait_connector_running() {
    local name="${1:-$CONNECTOR_NAME}" timeout="${2:-60}" t=0
    while :; do
        local cs ts
        cs=$(connector_state "$name"); ts=$(task_state "$name")
        [ "$cs" = RUNNING ] && [ "$ts" = RUNNING ] && return 0
        if [ "$cs" = FAILED ] || [ "$ts" = FAILED ]; then
            printf '  коннектор %s: connector=%s task=%s\n' "$name" "$cs" "$ts" >&2
            capi GET "/connectors/${name}/status" | JQ -r \
                '.tasks[0].trace // .connector.trace // "трейс отсутствует"' | head -25 >&2
            return 2
        fi
        t=$((t+1)); [ "$t" -ge "$timeout" ] && {
            printf '  таймаут: коннектор %s не вышел в RUNNING (connector=%s task=%s)\n' \
                   "$name" "$cs" "$ts" >&2; return 1; }
        sleep 1
    done
}

# ГРАБЛИ: `docker compose logs ... | grep -q ...` под `set -o pipefail` даёт
# ложноотрицательный результат: grep -q выходит на первом совпадении, docker
# compose получает SIGPIPE, код пайплайна оказывается ненулевым, и условие
# уходит в else ПРИ НАЛИЧИИ совпадения. grep -c дочитывает поток до конца.
logs_have() {
    local svc="$1" pattern="$2" n
    n=$(docker compose logs "$svc" --no-log-prefix 2>&1 | grep -cE "$pattern" || true)
    [ "${n:-0}" -gt 0 ]
}
logs_count() {
    docker compose logs "$1" --no-log-prefix 2>&1 | grep -cE "$2" || true
}

# Текущий LSN мастера в виде числа (pg_lsn -> int8), для сравнений
lsn_num() { pgq "SELECT pg_wal_lsn_diff('$1', '0/0')::bigint"; }
