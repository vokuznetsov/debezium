#!/usr/bin/env bash
# =============================================================================
#  create-connector.sh [slim|full] — поставить конфиг коннектора.
#
#  Что делает:
#    1. рендерит connector/<профиль>.jsonc: снимает построчные комментарии,
#       подставляет ${ПЕРЕМЕННЫЕ} из .env;
#    2. проверяет результат локально: валидный JSON, нет topic.creation.*,
#       связка порядка (idempotence + acks + max.in.flight) не расцеплена;
#    3. проверяет конфиг через Connect REST (/config/validate) — так неизвестные
#       свойства и опечатки видны ДО создания коннектора;
#    4. кладёт конфиг через PUT /connectors/<name>/config — идемпотентно,
#       создаёт или обновляет;
#    5. ждёт RUNNING и печатает фактическое состояние.
#
#  Слотов и публикаций не создаёт: они уже есть (sql/03,04). Если слота нет,
#  коннектор упадёт — это ожидаемое поведение, см. verify-infra.sh.
# =============================================================================
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PROFILE="${1:-slim}"
case "$PROFILE" in
    slim) SRC="connector/slim.jsonc"; NAME="$CONNECTOR_NAME" ;;
    full) SRC="connector/full.jsonc"; NAME="${CONNECTOR_NAME}-full" ;;
    *) fail "неизвестный профиль '$PROFILE'. Ожидается slim или full." ;;
esac
[ -f "$SRC" ] || fail "нет файла $SRC"

RENDERED="$(mktemp)"; trap 'rm -f "$RENDERED"' EXIT

# --- 1. рендер --------------------------------------------------------------
# Подстановка на чистом bash: без envsubst (нет на macOS) и без python.
# Список переменных явный — неизвестный ${...} останется в тексте и будет
# поймана проверкой ниже.
render() {
    local content
    content="$(sed 's|^[[:space:]]*//.*$||' "$SRC" | grep -v '^[[:space:]]*$')"
    local v
    for v in CONNECTOR_NAME DEBEZIUM_USER DEBEZIUM_PASSWORD POSTGRES_DB \
             TOPIC_PREFIX TOPIC_PREFIX_FULL CDC_SLOT CDC_SLOT_FULL \
             CDC_PUBLICATION CDC_TOPIC CDC_TOPIC_FULL CDC_SCHEMAS \
             SCHEMA_REGISTRY_INTERNAL_URL HEARTBEAT_INTERVAL_MS MAX_IN_FLIGHT \
             PRODUCER_COMPRESSION PRODUCER_LINGER_MS PRODUCER_BATCH_SIZE; do
        content="${content//\$\{$v\}/${!v}}"
    done
    printf '%s' "$content"
}
render > "$RENDERED"

if grep -q '\${' "$RENDERED"; then
    grep -n '\${' "$RENDERED" >&2
    fail "в отрендеренном конфиге осталась неподставленная переменная (см. выше)"
fi
JQ -e . "$RENDERED" >/dev/null || fail "после снятия комментариев получился невалидный JSON"

CONFIG_JSON="$(JQ -c '.config' "$RENDERED")"

head2 "профиль ${PROFILE}, коннектор ${NAME}"

# --- 2. локальные проверки конфига ------------------------------------------
if printf '%s' "$CONFIG_JSON" | JQ -e 'keys[] | select(startswith("topic.creation."))' >/dev/null 2>&1; then
    fail "в конфиге есть свойство topic.creation.* — топики создаются заранее (kafka-init), auto-create выключен"
fi
ok "свойств topic.creation.* нет"

IDEMP=$(printf '%s' "$CONFIG_JSON" | JQ -r '."producer.override.enable.idempotence" // "ОТСУТСТВУЕТ"')
ACKS=$(printf  '%s' "$CONFIG_JSON" | JQ -r '."producer.override.acks" // "ОТСУТСТВУЕТ"')
MIF=$(printf   '%s' "$CONFIG_JSON" | JQ -r '."producer.override.max.in.flight.requests.per.connection" // "ОТСУТСТВУЕТ"')
info "связка порядка: enable.idempotence=${IDEMP}  acks=${ACKS}  max.in.flight=${MIF}"
[ "$IDEMP" = "true" ] || fail "enable.idempotence != true — порядок не гарантирован (см. CLAUDE.md, «что НЕ делать»)"
[ "$ACKS" = "all" ]   || fail "acks != all"
case "$MIF" in ''|*[!0-9]*) fail "max.in.flight не число: '$MIF'";; esac
[ "$MIF" -le 5 ] || fail "max.in.flight=${MIF} > 5: Kafka не запустит идемпотентный producer"
ok "связка порядка не расцеплена"

TASKS=$(printf '%s' "$CONFIG_JSON" | JQ -r '."tasks.max"')
[ "$TASKS" = "1" ] || fail "tasks.max=${TASKS}, ожидается 1 (инвариант №1)"
ok "tasks.max=1"

# --- 3. валидация на воркере ------------------------------------------------
CLASS=$(printf '%s' "$CONFIG_JSON" | JQ -r '."connector.class"')
VAL=$(printf '%s' "$CONFIG_JSON" | JQ -c --arg n "$NAME" '. + {"name":$n}')
RESP=$(capi PUT "/connector-plugins/${CLASS}/config/validate" "$VAL") \
    || fail "воркер Connect недоступен"
ERRC=$(printf '%s' "$RESP" | JQ -r '.error_count // 0')
if [ "$ERRC" != "0" ]; then
    printf '%s' "$RESP" | JQ -r '.configs[] | select((.value.errors|length)>0)
        | "  \(.value.name): \(.value.errors|join("; "))"' >&2
    fail "воркер забраковал конфиг (ошибок: ${ERRC})"
fi
ok "воркер валидировал конфиг без ошибок"

# --- 4. установка -----------------------------------------------------------
BEFORE=$(capi_code "/connectors/${NAME}")
RESP=$(capi PUT "/connectors/${NAME}/config" "$CONFIG_JSON")
if printf '%s' "$RESP" | JQ -e '.error_code?' >/dev/null 2>&1; then
    printf '%s\n' "$RESP" >&2
    fail "Connect отверг конфиг"
fi
if [ "$BEFORE" = "404" ]; then ok "коннектор ${NAME} создан"; else ok "конфиг коннектора ${NAME} обновлён"; fi

# --- 5. состояние -----------------------------------------------------------
if ! wait_connector_running "$NAME" 90; then
    fail "коннектор ${NAME} не вышел в RUNNING (трейс выше)"
fi
ok "connector=RUNNING task=RUNNING"

head2 "фактическое состояние"
capi GET "/connectors/${NAME}/status" | JQ -r \
  '"  коннектор: \(.connector.state) на \(.connector.worker_id)",
   (.tasks[] | "  задача \(.id): \(.state) на \(.worker_id)")'

head2 "слот, на котором работает коннектор"
pgm -c "SELECT slot_name, plugin, active, active_pid, failover, restart_lsn, confirmed_flush_lsn
        FROM pg_replication_slots WHERE slot_type='logical' ORDER BY slot_name;"
