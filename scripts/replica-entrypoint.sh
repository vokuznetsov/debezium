#!/usr/bin/env bash
# =============================================================================
#  Бутстрап hot standby через pg_basebackup.
#
#  Идемпотентен: если $PGDATA уже инициализирован (том сохранился после
#  `make down`), базовая копия НЕ снимается заново, контейнер просто стартует
#  postgres и догоняет мастер по WAL из своего физического слота.
#
#  Физический слот НЕ создаётся здесь (нет -C): его заранее создаёт
#  sql/03-slots.sql через сервис pg-init, а compose гарантирует порядок через
#  depends_on: pg-init: service_completed_successfully. Никаких sleep.
# =============================================================================
set -euo pipefail

log() { echo "[replica-entrypoint][$(date +%H:%M:%S)] $*"; }
die() { echo "[replica-entrypoint][ОШИБКА] $*" >&2; exit 1; }

: "${PGDATA:?PGDATA не задан}"
: "${PRIMARY_HOST:?PRIMARY_HOST не задан}"
: "${REPLICA_SLOT:?REPLICA_SLOT не задан}"
: "${REPL_USER:?}" "${REPL_PASSWORD:?}" "${POSTGRES_DB:?}"
REPLICA_NAME="${REPLICA_NAME:-$(hostname)}"

# primary_conninfo с dbname: без dbname slotsync-воркер PG 17 не сможет
# подключиться к базе мастера и синхронизация failover-слотов не заработает.
# pg_basebackup -R переносит в postgresql.auto.conf ровно ту строку
# подключения, с которой его вызвали, поэтому dbname задаём здесь.
CONNINFO="host=${PRIMARY_HOST} port=5432 user=${REPL_USER} dbname=${POSTGRES_DB} application_name=${REPLICA_NAME}"

mkdir -p "$PGDATA"
chown -R postgres:postgres "$(dirname "$PGDATA")"
chmod 700 "$PGDATA"

if [ -s "$PGDATA/PG_VERSION" ]; then
    log "PGDATA уже инициализирован ($(cat "$PGDATA/PG_VERSION")) — pg_basebackup пропущен."
    if [ ! -f "$PGDATA/standby.signal" ]; then
        log "ВНИМАНИЕ: standby.signal отсутствует — этот каталог уже промоутнут в мастер."
        log "Это нормально после scripts/test-failover.sh. Для возврата в standby: make clean."
    fi
else
    log "PGDATA пуст — снимаю базовую копию с ${PRIMARY_HOST} через слот ${REPLICA_SLOT}"
    # -R          записать standby.signal + primary_conninfo + primary_slot_name
    # -X stream   стримить WAL параллельно копированию (копия сразу консистентна)
    # -S          использовать ГОТОВЫЙ слот (без -C: права на создание не нужны)
    # -c fast     не ждать чекпоинта мастера
    PGPASSWORD="$REPL_PASSWORD" gosu postgres \
        pg_basebackup -d "$CONNINFO" -D "$PGDATA" -R -X stream -S "$REPLICA_SLOT" -c fast -P -v \
        || die "pg_basebackup не удался. Обычные причины: слот ${REPLICA_SLOT} не создан (pg-init не отработал), нет строки в pg_hba для replication, не совпал пароль ${REPL_USER}."
    log "Базовая копия снята. primary_conninfo/primary_slot_name из postgresql.auto.conf:"
    grep -E 'primary_(conninfo|slot_name)' "$PGDATA/postgresql.auto.conf" | sed 's/password=[^ ]*/password=***/'
    grep -q 'dbname=' "$PGDATA/postgresql.auto.conf" \
        || die "в primary_conninfo нет dbname — sync_replication_slots работать не будет"
fi

log "старт postgres (config_file=/etc/postgresql/postgresql.conf)"
exec gosu postgres postgres \
    -c config_file=/etc/postgresql/postgresql.conf \
    -c hba_file=/etc/postgresql/pg_hba.conf
