#!/usr/bin/env bash
# Бутстрап hot standby: первый старт — pg_basebackup с мастера, дальше просто
# запуск postgres (том сохранился, реплика догонит мастер по WAL).
set -euo pipefail

# dbname в строке подключения обязателен: без него slotsync-воркер PG 17 не
# сможет подключиться к базе мастера и синхронизации слотов не будет.
CONNINFO="host=${PRIMARY_HOST} port=5432 user=${REPL_USER} dbname=${POSTGRES_DB} application_name=${REPLICA_NAME}"

mkdir -p "$PGDATA"
chown -R postgres:postgres "$(dirname "$PGDATA")"
chmod 700 "$PGDATA"

if [ -s "$PGDATA/PG_VERSION" ]; then
    echo "[${REPLICA_NAME}] PGDATA уже есть — pg_basebackup пропущен"
else
    echo "[${REPLICA_NAME}] снимаю копию с ${PRIMARY_HOST} через слот ${REPLICA_SLOT}"
    # -R  записать standby.signal + primary_conninfo + primary_slot_name
    # -S  использовать ГОТОВЫЙ слот (без -C: прав на создание слота не нужно)
    PGPASSWORD="$REPL_PASSWORD" gosu postgres \
        pg_basebackup -d "$CONNINFO" -D "$PGDATA" -R -X stream -S "$REPLICA_SLOT" -c fast -P
fi

exec gosu postgres postgres \
    -c config_file=/etc/postgresql/postgresql.conf \
    -c hba_file=/etc/postgresql/pg_hba.conf
