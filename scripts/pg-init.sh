#!/usr/bin/env bash
# =============================================================================
#  pg-init — одноразовая идемпотентная инициализация мастера.
#
#  ПОЧЕМУ ОТДЕЛЬНЫЙ СЕРВИС, А НЕ /docker-entrypoint-initdb.d:
#  initdb-хуки образа postgres отрабатывают ТОЛЬКО на пустом каталоге данных.
#  Тома у нас сохраняются между `make down` / `make up`, значит всё, что должно
#  выполняться при каждом старте (досоздание слотов после clean, довыдача прав,
#  добавление таблиц в публикацию), в initdb-хуки класть нельзя — оно просто
#  никогда не запустится второй раз.
#
#  Сервис объявлен с restart: "no" и depends_on: pg-master: service_healthy —
#  отработал, вышел с кодом 0, реплики стартуют после него.
# =============================================================================
set -euo pipefail

log()  { echo ""; echo "=== [pg-init] $* ==="; }
die()  { echo "[pg-init][ОШИБКА] $*" >&2; exit 1; }

: "${POSTGRES_USER:?}" "${POSTGRES_PASSWORD:?}" "${POSTGRES_DB:?}"
: "${REPL_USER:?}" "${REPL_PASSWORD:?}" "${DEBEZIUM_USER:?}" "${DEBEZIUM_PASSWORD:?}"
: "${REPLICA1_SLOT:?}" "${REPLICA2_SLOT:?}" "${CDC_SLOT:?}" "${CDC_PUBLICATION:?}"
# Состав CDC: единая точка правки, см. CDC_SCHEMAS в .env
: "${CDC_SCHEMAS:?}"

PRIMARY_HOST="${PRIMARY_HOST:-pg-master}"
# true при обычном `make up`; false — в scenario-cold-start.sh, где слот
# создаётся отдельным шагом, чтобы показать границу удержания WAL.
CREATE_CDC_SLOT="${CREATE_CDC_SLOT:-true}"
# true при обычном `make up`; false — когда данные заливаются отдельным шагом.
SEED_DATA="${SEED_DATA:-true}"
# Список таблиц (через запятую), которым нужен REPLICA IDENTITY FULL.
REPLICA_IDENTITY_FULL="${REPLICA_IDENTITY_FULL:-}"

export PGPASSWORD="$POSTGRES_PASSWORD"
PSQL=(psql -h "$PRIMARY_HOST" -p 5432 -U "$POSTGRES_USER" -d "$POSTGRES_DB"
      -v ON_ERROR_STOP=1 --no-psqlrc)

echo "[pg-init] цель: ${PRIMARY_HOST}/${POSTGRES_DB}"
echo "[pg-init] CREATE_CDC_SLOT=${CREATE_CDC_SLOT}  SEED_DATA=${SEED_DATA}"
"${PSQL[@]}" -tAc 'SELECT version()' || die "мастер недоступен"

# =============================================================================
#  ПОРЯДОК ВЫПОЛНЕНИЯ ОТЛИЧАЕТСЯ ОТ НУМЕРАЦИИ ФАЙЛОВ — ЭТО НАМЕРЕННО.
#
#  Нумерация файлов — по теме (роли, схемы, слоты, публикация, данные).
#  Порядок выполнения задаёт семантика CDC:
#
#      01-roles -> 02-schemas -> 05-seed -> 04-publication -> 03-slots
#                                 ^^^^^^^                     ^^^^^^^^^
#                                 ДО слота                    ПОСЛЕДНИМ
#
#  Слот создаётся ПОСЛЕДНИМ, потому что граница «что попадёт в топик»
#  проходит по моменту создания слота. Начальные данные (05-seed.sql) — это
#  модель состояния, на котором базы (А) и (Б) уже синхронизированы другим
#  способом. Если залить их ПОСЛЕ создания слота, они уедут в cdc.events и
#  consumer части 2 применит в (Б) то, что там уже есть.
#
#  Проверено на стенде: при обратном порядке в cdc.events появлялись 18
#  событий op=c по строкам seed'а.
# =============================================================================

log "01-roles.sql — роли репликации и Debezium"
"${PSQL[@]}" \
    -v repl_user="$REPL_USER"          -v repl_password="$REPL_PASSWORD" \
    -v dbz_user="$DEBEZIUM_USER"       -v dbz_password="$DEBEZIUM_PASSWORD" \
    -f /sql/01-roles.sql

log "02-schemas.sql — схемы, таблицы, межсхемные FK, права"
"${PSQL[@]}" -v dbz_user="$DEBEZIUM_USER" -v cdc_schemas="$CDC_SCHEMAS" -f /sql/02-schemas.sql

if [ "$SEED_DATA" = "true" ]; then
    log "05-seed.sql — начальные данные (заливаются ДО создания слота)"
    "${PSQL[@]}" -f /sql/05-seed.sql
else
    log "05-seed.sql пропущен (SEED_DATA=false)"
fi

log "04-publication.sql — публикация и REPLICA IDENTITY"
"${PSQL[@]}" \
    -v publication="$CDC_PUBLICATION" \
    -v replica_identity_full="$REPLICA_IDENTITY_FULL" \
    -v cdc_schemas="$CDC_SCHEMAS" \
    -f /sql/04-publication.sql

log "03-slots.sql — слоты репликации (ПОСЛЕДНИМ: с этого момента WAL удерживается)"
"${PSQL[@]}" \
    -v replica1_slot="$REPLICA1_SLOT" -v replica2_slot="$REPLICA2_SLOT" \
    -v cdc_slot="$CDC_SLOT" -v create_cdc_slot="$CREATE_CDC_SLOT" \
    -f /sql/03-slots.sql

log "pg-init завершён успешно"
